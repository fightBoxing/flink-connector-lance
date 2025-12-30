/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.lance;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.ipc.LanceScanner;
import com.lancedb.lance.ipc.ScanOptions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Lance 数据源实现。
 * 
 * <p>从 Lance 数据集中读取数据并转换为 Flink RowData。
 * 
 * <p>使用示例：
 * <pre>{@code
 * LanceOptions options = LanceOptions.builder()
 *     .path("/path/to/lance/dataset")
 *     .readBatchSize(1024)
 *     .build();
 * 
 * LanceSource source = new LanceSource(options, rowType);
 * DataStream<RowData> stream = env.addSource(source);
 * }</pre>
 */
public class LanceSource extends RichParallelSourceFunction<RowData> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceSource.class);

    private final LanceOptions options;
    private final RowType rowType;
    private final String[] selectedColumns;

    private transient volatile boolean running;
    private transient BufferAllocator allocator;
    private transient Dataset dataset;
    private transient RowDataConverter converter;

    /**
     * 创建 LanceSource
     *
     * @param options Lance 配置选项
     * @param rowType Flink RowType
     */
    public LanceSource(LanceOptions options, RowType rowType) {
        this.options = options;
        this.rowType = rowType;
        
        List<String> columns = options.getReadColumns();
        this.selectedColumns = columns != null && !columns.isEmpty() 
                ? columns.toArray(new String[0]) 
                : null;
    }

    /**
     * 创建 LanceSource（自动推断 Schema）
     *
     * @param options Lance 配置选项
     */
    public LanceSource(LanceOptions options) {
        this(options, null);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        LOG.info("打开 Lance 数据源: {}", options.getPath());
        
        this.running = true;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        
        // 打开 Lance 数据集
        String datasetPath = options.getPath();
        if (datasetPath == null || datasetPath.isEmpty()) {
            throw new IllegalArgumentException("Lance 数据集路径不能为空");
        }
        
        Path path = Paths.get(datasetPath);
        try {
            this.dataset = Dataset.open(path.toString(), allocator);
        } catch (Exception e) {
            throw new IOException("无法打开 Lance 数据集: " + datasetPath, e);
        }
        
        // 初始化 RowDataConverter
        RowType actualRowType = this.rowType;
        if (actualRowType == null) {
            // 从数据集 Schema 推断 RowType
            Schema arrowSchema = dataset.getSchema();
            actualRowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
        }
        this.converter = new RowDataConverter(actualRowType);
        
        LOG.info("Lance 数据源已打开，Schema: {}", actualRowType);
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        LOG.info("开始读取 Lance 数据集: {}", options.getPath());
        
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
        int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        
        // 获取数据集的所有 Fragment
        List<Fragment> fragments = dataset.getFragments();
        LOG.info("数据集共有 {} 个 Fragment，当前子任务 {}/{}", 
                fragments.size(), subtaskIndex, numSubtasks);
        
        // 按子任务分配 Fragment
        for (int i = 0; i < fragments.size() && running; i++) {
            // 简单的轮询分配策略
            if (i % numSubtasks != subtaskIndex) {
                continue;
            }
            
            Fragment fragment = fragments.get(i);
            readFragment(ctx, fragment);
        }
        
        LOG.info("Lance 数据源读取完成");
    }

    /**
     * 读取单个 Fragment
     */
    private void readFragment(SourceContext<RowData> ctx, Fragment fragment) throws Exception {
        LOG.debug("读取 Fragment: {}", fragment.getId());
        
        // 构建扫描选项
        ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
        
        // 设置批次大小
        scanOptionsBuilder.batchSize(options.getReadBatchSize());
        
        // 设置列过滤
        if (selectedColumns != null && selectedColumns.length > 0) {
            scanOptionsBuilder.columns(Arrays.asList(selectedColumns));
        }
        
        // 设置数据过滤条件
        String filter = options.getReadFilter();
        if (filter != null && !filter.isEmpty()) {
            scanOptionsBuilder.filter(filter);
        }
        
        ScanOptions scanOptions = scanOptionsBuilder.build();
        
        // 创建 Scanner 并读取数据
        try (LanceScanner scanner = fragment.newScan(scanOptions)) {
            try (ArrowReader reader = scanner.scanBatches()) {
                while (reader.loadNextBatch() && running) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    
                    // 转换为 RowData 并输出
                    List<RowData> rows = converter.toRowDataList(root);
                    synchronized (ctx.getCheckpointLock()) {
                        for (RowData row : rows) {
                            ctx.collect(row);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void cancel() {
        LOG.info("取消 Lance 数据源");
        this.running = false;
    }

    @Override
    public void close() throws Exception {
        LOG.info("关闭 Lance 数据源");
        
        this.running = false;
        
        if (dataset != null) {
            try {
                dataset.close();
            } catch (Exception e) {
                LOG.warn("关闭 Lance 数据集时出错", e);
            }
            dataset = null;
        }
        
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("关闭内存分配器时出错", e);
            }
            allocator = null;
        }
        
        super.close();
    }

    /**
     * 获取 RowType
     */
    public RowType getRowType() {
        return rowType;
    }

    /**
     * 获取配置选项
     */
    public LanceOptions getOptions() {
        return options;
    }

    /**
     * 获取选择的列
     */
    public String[] getSelectedColumns() {
        return selectedColumns;
    }

    /**
     * Builder 模式构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * LanceSource 构建器
     */
    public static class Builder {
        private String path;
        private int batchSize = 1024;
        private List<String> columns;
        private String filter;
        private RowType rowType;

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder columns(List<String> columns) {
            this.columns = columns;
            return this;
        }

        public Builder filter(String filter) {
            this.filter = filter;
            return this;
        }

        public Builder rowType(RowType rowType) {
            this.rowType = rowType;
            return this;
        }

        public LanceSource build() {
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("数据集路径不能为空");
            }

            LanceOptions options = LanceOptions.builder()
                    .path(path)
                    .readBatchSize(batchSize)
                    .readColumns(columns)
                    .readFilter(filter)
                    .build();

            return new LanceSource(options, rowType);
        }
    }
}

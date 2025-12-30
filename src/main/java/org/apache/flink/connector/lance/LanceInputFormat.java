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

import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.core.io.InputSplitAssigner;
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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Lance InputFormat 实现。
 * 
 * <p>使用 InputFormat 接口从 Lance 数据集读取数据，支持分片并行读取。
 */
public class LanceInputFormat extends RichInputFormat<RowData, LanceSplit> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceInputFormat.class);

    private final LanceOptions options;
    private final RowType rowType;
    private final String[] selectedColumns;

    private transient BufferAllocator allocator;
    private transient Dataset dataset;
    private transient RowDataConverter converter;
    private transient LanceScanner currentScanner;
    private transient ArrowReader currentReader;
    private transient Iterator<RowData> currentBatchIterator;
    private transient boolean reachedEnd;

    /**
     * 创建 LanceInputFormat
     *
     * @param options Lance 配置选项
     * @param rowType Flink RowType
     */
    public LanceInputFormat(LanceOptions options, RowType rowType) {
        this.options = options;
        this.rowType = rowType;
        
        List<String> columns = options.getReadColumns();
        this.selectedColumns = columns != null && !columns.isEmpty() 
                ? columns.toArray(new String[0]) 
                : null;
    }

    @Override
    public void configure(Configuration parameters) {
        // 配置已在构造函数中完成
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        // 返回基础统计信息
        return cachedStatistics;
    }

    @Override
    public LanceSplit[] createInputSplits(int minNumSplits) throws IOException {
        LOG.info("创建输入分片，最小分片数: {}", minNumSplits);
        
        String datasetPath = options.getPath();
        if (datasetPath == null || datasetPath.isEmpty()) {
            throw new IOException("数据集路径不能为空");
        }

        BufferAllocator tempAllocator = new RootAllocator(Long.MAX_VALUE);
        try {
            Dataset tempDataset = Dataset.open(datasetPath, tempAllocator);
            try {
                List<Fragment> fragments = tempDataset.getFragments();
                LanceSplit[] splits = new LanceSplit[fragments.size()];
                
                for (int i = 0; i < fragments.size(); i++) {
                    Fragment fragment = fragments.get(i);
                    long rowCount = fragment.countRows();
                    splits[i] = new LanceSplit(i, fragment.getId(), datasetPath, rowCount);
                }
                
                LOG.info("创建了 {} 个输入分片", splits.length);
                return splits;
            } finally {
                tempDataset.close();
            }
        } finally {
            tempAllocator.close();
        }
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(LanceSplit[] inputSplits) {
        return new LanceSplitAssigner(inputSplits);
    }

    @Override
    public void open(LanceSplit split) throws IOException {
        LOG.info("打开分片: {}", split);
        
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.reachedEnd = false;
        
        // 打开数据集
        String datasetPath = split.getDatasetPath();
        try {
            this.dataset = Dataset.open(datasetPath, allocator);
        } catch (Exception e) {
            throw new IOException("无法打开数据集: " + datasetPath, e);
        }
        
        // 初始化转换器
        RowType actualRowType = this.rowType;
        if (actualRowType == null) {
            Schema arrowSchema = dataset.getSchema();
            actualRowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
        }
        this.converter = new RowDataConverter(actualRowType);
        
        // 获取指定的 Fragment
        List<Fragment> fragments = dataset.getFragments();
        Fragment targetFragment = null;
        for (Fragment fragment : fragments) {
            if (fragment.getId() == split.getFragmentId()) {
                targetFragment = fragment;
                break;
            }
        }
        
        if (targetFragment == null) {
            throw new IOException("找不到 Fragment: " + split.getFragmentId());
        }
        
        // 构建扫描选项
        ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder();
        scanOptionsBuilder.batchSize(options.getReadBatchSize());
        
        if (selectedColumns != null && selectedColumns.length > 0) {
            scanOptionsBuilder.columns(Arrays.asList(selectedColumns));
        }
        
        String filter = options.getReadFilter();
        if (filter != null && !filter.isEmpty()) {
            scanOptionsBuilder.filter(filter);
        }
        
        ScanOptions scanOptions = scanOptionsBuilder.build();
        
        // 创建 Scanner
        try {
            this.currentScanner = targetFragment.newScan(scanOptions);
            this.currentReader = currentScanner.scanBatches();
        } catch (Exception e) {
            throw new IOException("创建 Scanner 失败", e);
        }
        
        // 加载第一批数据
        loadNextBatch();
    }

    /**
     * 加载下一批数据
     */
    private void loadNextBatch() throws IOException {
        try {
            if (currentReader.loadNextBatch()) {
                VectorSchemaRoot root = currentReader.getVectorSchemaRoot();
                List<RowData> rows = converter.toRowDataList(root);
                this.currentBatchIterator = rows.iterator();
            } else {
                this.reachedEnd = true;
                this.currentBatchIterator = null;
            }
        } catch (Exception e) {
            throw new IOException("加载数据批次失败", e);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return reachedEnd;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (reachedEnd) {
            return null;
        }
        
        // 当前批次还有数据
        if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
            return currentBatchIterator.next();
        }
        
        // 加载下一批
        loadNextBatch();
        
        if (reachedEnd) {
            return null;
        }
        
        if (currentBatchIterator != null && currentBatchIterator.hasNext()) {
            return currentBatchIterator.next();
        }
        
        return null;
    }

    @Override
    public void close() throws IOException {
        LOG.info("关闭 LanceInputFormat");
        
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (Exception e) {
                LOG.warn("关闭 Reader 失败", e);
            }
            currentReader = null;
        }
        
        if (currentScanner != null) {
            try {
                currentScanner.close();
            } catch (Exception e) {
                LOG.warn("关闭 Scanner 失败", e);
            }
            currentScanner = null;
        }
        
        if (dataset != null) {
            try {
                dataset.close();
            } catch (Exception e) {
                LOG.warn("关闭数据集失败", e);
            }
            dataset = null;
        }
        
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("关闭分配器失败", e);
            }
            allocator = null;
        }
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
     * Lance 分片分配器
     */
    private static class LanceSplitAssigner implements InputSplitAssigner {
        private final List<LanceSplit> remainingSplits;

        public LanceSplitAssigner(LanceSplit[] splits) {
            this.remainingSplits = new ArrayList<>();
            for (LanceSplit split : splits) {
                remainingSplits.add(split);
            }
        }

        @Override
        public synchronized LanceSplit getNextInputSplit(String host, int taskId) {
            if (remainingSplits.isEmpty()) {
                return null;
            }
            return remainingSplits.remove(remainingSplits.size() - 1);
        }

        @Override
        public void returnInputSplit(List<org.apache.flink.core.io.InputSplit> splits, int taskId) {
            for (org.apache.flink.core.io.InputSplit split : splits) {
                if (split instanceof LanceSplit) {
                    synchronized (this) {
                        remainingSplits.add((LanceSplit) split);
                    }
                }
            }
        }
    }
}

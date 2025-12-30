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

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.config.LanceOptions.MetricType;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.index.DistanceType;
import com.lancedb.lance.ipc.LanceScanner;
import com.lancedb.lance.ipc.Query;
import com.lancedb.lance.ipc.ScanOptions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Lance 向量检索实现。
 * 
 * <p>支持 KNN 检索，支持 L2、Cosine、Dot 三种距离度量方式。
 * 
 * <p>使用示例：
 * <pre>{@code
 * LanceVectorSearch search = LanceVectorSearch.builder()
 *     .datasetPath("/path/to/dataset")
 *     .columnName("embedding")
 *     .metricType(MetricType.L2)
 *     .nprobes(20)
 *     .build();
 * 
 * List<SearchResult> results = search.search(queryVector, 10);
 * }</pre>
 */
public class LanceVectorSearch implements Closeable, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceVectorSearch.class);

    private final String datasetPath;
    private final String columnName;
    private final MetricType metricType;
    private final int nprobes;
    private final int ef;
    private final Integer refineFactor;

    private transient BufferAllocator allocator;
    private transient Dataset dataset;
    private transient RowType rowType;
    private transient RowDataConverter converter;

    private LanceVectorSearch(Builder builder) {
        this.datasetPath = builder.datasetPath;
        this.columnName = builder.columnName;
        this.metricType = builder.metricType;
        this.nprobes = builder.nprobes;
        this.ef = builder.ef;
        this.refineFactor = builder.refineFactor;
    }

    /**
     * 打开数据集连接
     */
    public void open() throws IOException {
        LOG.info("打开向量检索，数据集: {}", datasetPath);
        
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        
        try {
            this.dataset = Dataset.open(datasetPath, allocator);
            
            // 获取 Schema 并创建转换器
            Schema arrowSchema = dataset.getSchema();
            this.rowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
            this.converter = new RowDataConverter(rowType);
            
        } catch (Exception e) {
            throw new IOException("无法打开数据集: " + datasetPath, e);
        }
    }

    /**
     * 执行向量检索
     *
     * @param queryVector 查询向量
     * @param k 返回的最近邻数量
     * @return 检索结果列表
     */
    public List<SearchResult> search(float[] queryVector, int k) throws IOException {
        return search(queryVector, k, null);
    }

    /**
     * 执行向量检索（带过滤条件）
     *
     * @param queryVector 查询向量
     * @param k 返回的最近邻数量
     * @param filter 过滤条件（SQL WHERE 语法）
     * @return 检索结果列表
     */
    public List<SearchResult> search(float[] queryVector, int k, String filter) throws IOException {
        if (dataset == null) {
            open();
        }
        
        LOG.debug("执行向量检索，k={}, 向量维度={}", k, queryVector.length);
        
        // 验证查询向量
        validateQueryVector(queryVector);
        
        List<SearchResult> results = new ArrayList<>();
        
        try {
            // 构建向量查询
            Query.Builder queryBuilder = new Query.Builder()
                    .setColumn(columnName)
                    .setKey(queryVector)
                    .setK(k)
                    .setNprobes(nprobes)
                    .setDistanceType(toDistanceType(metricType))
                    .setUseIndex(true);
            
            if (ef > 0) {
                queryBuilder.setEf(ef);
            }
            
            if (refineFactor != null && refineFactor > 0) {
                queryBuilder.setRefineFactor(refineFactor);
            }
            
            Query query = queryBuilder.build();
            
            // 构建扫描选项
            ScanOptions.Builder scanOptionsBuilder = new ScanOptions.Builder()
                    .nearest(query)
                    .withRowId(true);
            
            if (filter != null && !filter.isEmpty()) {
                scanOptionsBuilder.filter(filter);
            }
            
            ScanOptions scanOptions = scanOptionsBuilder.build();
            
            // 执行检索
            try (LanceScanner scanner = dataset.newScan(scanOptions)) {
                try (ArrowReader reader = scanner.scanBatches()) {
                    while (reader.loadNextBatch()) {
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        
                        // 转换为 RowData
                        List<RowData> rows = converter.toRowDataList(root);
                        
                        // 尝试获取距离分数（如果存在 _distance 列）
                        Float8Vector distanceVector = null;
                        try {
                            distanceVector = (Float8Vector) root.getVector("_distance");
                        } catch (Exception e) {
                            // _distance 列可能不存在
                        }
                        
                        for (int i = 0; i < rows.size(); i++) {
                            double distance = 0.0;
                            if (distanceVector != null && !distanceVector.isNull(i)) {
                                distance = distanceVector.get(i);
                            }
                            results.add(new SearchResult(rows.get(i), distance));
                        }
                    }
                }
            }
            
            LOG.debug("检索完成，返回 {} 个结果", results.size());
            return results;
            
        } catch (Exception e) {
            throw new IOException("向量检索失败", e);
        }
    }

    /**
     * 执行向量检索（返回 RowData 列表）
     *
     * @param queryVector 查询向量
     * @param k 返回的最近邻数量
     * @return RowData 列表
     */
    public List<RowData> searchRowData(float[] queryVector, int k) throws IOException {
        List<SearchResult> results = search(queryVector, k);
        List<RowData> rowDataList = new ArrayList<>(results.size());
        
        for (SearchResult result : results) {
            // 将距离分数附加到 RowData 中
            GenericRowData rowWithDistance = new GenericRowData(rowType.getFieldCount() + 1);
            RowData originalRow = result.getRowData();
            
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                rowWithDistance.setField(i, getFieldValue(originalRow, i));
            }
            rowWithDistance.setField(rowType.getFieldCount(), result.getDistance());
            
            rowDataList.add(rowWithDistance);
        }
        
        return rowDataList;
    }

    /**
     * 获取 RowData 中的字段值
     */
    private Object getFieldValue(RowData rowData, int index) {
        if (rowData.isNullAt(index)) {
            return null;
        }
        
        // 这里简化处理，实际应根据字段类型获取
        if (rowData instanceof GenericRowData) {
            return ((GenericRowData) rowData).getField(index);
        }
        
        return null;
    }

    /**
     * 验证查询向量
     */
    private void validateQueryVector(float[] queryVector) throws IOException {
        if (queryVector == null || queryVector.length == 0) {
            throw new IllegalArgumentException("查询向量不能为空");
        }
        
        // 检查是否有 NaN 或 Infinity
        for (float value : queryVector) {
            if (Float.isNaN(value) || Float.isInfinite(value)) {
                throw new IllegalArgumentException("查询向量包含无效值 (NaN 或 Infinity)");
            }
        }
    }

    /**
     * 转换距离度量类型
     */
    private DistanceType toDistanceType(MetricType metricType) {
        switch (metricType) {
            case L2:
                return DistanceType.L2;
            case COSINE:
                return DistanceType.Cosine;
            case DOT:
                return DistanceType.Dot;
            default:
                return DistanceType.L2;
        }
    }

    @Override
    public void close() throws IOException {
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
     * 创建构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 从 LanceOptions 创建向量检索器
     */
    public static LanceVectorSearch fromOptions(LanceOptions options) {
        return builder()
                .datasetPath(options.getPath())
                .columnName(options.getVectorColumn())
                .metricType(options.getVectorMetric())
                .nprobes(options.getVectorNprobes())
                .ef(options.getVectorEf())
                .refineFactor(options.getVectorRefineFactor())
                .build();
    }

    /**
     * 构建器
     */
    public static class Builder {
        private String datasetPath;
        private String columnName;
        private MetricType metricType = MetricType.L2;
        private int nprobes = 20;
        private int ef = 100;
        private Integer refineFactor;

        public Builder datasetPath(String datasetPath) {
            this.datasetPath = datasetPath;
            return this;
        }

        public Builder columnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder metricType(MetricType metricType) {
            this.metricType = metricType;
            return this;
        }

        public Builder nprobes(int nprobes) {
            this.nprobes = nprobes;
            return this;
        }

        public Builder ef(int ef) {
            this.ef = ef;
            return this;
        }

        public Builder refineFactor(Integer refineFactor) {
            this.refineFactor = refineFactor;
            return this;
        }

        public LanceVectorSearch build() {
            validate();
            return new LanceVectorSearch(this);
        }

        private void validate() {
            if (datasetPath == null || datasetPath.isEmpty()) {
                throw new IllegalArgumentException("数据集路径不能为空");
            }
            if (columnName == null || columnName.isEmpty()) {
                throw new IllegalArgumentException("列名不能为空");
            }
            if (nprobes <= 0) {
                throw new IllegalArgumentException("nprobes 必须大于 0");
            }
        }
    }

    /**
     * 检索结果
     */
    public static class SearchResult implements Serializable {
        private static final long serialVersionUID = 1L;

        private final RowData rowData;
        private final double distance;

        public SearchResult(RowData rowData, double distance) {
            this.rowData = rowData;
            this.distance = distance;
        }

        public RowData getRowData() {
            return rowData;
        }

        public double getDistance() {
            return distance;
        }

        /**
         * 获取相似度分数（距离的倒数或负数，取决于距离类型）
         */
        public double getSimilarity() {
            if (distance == 0) {
                return 1.0;
            }
            // 对于 L2 距离，使用 1 / (1 + distance) 作为相似度
            return 1.0 / (1.0 + distance);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SearchResult that = (SearchResult) o;
            return Double.compare(that.distance, distance) == 0 &&
                    Objects.equals(rowData, that.rowData);
        }

        @Override
        public int hashCode() {
            return Objects.hash(rowData, distance);
        }

        @Override
        public String toString() {
            return "SearchResult{" +
                    "rowData=" + rowData +
                    ", distance=" + distance +
                    '}';
        }
    }
}

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
import org.apache.flink.connector.lance.config.LanceOptions.IndexType;
import org.apache.flink.connector.lance.config.LanceOptions.MetricType;
import org.apache.flink.connector.lance.config.LanceOptions.WriteMode;
import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.connector.lance.converter.RowDataConverter;
import org.apache.flink.connector.lance.table.LanceCatalog;
import org.apache.flink.connector.lance.table.LanceDynamicTableFactory;
import org.apache.flink.connector.lance.table.LanceDynamicTableSink;
import org.apache.flink.connector.lance.table.LanceDynamicTableSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lance Connector 端到端集成测试。
 */
class LanceConnectorITCase {

    @TempDir
    Path tempDir;

    private String datasetPath;
    private String warehousePath;
    private RowType rowType;
    private DataType dataType;

    @BeforeEach
    void setUp() {
        datasetPath = tempDir.resolve("test_e2e_dataset").toString();
        warehousePath = tempDir.resolve("test_e2e_warehouse").toString();

        // 创建测试 Schema
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("id", new BigIntType()));
        fields.add(new RowType.RowField("content", new VarCharType()));
        fields.add(new RowType.RowField("embedding", new ArrayType(new FloatType())));
        rowType = new RowType(fields);

        dataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("content", DataTypes.STRING()),
                DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
        );
    }

    @Test
    @DisplayName("测试完整的配置选项流程")
    void testCompleteOptionsWorkflow() {
        // 构建完整配置
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                // Source 配置
                .readBatchSize(512)
                .readColumns(Arrays.asList("id", "content", "embedding"))
                .readFilter("id > 0")
                // Sink 配置
                .writeBatchSize(256)
                .writeMode(WriteMode.APPEND)
                .writeMaxRowsPerFile(100000)
                // 索引配置
                .indexType(IndexType.IVF_PQ)
                .indexColumn("embedding")
                .indexNumPartitions(128)
                .indexNumSubVectors(16)
                .indexNumBits(8)
                // 向量检索配置
                .vectorColumn("embedding")
                .vectorMetric(MetricType.L2)
                .vectorNprobes(20)
                .vectorEf(100)
                // Catalog 配置
                .defaultDatabase("default")
                .warehouse(warehousePath)
                .build();

        // 验证所有配置
        assertThat(options.getPath()).isEqualTo(datasetPath);
        assertThat(options.getReadBatchSize()).isEqualTo(512);
        assertThat(options.getReadColumns()).containsExactly("id", "content", "embedding");
        assertThat(options.getReadFilter()).isEqualTo("id > 0");
        assertThat(options.getWriteBatchSize()).isEqualTo(256);
        assertThat(options.getWriteMode()).isEqualTo(WriteMode.APPEND);
        assertThat(options.getWriteMaxRowsPerFile()).isEqualTo(100000);
        assertThat(options.getIndexType()).isEqualTo(IndexType.IVF_PQ);
        assertThat(options.getIndexColumn()).isEqualTo("embedding");
        assertThat(options.getIndexNumPartitions()).isEqualTo(128);
        assertThat(options.getIndexNumSubVectors()).isEqualTo(16);
        assertThat(options.getIndexNumBits()).isEqualTo(8);
        assertThat(options.getVectorColumn()).isEqualTo("embedding");
        assertThat(options.getVectorMetric()).isEqualTo(MetricType.L2);
        assertThat(options.getVectorNprobes()).isEqualTo(20);
        assertThat(options.getVectorEf()).isEqualTo(100);
        assertThat(options.getDefaultDatabase()).isEqualTo("default");
        assertThat(options.getWarehouse()).isEqualTo(warehousePath);
    }

    @Test
    @DisplayName("测试 RowDataConverter 数据转换流程")
    void testRowDataConverterWorkflow() {
        RowDataConverter converter = new RowDataConverter(rowType);

        // 创建测试数据
        List<RowData> testData = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            GenericRowData row = new GenericRowData(3);
            row.setField(0, (long) i);
            row.setField(1, StringData.fromString("Content " + i));

            // 创建向量数据
            Float[] vector = new Float[128];
            for (int j = 0; j < 128; j++) {
                vector[j] = (float) (i * 0.1 + j * 0.01);
            }
            row.setField(2, new GenericArrayData(vector));

            testData.add(row);
        }

        // 验证数据创建成功
        assertThat(testData).hasSize(10);
        assertThat(converter.getRowType()).isEqualTo(rowType);
        assertThat(converter.getFieldNames()).containsExactly("id", "content", "embedding");
    }

    @Test
    @DisplayName("测试 LanceSource 构建器模式")
    void testLanceSourceBuilder() {
        LanceSource source = LanceSource.builder()
                .path(datasetPath)
                .batchSize(256)
                .columns(Arrays.asList("id", "embedding"))
                .filter("id < 1000")
                .rowType(rowType)
                .build();

        assertThat(source.getOptions().getPath()).isEqualTo(datasetPath);
        assertThat(source.getOptions().getReadBatchSize()).isEqualTo(256);
        assertThat(source.getSelectedColumns()).containsExactly("id", "embedding");
        assertThat(source.getRowType()).isEqualTo(rowType);
    }

    @Test
    @DisplayName("测试 LanceSink 构建器模式")
    void testLanceSinkBuilder() {
        LanceSink sink = LanceSink.builder()
                .path(datasetPath)
                .batchSize(128)
                .writeMode(WriteMode.OVERWRITE)
                .maxRowsPerFile(50000)
                .rowType(rowType)
                .build();

        assertThat(sink.getOptions().getPath()).isEqualTo(datasetPath);
        assertThat(sink.getOptions().getWriteBatchSize()).isEqualTo(128);
        assertThat(sink.getOptions().getWriteMode()).isEqualTo(WriteMode.OVERWRITE);
        assertThat(sink.getOptions().getWriteMaxRowsPerFile()).isEqualTo(50000);
        assertThat(sink.getRowType()).isEqualTo(rowType);
    }

    @Test
    @DisplayName("测试 LanceIndexBuilder 构建器模式")
    void testLanceIndexBuilder() {
        LanceIndexBuilder builder = LanceIndexBuilder.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .indexType(IndexType.IVF_HNSW)
                .metricType(MetricType.COSINE)
                .numPartitions(64)
                .maxLevel(5)
                .m(24)
                .efConstruction(200)
                .replace(true)
                .build();

        assertThat(builder).isNotNull();
    }

    @Test
    @DisplayName("测试 LanceVectorSearch 构建器模式")
    void testLanceVectorSearchBuilder() {
        LanceVectorSearch search = LanceVectorSearch.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .metricType(MetricType.DOT)
                .nprobes(30)
                .ef(150)
                .refineFactor(5)
                .build();

        assertThat(search).isNotNull();
    }

    @Test
    @DisplayName("测试 Table API 组件创建")
    void testTableApiComponents() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .build();

        // 创建 DynamicTableSource
        LanceDynamicTableSource source = new LanceDynamicTableSource(options, dataType);
        assertThat(source.asSummaryString()).isEqualTo("Lance Table Source");

        // 创建 DynamicTableSink
        LanceDynamicTableSink sink = new LanceDynamicTableSink(options, dataType);
        assertThat(sink.asSummaryString()).isEqualTo("Lance Table Sink");

        // 创建 Factory
        LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
        assertThat(factory.factoryIdentifier()).isEqualTo("lance");
    }

    @Test
    @DisplayName("测试 Catalog 生命周期")
    void testCatalogLifecycle() throws Exception {
        LanceCatalog catalog = new LanceCatalog("test_catalog", "default", warehousePath);

        // 打开 Catalog
        catalog.open();
        assertThat(catalog.getDefaultDatabase()).isEqualTo("default");
        assertThat(catalog.getWarehouse()).isEqualTo(warehousePath);

        // 验证默认数据库存在
        assertThat(catalog.databaseExists("default")).isTrue();

        // 创建测试数据库
        catalog.createDatabase("test_db", null, true);
        assertThat(catalog.databaseExists("test_db")).isTrue();
        assertThat(catalog.listDatabases()).contains("default", "test_db");

        // 列举空表
        assertThat(catalog.listTables("test_db")).isEmpty();

        // 删除测试数据库
        catalog.dropDatabase("test_db", true, true);
        assertThat(catalog.databaseExists("test_db")).isFalse();

        // 关闭 Catalog
        catalog.close();
    }

    @Test
    @DisplayName("测试类型转换双向一致性")
    void testTypeConversionConsistency() {
        // Flink RowType -> Arrow Schema -> Flink RowType
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = 
                LanceTypeConverter.toArrowSchema(rowType);
        RowType convertedRowType = LanceTypeConverter.toFlinkRowType(arrowSchema);

        // 验证字段数量
        assertThat(convertedRowType.getFieldCount()).isEqualTo(rowType.getFieldCount());

        // 验证字段名称
        assertThat(convertedRowType.getFieldNames()).isEqualTo(rowType.getFieldNames());
    }

    @Test
    @DisplayName("测试向量数据转换")
    void testVectorDataConversion() {
        // 创建 float 数组
        float[] originalVector = new float[] {0.1f, 0.2f, 0.3f, 0.4f, 0.5f};

        // 转换为 ArrayData
        org.apache.flink.table.data.ArrayData arrayData = 
                RowDataConverter.toArrayData(originalVector);

        // 转换回 float 数组
        float[] convertedVector = RowDataConverter.toFloatArray(arrayData);

        // 验证一致性
        assertThat(convertedVector).containsExactly(originalVector);
    }

    @Test
    @DisplayName("测试 double 向量数据转换")
    void testDoubleVectorDataConversion() {
        // 创建 double 数组
        double[] originalVector = new double[] {0.1, 0.2, 0.3, 0.4, 0.5};

        // 转换为 ArrayData
        org.apache.flink.table.data.ArrayData arrayData = 
                RowDataConverter.toArrayData(originalVector);

        // 转换回 double 数组
        double[] convertedVector = RowDataConverter.toDoubleArray(arrayData);

        // 验证一致性
        assertThat(convertedVector).containsExactly(originalVector);
    }

    @Test
    @DisplayName("测试 LanceSplit 序列化兼容性")
    void testLanceSplitSerialization() {
        LanceSplit split1 = new LanceSplit(0, 1, datasetPath, 10000);
        LanceSplit split2 = new LanceSplit(0, 1, datasetPath, 10000);
        LanceSplit split3 = new LanceSplit(1, 2, datasetPath, 20000);

        // 相等性测试
        assertThat(split1).isEqualTo(split2);
        assertThat(split1.hashCode()).isEqualTo(split2.hashCode());
        assertThat(split1).isNotEqualTo(split3);

        // toString 测试
        String str = split1.toString();
        assertThat(str).contains("LanceSplit");
        assertThat(str).contains("fragmentId=1");
        assertThat(str).contains("rowCount=10000");
    }

    @Test
    @DisplayName("测试搜索结果相似度计算")
    void testSearchResultSimilarityCalculation() {
        // 完全匹配（距离=0）
        LanceVectorSearch.SearchResult perfectMatch = 
                new LanceVectorSearch.SearchResult(null, 0.0);
        assertThat(perfectMatch.getSimilarity()).isEqualTo(1.0);

        // 一般匹配（距离=1）
        LanceVectorSearch.SearchResult normalMatch = 
                new LanceVectorSearch.SearchResult(null, 1.0);
        assertThat(normalMatch.getSimilarity()).isEqualTo(0.5);

        // 较远匹配（距离=9）
        LanceVectorSearch.SearchResult farMatch = 
                new LanceVectorSearch.SearchResult(null, 9.0);
        assertThat(farMatch.getSimilarity()).isEqualTo(0.1);
    }

    @Test
    @DisplayName("测试配置 toString 和 hashCode")
    void testOptionsToStringAndHashCode() {
        LanceOptions options1 = LanceOptions.builder()
                .path(datasetPath)
                .readBatchSize(512)
                .build();

        LanceOptions options2 = LanceOptions.builder()
                .path(datasetPath)
                .readBatchSize(512)
                .build();

        // hashCode 相等
        assertThat(options1.hashCode()).isEqualTo(options2.hashCode());

        // equals 相等
        assertThat(options1).isEqualTo(options2);

        // toString 包含关键信息
        String str = options1.toString();
        assertThat(str).contains("LanceOptions");
        assertThat(str).contains("readBatchSize=512");
    }

    @Test
    @DisplayName("测试所有枚举类型")
    void testAllEnumTypes() {
        // WriteMode
        assertThat(WriteMode.values()).hasSize(2);
        assertThat(WriteMode.APPEND.getValue()).isEqualTo("append");
        assertThat(WriteMode.OVERWRITE.getValue()).isEqualTo("overwrite");

        // IndexType
        assertThat(IndexType.values()).hasSize(3);
        assertThat(IndexType.IVF_PQ.getValue()).isEqualTo("IVF_PQ");
        assertThat(IndexType.IVF_HNSW.getValue()).isEqualTo("IVF_HNSW");
        assertThat(IndexType.IVF_FLAT.getValue()).isEqualTo("IVF_FLAT");

        // MetricType
        assertThat(MetricType.values()).hasSize(3);
        assertThat(MetricType.L2.getValue()).isEqualTo("L2");
        assertThat(MetricType.COSINE.getValue()).isEqualTo("Cosine");
        assertThat(MetricType.DOT.getValue()).isEqualTo("Dot");
    }
}

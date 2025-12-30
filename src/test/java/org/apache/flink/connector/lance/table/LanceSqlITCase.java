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

package org.apache.flink.connector.lance.table;

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Lance SQL 集成测试。
 */
class LanceSqlITCase {

    @TempDir
    Path tempDir;

    private String datasetPath;
    private String warehousePath;

    @BeforeEach
    void setUp() {
        datasetPath = tempDir.resolve("test_sql_dataset").toString();
        warehousePath = tempDir.resolve("test_warehouse").toString();
    }

    @Test
    @DisplayName("测试 LanceDynamicTableFactory 标识符")
    void testFactoryIdentifier() {
        LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
        assertThat(factory.factoryIdentifier()).isEqualTo("lance");
    }

    @Test
    @DisplayName("测试 LanceDynamicTableFactory 必需选项")
    void testRequiredOptions() {
        LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
        Set<String> requiredOptionKeys = new HashSet<>();
        factory.requiredOptions().forEach(opt -> requiredOptionKeys.add(opt.key()));

        assertThat(requiredOptionKeys).contains("path");
    }

    @Test
    @DisplayName("测试 LanceDynamicTableFactory 可选选项")
    void testOptionalOptions() {
        LanceDynamicTableFactory factory = new LanceDynamicTableFactory();
        Set<String> optionalOptionKeys = new HashSet<>();
        factory.optionalOptions().forEach(opt -> optionalOptionKeys.add(opt.key()));

        assertThat(optionalOptionKeys).contains(
                "read.batch-size",
                "read.columns",
                "read.filter",
                "write.batch-size",
                "write.mode",
                "write.max-rows-per-file",
                "index.type",
                "index.column",
                "vector.column",
                "vector.metric"
        );
    }

    @Test
    @DisplayName("测试 LanceDynamicTableSource 创建")
    void testDynamicTableSourceCreation() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .readBatchSize(512)
                .build();

        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("id", new BigIntType()));
        fields.add(new RowType.RowField("content", new VarCharType()));
        fields.add(new RowType.RowField("embedding", new ArrayType(new FloatType())));
        RowType rowType = new RowType(fields);

        DataType dataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("content", DataTypes.STRING()),
                DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
        );

        LanceDynamicTableSource source = new LanceDynamicTableSource(options, dataType);

        assertThat(source.getOptions()).isEqualTo(options);
        assertThat(source.getPhysicalDataType()).isEqualTo(dataType);
        assertThat(source.asSummaryString()).isEqualTo("Lance Table Source");
    }

    @Test
    @DisplayName("测试 LanceDynamicTableSink 创建")
    void testDynamicTableSinkCreation() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .writeBatchSize(256)
                .writeMode(LanceOptions.WriteMode.APPEND)
                .build();

        DataType dataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("content", DataTypes.STRING()),
                DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT()))
        );

        LanceDynamicTableSink sink = new LanceDynamicTableSink(options, dataType);

        assertThat(sink.getOptions()).isEqualTo(options);
        assertThat(sink.getPhysicalDataType()).isEqualTo(dataType);
        assertThat(sink.asSummaryString()).isEqualTo("Lance Table Sink");
    }

    @Test
    @DisplayName("测试 LanceDynamicTableSource 复制")
    void testDynamicTableSourceCopy() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .build();

        DataType dataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT())
        );

        LanceDynamicTableSource source = new LanceDynamicTableSource(options, dataType);
        LanceDynamicTableSource copiedSource = (LanceDynamicTableSource) source.copy();

        assertThat(copiedSource).isNotSameAs(source);
        assertThat(copiedSource.getOptions()).isEqualTo(source.getOptions());
    }

    @Test
    @DisplayName("测试 LanceDynamicTableSink 复制")
    void testDynamicTableSinkCopy() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .build();

        DataType dataType = DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT())
        );

        LanceDynamicTableSink sink = new LanceDynamicTableSink(options, dataType);
        LanceDynamicTableSink copiedSink = (LanceDynamicTableSink) sink.copy();

        assertThat(copiedSink).isNotSameAs(sink);
        assertThat(copiedSink.getOptions()).isEqualTo(sink.getOptions());
    }

    @Test
    @DisplayName("测试 LanceCatalogFactory 标识符")
    void testCatalogFactoryIdentifier() {
        LanceCatalogFactory factory = new LanceCatalogFactory();
        assertThat(factory.factoryIdentifier()).isEqualTo("lance");
    }

    @Test
    @DisplayName("测试 LanceCatalogFactory 必需选项")
    void testCatalogRequiredOptions() {
        LanceCatalogFactory factory = new LanceCatalogFactory();
        Set<String> requiredOptionKeys = new HashSet<>();
        factory.requiredOptions().forEach(opt -> requiredOptionKeys.add(opt.key()));

        assertThat(requiredOptionKeys).contains("warehouse");
    }

    @Test
    @DisplayName("测试 LanceCatalogFactory 可选选项")
    void testCatalogOptionalOptions() {
        LanceCatalogFactory factory = new LanceCatalogFactory();
        Set<String> optionalOptionKeys = new HashSet<>();
        factory.optionalOptions().forEach(opt -> optionalOptionKeys.add(opt.key()));

        assertThat(optionalOptionKeys).contains("default-database");
    }

    @Test
    @DisplayName("测试 LanceCatalog 创建和基本操作")
    void testLanceCatalogBasicOperations() throws Exception {
        LanceCatalog catalog = new LanceCatalog("test_catalog", "default", warehousePath);
        
        try {
            catalog.open();
            
            // 验证默认数据库存在
            assertThat(catalog.databaseExists("default")).isTrue();
            
            // 列举数据库
            List<String> databases = catalog.listDatabases();
            assertThat(databases).contains("default");
            
            // 创建新数据库
            catalog.createDatabase("test_db", null, false);
            assertThat(catalog.databaseExists("test_db")).isTrue();
            
            // 列举表（空）
            List<String> tables = catalog.listTables("test_db");
            assertThat(tables).isEmpty();
            
            // 删除数据库
            catalog.dropDatabase("test_db", false, true);
            assertThat(catalog.databaseExists("test_db")).isFalse();
            
        } finally {
            catalog.close();
        }
    }

    @Test
    @DisplayName("测试 LanceCatalog 仓库路径")
    void testLanceCatalogWarehouse() throws Exception {
        LanceCatalog catalog = new LanceCatalog("test", "default", warehousePath);
        
        try {
            catalog.open();
            assertThat(catalog.getWarehouse()).isEqualTo(warehousePath);
        } finally {
            catalog.close();
        }
    }

    @Test
    @DisplayName("测试配置选项定义")
    void testConfigOptions() {
        assertThat(LanceDynamicTableFactory.PATH.key()).isEqualTo("path");
        assertThat(LanceDynamicTableFactory.READ_BATCH_SIZE.key()).isEqualTo("read.batch-size");
        assertThat(LanceDynamicTableFactory.READ_BATCH_SIZE.defaultValue()).isEqualTo(1024);
        assertThat(LanceDynamicTableFactory.WRITE_BATCH_SIZE.key()).isEqualTo("write.batch-size");
        assertThat(LanceDynamicTableFactory.WRITE_MODE.key()).isEqualTo("write.mode");
        assertThat(LanceDynamicTableFactory.WRITE_MODE.defaultValue()).isEqualTo("append");
        assertThat(LanceDynamicTableFactory.INDEX_TYPE.key()).isEqualTo("index.type");
        assertThat(LanceDynamicTableFactory.INDEX_TYPE.defaultValue()).isEqualTo("IVF_PQ");
        assertThat(LanceDynamicTableFactory.VECTOR_METRIC.key()).isEqualTo("vector.metric");
        assertThat(LanceDynamicTableFactory.VECTOR_METRIC.defaultValue()).isEqualTo("L2");
    }

    @Test
    @DisplayName("测试 Catalog 配置选项定义")
    void testCatalogConfigOptions() {
        assertThat(LanceCatalogFactory.WAREHOUSE.key()).isEqualTo("warehouse");
        assertThat(LanceCatalogFactory.DEFAULT_DATABASE.key()).isEqualTo("default-database");
        assertThat(LanceCatalogFactory.DEFAULT_DATABASE.defaultValue()).isEqualTo("default");
    }

    @Test
    @DisplayName("测试向量检索 UDF 配置")
    void testVectorSearchFunctionConfiguration() {
        LanceVectorSearchFunction function = new LanceVectorSearchFunction();
        assertThat(function).isNotNull();
    }
}

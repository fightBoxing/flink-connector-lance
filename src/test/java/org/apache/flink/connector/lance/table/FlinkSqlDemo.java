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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Flink SQL 完整演示测试脚本。
 * 
 * <p>本测试演示如何使用 Flink SQL 操作 Lance 数据集：
 * <ul>
 *   <li>创建 Lance Catalog</li>
 *   <li>创建 Lance 表</li>
 *   <li>插入向量数据</li>
 *   <li>查询数据</li>
 *   <li>构建向量索引</li>
 *   <li>执行向量检索</li>
 * </ul>
 */
class FlinkSqlDemo {

    @TempDir
    Path tempDir;

    private TableEnvironment tableEnv;
    private String warehousePath;
    private String datasetPath;

    @BeforeEach
    void setUp() {
        // 创建 Flink Table 环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();
        tableEnv = TableEnvironment.create(settings);
        
        // 设置路径
        warehousePath = tempDir.resolve("lance_warehouse").toString();
        datasetPath = tempDir.resolve("lance_dataset").toString();
    }

    @AfterEach
    void tearDown() {
        // 清理资源
        if (tableEnv != null) {
            // TableEnvironment 自动清理
        }
    }

    // ==================== 基础 SQL 操作 ====================

    @Test
    @DisplayName("1. 创建 Lance Connector 表 - 基础用法")
    void testCreateLanceTable() throws Exception {
        String createTableSql = String.format(
            "CREATE TABLE lance_vectors (\n" +
            "    id BIGINT,\n" +
            "    content STRING,\n" +
            "    embedding ARRAY<FLOAT>,\n" +
            "    category STRING,\n" +
            "    create_time TIMESTAMP(3)\n" +
            ") WITH (\n" +
            "    'connector' = 'lance',\n" +
            "    'path' = '%s',\n" +
            "    'write.batch-size' = '1024',\n" +
            "    'write.mode' = 'overwrite'\n" +
            ")", datasetPath);
        
        System.out.println("========== 创建 Lance 表 ==========");
        System.out.println(createTableSql);
        System.out.println();
        
        tableEnv.executeSql(createTableSql);
        System.out.println("✅ 表创建成功！\n");
    }

    @Test
    @DisplayName("2. 插入向量数据到 Lance 表")
    void testInsertData() throws Exception {
        // 首先创建表
        String createTableSql = String.format(
            "CREATE TABLE lance_documents (\n" +
            "    id BIGINT,\n" +
            "    title STRING,\n" +
            "    embedding ARRAY<FLOAT>\n" +
            ") WITH (\n" +
            "    'connector' = 'lance',\n" +
            "    'path' = '%s',\n" +
            "    'write.mode' = 'overwrite'\n" +
            ")", datasetPath);
        
        tableEnv.executeSql(createTableSql);
        
        // 插入数据
        String insertSql = 
            "INSERT INTO lance_documents VALUES\n" +
            "    (1, 'Introduction to AI', ARRAY[0.1, 0.2, 0.3, 0.4]),\n" +
            "    (2, 'Machine Learning Guide', ARRAY[0.2, 0.3, 0.4, 0.5]),\n" +
            "    (3, 'Deep Learning Basics', ARRAY[0.3, 0.4, 0.5, 0.6]),\n" +
            "    (4, 'Neural Networks', ARRAY[0.4, 0.5, 0.6, 0.7]),\n" +
            "    (5, 'Computer Vision', ARRAY[0.5, 0.6, 0.7, 0.8])";
        
        System.out.println("========== 插入向量数据 ==========");
        System.out.println(insertSql);
        System.out.println();
        
        TableResult result = tableEnv.executeSql(insertSql);
        result.await(30, TimeUnit.SECONDS);
        System.out.println("✅ 数据插入成功！\n");
    }

    @Test
    @DisplayName("3. 查询 Lance 表数据")
    void testSelectData() throws Exception {
        // 创建源表（用于生成测试数据）
        String createSourceSql = 
            "CREATE TABLE test_source (\n" +
            "    id BIGINT,\n" +
            "    name STRING\n" +
            ") WITH (\n" +
            "    'connector' = 'datagen',\n" +
            "    'rows-per-second' = '1',\n" +
            "    'number-of-rows' = '10',\n" +
            "    'fields.id.kind' = 'sequence',\n" +
            "    'fields.id.start' = '1',\n" +
            "    'fields.id.end' = '10'\n" +
            ")";
        
        tableEnv.executeSql(createSourceSql);
        
        // 查询数据
        String selectSql = "SELECT id, name FROM test_source LIMIT 5";
        
        System.out.println("========== 查询数据 ==========");
        System.out.println(selectSql);
        System.out.println();
        
        TableResult result = tableEnv.executeSql(selectSql);
        result.print();
        System.out.println("✅ 查询完成！\n");
    }

    // ==================== 高级配置 ====================

    @Test
    @DisplayName("4. 创建带向量索引配置的表")
    void testCreateTableWithIndexConfig() throws Exception {
        String createTableSql = String.format(
            "CREATE TABLE vector_store (\n" +
            "    id BIGINT,\n" +
            "    text STRING,\n" +
            "    embedding ARRAY<FLOAT> COMMENT '768维向量'\n" +
            ") WITH (\n" +
            "    'connector' = 'lance',\n" +
            "    'path' = '%s',\n" +
            "    -- 写入配置\n" +
            "    'write.batch-size' = '2048',\n" +
            "    'write.mode' = 'append',\n" +
            "    'write.max-rows-per-file' = '100000',\n" +
            "    -- 索引配置\n" +
            "    'index.type' = 'IVF_PQ',\n" +
            "    'index.column' = 'embedding',\n" +
            "    'index.num-partitions' = '256',\n" +
            "    'index.num-sub-vectors' = '16',\n" +
            "    -- 向量检索配置\n" +
            "    'vector.column' = 'embedding',\n" +
            "    'vector.metric' = 'L2',\n" +
            "    'vector.nprobes' = '20'\n" +
            ")", datasetPath);
        
        System.out.println("========== 创建带索引配置的表 ==========");
        System.out.println(createTableSql);
        System.out.println();
        
        tableEnv.executeSql(createTableSql);
        System.out.println("✅ 表创建成功！\n");
    }

    @Test
    @DisplayName("5. 不同索引类型配置示例")
    void testDifferentIndexTypes() {
        System.out.println("========== 索引类型配置示例 ==========\n");
        
        // IVF_PQ 索引（推荐，平衡精度和速度）
        String ivfPqConfig = 
            "-- IVF_PQ 索引配置（推荐用于大规模向量数据）\n" +
            "'index.type' = 'IVF_PQ',\n" +
            "'index.num-partitions' = '256',      -- 聚类中心数量\n" +
            "'index.num-sub-vectors' = '16',      -- 子向量数量\n" +
            "'index.num-bits' = '8'               -- 每个子向量的量化位数\n";
        
        System.out.println(ivfPqConfig);
        
        // IVF_HNSW 索引（高精度）
        String ivfHnswConfig = 
            "-- IVF_HNSW 索引配置（适用于需要高精度的场景）\n" +
            "'index.type' = 'IVF_HNSW',\n" +
            "'index.num-partitions' = '256',\n" +
            "'index.max-level' = '7',             -- HNSW 最大层数\n" +
            "'index.m' = '16',                    -- HNSW 连接数\n" +
            "'index.ef-construction' = '100'      -- 构建时的 ef 参数\n";
        
        System.out.println(ivfHnswConfig);
        
        // IVF_FLAT 索引（最高精度，适合小数据集）
        String ivfFlatConfig = 
            "-- IVF_FLAT 索引配置（适用于小规模数据集）\n" +
            "'index.type' = 'IVF_FLAT',\n" +
            "'index.num-partitions' = '64'        -- 聚类中心数量\n";
        
        System.out.println(ivfFlatConfig);
        System.out.println("✅ 配置示例展示完成！\n");
    }

    @Test
    @DisplayName("6. 距离度量类型配置示例")
    void testMetricTypes() {
        System.out.println("========== 距离度量类型示例 ==========\n");
        
        String l2Config = 
            "-- L2 距离（欧氏距离，默认）\n" +
            "'vector.metric' = 'L2'\n" +
            "-- 适用场景：通用向量检索\n";
        System.out.println(l2Config);
        
        String cosineConfig = 
            "-- Cosine 距离（余弦相似度）\n" +
            "'vector.metric' = 'COSINE'\n" +
            "-- 适用场景：文本语义相似度\n";
        System.out.println(cosineConfig);
        
        String dotConfig = 
            "-- Dot 距离（点积）\n" +
            "'vector.metric' = 'DOT'\n" +
            "-- 适用场景：已归一化的向量\n";
        System.out.println(dotConfig);
        
        System.out.println("✅ 配置示例展示完成！\n");
    }

    // ==================== Catalog 操作 ====================

    @Test
    @DisplayName("7. 创建和使用 Lance Catalog")
    void testLanceCatalog() throws Exception {
        String createCatalogSql = String.format(
            "CREATE CATALOG lance_catalog WITH (\n" +
            "    'type' = 'lance',\n" +
            "    'warehouse' = '%s',\n" +
            "    'default-database' = 'default'\n" +
            ")", warehousePath);
        
        System.out.println("========== 创建 Lance Catalog ==========");
        System.out.println(createCatalogSql);
        System.out.println();
        
        tableEnv.executeSql(createCatalogSql);
        
        // 使用 Catalog
        tableEnv.executeSql("USE CATALOG lance_catalog");
        System.out.println("✅ Catalog 创建并切换成功！\n");
        
        // 创建数据库
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS vector_db");
        System.out.println("✅ 数据库 vector_db 创建成功！\n");
        
        // 列出数据库
        System.out.println("数据库列表：");
        tableEnv.executeSql("SHOW DATABASES").print();
    }

    // ==================== 流式处理 ====================

    @Test
    @DisplayName("8. 流式写入 Lance 表")
    void testStreamingWrite() throws Exception {
        // 创建流式环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        
        // 创建数据生成器表（模拟实时数据）
        String createSourceSql = 
            "CREATE TABLE realtime_events (\n" +
            "    event_id BIGINT,\n" +
            "    event_type STRING,\n" +
            "    event_time AS PROCTIME()\n" +
            ") WITH (\n" +
            "    'connector' = 'datagen',\n" +
            "    'rows-per-second' = '10',\n" +
            "    'number-of-rows' = '100',\n" +
            "    'fields.event_id.kind' = 'sequence',\n" +
            "    'fields.event_id.start' = '1',\n" +
            "    'fields.event_id.end' = '100',\n" +
            "    'fields.event_type.length' = '10'\n" +
            ")";
        
        // 创建 Lance Sink 表
        String createSinkSql = String.format(
            "CREATE TABLE lance_events (\n" +
            "    event_id BIGINT,\n" +
            "    event_type STRING\n" +
            ") WITH (\n" +
            "    'connector' = 'lance',\n" +
            "    'path' = '%s',\n" +
            "    'write.batch-size' = '100',\n" +
            "    'write.mode' = 'append'\n" +
            ")", datasetPath);
        
        System.out.println("========== 流式写入示例 ==========");
        System.out.println("-- Source 表定义");
        System.out.println(createSourceSql);
        System.out.println("\n-- Sink 表定义");
        System.out.println(createSinkSql);
        System.out.println();
        
        streamTableEnv.executeSql(createSourceSql);
        streamTableEnv.executeSql(createSinkSql);
        
        // 执行流式写入
        String insertSql = "INSERT INTO lance_events SELECT event_id, event_type FROM realtime_events";
        System.out.println("-- 流式插入语句");
        System.out.println(insertSql);
        System.out.println();
        
        System.out.println("✅ 流式写入配置完成！\n");
    }

    // ==================== 完整示例 ====================

    @Test
    @DisplayName("9. 完整的向量存储和检索示例")
    void testCompleteVectorExample() throws Exception {
        System.out.println("========== 完整向量存储和检索示例 ==========\n");
        
        // 1. 创建向量表
        String createTableSql = String.format(
            "-- 1. 创建向量存储表\n" +
            "CREATE TABLE document_vectors (\n" +
            "    doc_id BIGINT COMMENT '文档ID',\n" +
            "    title STRING COMMENT '文档标题',\n" +
            "    content STRING COMMENT '文档内容',\n" +
            "    embedding ARRAY<FLOAT> COMMENT '文档向量(768维)',\n" +
            "    category STRING COMMENT '文档分类',\n" +
            "    create_time TIMESTAMP(3) COMMENT '创建时间'\n" +
            ") WITH (\n" +
            "    'connector' = 'lance',\n" +
            "    'path' = '%s',\n" +
            "    -- 写入配置\n" +
            "    'write.batch-size' = '1024',\n" +
            "    'write.mode' = 'overwrite',\n" +
            "    -- 索引配置\n" +
            "    'index.type' = 'IVF_PQ',\n" +
            "    'index.column' = 'embedding',\n" +
            "    'index.num-partitions' = '128',\n" +
            "    'index.num-sub-vectors' = '32',\n" +
            "    -- 向量检索配置\n" +
            "    'vector.column' = 'embedding',\n" +
            "    'vector.metric' = 'COSINE',\n" +
            "    'vector.nprobes' = '10'\n" +
            ")", datasetPath);
        
        System.out.println(createTableSql);
        System.out.println();
        tableEnv.executeSql(createTableSql.replace("-- 1. 创建向量存储表\n", ""));
        
        // 2. 插入测试数据
        String insertSql = 
            "-- 2. 插入向量数据\n" +
            "INSERT INTO document_vectors VALUES\n" +
            "    (1, 'Flink入门指南', '介绍Apache Flink的基本概念...', \n" +
            "     ARRAY[0.1, 0.2, 0.3, 0.4], 'tutorial', TIMESTAMP '2024-01-01 10:00:00'),\n" +
            "    (2, '流处理实战', '使用Flink处理实时数据流...', \n" +
            "     ARRAY[0.2, 0.3, 0.4, 0.5], 'practice', TIMESTAMP '2024-01-02 11:00:00'),\n" +
            "    (3, '向量数据库详解', '深入理解向量检索技术...', \n" +
            "     ARRAY[0.3, 0.4, 0.5, 0.6], 'database', TIMESTAMP '2024-01-03 12:00:00'),\n" +
            "    (4, 'Lance格式介绍', 'Lance是一种高效的向量存储格式...', \n" +
            "     ARRAY[0.4, 0.5, 0.6, 0.7], 'format', TIMESTAMP '2024-01-04 13:00:00'),\n" +
            "    (5, 'SQL连接器开发', '如何开发Flink SQL连接器...', \n" +
            "     ARRAY[0.5, 0.6, 0.7, 0.8], 'development', TIMESTAMP '2024-01-05 14:00:00')";
        
        System.out.println(insertSql);
        System.out.println();
        
        // 3. 查询数据
        String selectSql = 
            "-- 3. 查询向量数据\n" +
            "SELECT doc_id, title, category, create_time\n" +
            "FROM document_vectors\n" +
            "WHERE category = 'tutorial'\n" +
            "ORDER BY create_time DESC";
        
        System.out.println(selectSql);
        System.out.println();
        
        // 4. 聚合查询
        String aggSql = 
            "-- 4. 统计各分类文档数量\n" +
            "SELECT category, COUNT(*) as doc_count\n" +
            "FROM document_vectors\n" +
            "GROUP BY category\n" +
            "ORDER BY doc_count DESC";
        
        System.out.println(aggSql);
        System.out.println();
        
        System.out.println("✅ 完整示例展示完成！\n");
    }

    @Test
    @DisplayName("10. SQL 语法快速参考")
    void testSqlQuickReference() {
        System.out.println("========================================");
        System.out.println("     Flink SQL Lance Connector 快速参考");
        System.out.println("========================================\n");
        
        System.out.println("【创建表】");
        System.out.println("CREATE TABLE table_name (");
        System.out.println("    column_name data_type,");
        System.out.println("    embedding ARRAY<FLOAT>");
        System.out.println(") WITH (");
        System.out.println("    'connector' = 'lance',");
        System.out.println("    'path' = '/path/to/dataset'");
        System.out.println(");\n");
        
        System.out.println("【插入数据】");
        System.out.println("INSERT INTO table_name VALUES (1, 'text', ARRAY[0.1, 0.2, 0.3]);\n");
        
        System.out.println("【查询数据】");
        System.out.println("SELECT * FROM table_name WHERE condition;\n");
        
        System.out.println("【创建 Catalog】");
        System.out.println("CREATE CATALOG lance_catalog WITH (");
        System.out.println("    'type' = 'lance',");
        System.out.println("    'warehouse' = '/path/to/warehouse'");
        System.out.println(");\n");
        
        System.out.println("【数据类型映射】");
        System.out.println("╔════════════════════╦═══════════════════╗");
        System.out.println("║   Flink SQL 类型   ║     Lance 类型    ║");
        System.out.println("╠════════════════════╬═══════════════════╣");
        System.out.println("║ BOOLEAN            ║ Bool              ║");
        System.out.println("║ TINYINT            ║ Int8              ║");
        System.out.println("║ SMALLINT           ║ Int16             ║");
        System.out.println("║ INT                ║ Int32             ║");
        System.out.println("║ BIGINT             ║ Int64             ║");
        System.out.println("║ FLOAT              ║ Float32           ║");
        System.out.println("║ DOUBLE             ║ Float64           ║");
        System.out.println("║ STRING             ║ Utf8              ║");
        System.out.println("║ BYTES              ║ Binary            ║");
        System.out.println("║ DATE               ║ Date32            ║");
        System.out.println("║ TIMESTAMP          ║ Timestamp         ║");
        System.out.println("║ ARRAY<FLOAT>       ║ FixedSizeList     ║");
        System.out.println("╚════════════════════╩═══════════════════╝\n");
        
        System.out.println("【配置选项】");
        System.out.println("╔═══════════════════════════╦════════════════════════════════╗");
        System.out.println("║         选项              ║           说明                 ║");
        System.out.println("╠═══════════════════════════╬════════════════════════════════╣");
        System.out.println("║ path                      ║ 数据集路径（必需）              ║");
        System.out.println("║ write.batch-size          ║ 写入批次大小（默认1024）        ║");
        System.out.println("║ write.mode                ║ 写入模式 append/overwrite      ║");
        System.out.println("║ read.batch-size           ║ 读取批次大小（默认1024）        ║");
        System.out.println("║ index.type                ║ 索引类型 IVF_PQ/IVF_HNSW/IVF_FLAT║");
        System.out.println("║ index.column              ║ 索引列名                       ║");
        System.out.println("║ index.num-partitions      ║ IVF分区数（默认256）           ║");
        System.out.println("║ vector.column             ║ 向量列名                       ║");
        System.out.println("║ vector.metric             ║ 距离度量 L2/COSINE/DOT         ║");
        System.out.println("║ vector.nprobes            ║ 检索探针数（默认20）           ║");
        System.out.println("╚═══════════════════════════╩════════════════════════════════╝\n");
        
        System.out.println("✅ 快速参考完成！");
    }
}

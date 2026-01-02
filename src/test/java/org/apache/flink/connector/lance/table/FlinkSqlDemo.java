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

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Paths;
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
        // 使用相对路径，基于项目根目录
        Path path = Paths.get(System.getProperty("user.dir"), "test-data");
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
            ")", path.resolve("lance-db1"));
        
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
        // 使用相对路径，基于项目根目录
        Path path = Paths.get(System.getProperty("user.dir"), "test-data");
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
            ")", path.resolve("lance-db3"));
        
        System.out.println(createTableSql);
        System.out.println();
        tableEnv.executeSql(createTableSql);
//        tableEnv.executeSql(createTableSql.replace("-- 1. 创建向量存储表\n", ""));
        
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
        TableResult result = tableEnv.executeSql(insertSql);
        result.await(30, TimeUnit.SECONDS);
        
        // 3. 查询数据
        String selectSql = 
            "-- 3. 查询向量数据\n" +
            "SELECT doc_id, title, category, create_time\n" +
            "FROM document_vectors\n" +
            "WHERE category = 'tutorial'\n" +
            "ORDER BY create_time DESC";
        
        System.out.println(selectSql);
        System.out.println();
        TableResult tableResult = tableEnv.executeSql(selectSql);
        tableResult
                .await(3,TimeUnit.SECONDS);
        CloseableIterator<Row> collect = tableResult.collect();
        while (collect.hasNext()) {
            System.out.println(collect.next());
        }

        // 4. 聚合查询
        String aggSql = 
            "-- 4. 统计各分类文档数量\n" +
            "SELECT category, COUNT(*) as doc_count\n" +
            "FROM document_vectors\n" +
            "GROUP BY category\n" +
            "ORDER BY doc_count DESC";
        
        System.out.println(aggSql);
        System.out.println();
        tableEnv.executeSql(aggSql).print();

        System.out.println("✅ 完整示例展示完成！\n");
    }

    @Test
    @DisplayName("9.1 向量检索 IVF_PQ 索引示例")
    void testVectorSearchWithIvfPq() throws Exception {
        System.out.println("========== 向量检索 IVF_PQ 索引示例 ==========");
        
        // 使用相对路径，基于项目根目录
        Path basePath = Paths.get(System.getProperty("user.dir"), "test-data");
        String datasetPath = basePath.resolve("lance-vector-search").toString();
        
        // ============================================
        // 第一步：创建带有 IVF_PQ 索引配置的向量表
        // ============================================
        String createTableSql = String.format(
            "CREATE TABLE vector_documents (\n" +
            "    id BIGINT,\n" +
            "    title STRING,\n" +
            "    embedding ARRAY<FLOAT>\n" +
            ") WITH (\n" +
            "    'connector' = 'lance',\n" +
            "    'path' = '%s',\n" +
            "    'write.batch-size' = '1024',\n" +
            "    'write.mode' = 'overwrite',\n" +
            "    -- IVF_PQ 索引配置\n" +
            "    'index.type' = 'IVF_PQ',\n" +
            "    'index.column' = 'embedding',\n" +
            "    'index.num-partitions' = '16',\n" +
            "    'index.num-sub-vectors' = '8',\n" +
            "    -- 向量检索配置\n" +
            "    'vector.column' = 'embedding',\n" +
            "    'vector.metric' = 'L2',\n" +
            "    'vector.nprobes' = '10'\n" +
            ")", datasetPath);
        
        System.out.println("-- 步骤1: 创建带有 IVF_PQ 索引配置的向量表");
        System.out.println(createTableSql);
        System.out.println();
        tableEnv.executeSql(createTableSql);
        
        // ============================================
        // 第二步：插入向量数据
        // ============================================
        String insertSql = 
            "INSERT INTO vector_documents VALUES\n" +
            "    (1, 'Flink流处理', ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]),\n" +
            "    (2, 'Spark批处理', ARRAY[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]),\n" +
            "    (3, 'Kafka消息队列', ARRAY[0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]),\n" +
            "    (4, '向量数据库', ARRAY[0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85]),\n" +
            "    (5, '机器学习基础', ARRAY[0.12, 0.22, 0.32, 0.42, 0.52, 0.62, 0.72, 0.82])";
        
        System.out.println("-- 步骤2: 插入向量数据");
        System.out.println(insertSql);
        System.out.println();
        tableEnv.executeSql(insertSql).await(30, TimeUnit.SECONDS);
        System.out.println("✅ 数据插入完成\n");
        
        // ============================================
        // 第三步：注册向量检索 UDF
        // ============================================
        String createFunctionSql = 
            "CREATE TEMPORARY FUNCTION vector_search AS \n" +
            "    'org.apache.flink.connector.lance.table.LanceVectorSearchFunction'";
        
        System.out.println("-- 步骤3: 注册向量检索 UDF");
        System.out.println(createFunctionSql);
        System.out.println();
        tableEnv.executeSql(createFunctionSql);
        System.out.println("✅ UDF 注册完成\n");
        
        // ============================================
        // 第四步：执行向量检索 - 基本用法
        // ============================================
        System.out.println("-- 步骤4: 执行向量检索 (基本用法)");
        System.out.println("-- 参数说明:");
        System.out.println("--   参数1: 数据集路径");
        System.out.println("--   参数2: 向量列名");
        System.out.println("--   参数3: 查询向量");
        System.out.println("--   参数4: 返回TopK数量");
        System.out.println("--   参数5: 距离度量类型 (L2/COSINE/DOT)");
        System.out.println();
        
        String vectorSearchSql = String.format(
            "SELECT * FROM TABLE(\n" +
            "    vector_search(\n" +
            "        '%s',                              -- 数据集路径\n" +
            "        'embedding',                       -- 向量列名\n" +
            "        ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],  -- 查询向量\n" +
            "        3,                                 -- 返回 Top 3\n" +
            "        'L2'                               -- L2 距离度量\n" +
            "    )\n" +
            ")", datasetPath);
        
        System.out.println(vectorSearchSql);
        System.out.println();
        System.out.println("📊 检索结果 (按L2距离排序，距离越小越相似):");
        System.out.println("---------------------------------------------------");
        
        try {
            TableResult result = tableEnv.executeSql(vectorSearchSql);
            result.print();
        } catch (Exception e) {
            System.out.println("⚠️ 向量检索执行出错: " + e.getMessage());
            System.out.println("   这可能是因为数据集需要先构建索引");
        }
        
        // ============================================
        // 第五步：使用 COSINE 余弦相似度检索
        // ============================================
        System.out.println("\n-- 步骤5: 使用 COSINE 余弦相似度检索");
        
        String cosineSearchSql = String.format(
            "SELECT * FROM TABLE(\n" +
            "    vector_search(\n" +
            "        '%s',\n" +
            "        'embedding',\n" +
            "        ARRAY[0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1],\n" +
            "        3,\n" +
            "        'COSINE'                           -- 余弦相似度\n" +
            "    )\n" +
            ")", datasetPath);
        
        System.out.println(cosineSearchSql);
        System.out.println();
        System.out.println("📊 检索结果 (按余弦距离排序):");
        System.out.println("---------------------------------------------------");
        
        try {
            tableEnv.executeSql(cosineSearchSql).print();
        } catch (Exception e) {
            System.out.println("⚠️ 执行出错: " + e.getMessage());
        }
        
        // ============================================
        // 第六步：结合普通查询使用向量检索
        // ============================================
        System.out.println("\n-- 步骤6: 向量检索与其他查询结合 (LATERAL TABLE)");
        
        String lateralSearchSql = String.format(
            "-- 先查询数据，再基于结果进行向量检索\n" +
            "SELECT \n" +
            "    v.id,\n" +
            "    v.title,\n" +
            "    v._distance as similarity_distance\n" +
            "FROM TABLE(\n" +
            "    vector_search('%s', 'embedding', ARRAY[0.15, 0.25, 0.35, 0.45, 0.55, 0.65, 0.75, 0.85], 5, 'L2')\n" +
            ") AS v\n" +
            "WHERE v._distance < 1.0  -- 只返回距离小于1的结果", datasetPath);
        
        System.out.println(lateralSearchSql);
        System.out.println();
        
        // ============================================
        // 打印配置参数说明
        // ============================================
        System.out.println("\n========== IVF_PQ 索引配置参数说明 ==========");
        System.out.println("╔═════════════════════════════╦════════════════════════════════════════════════════╗");
        System.out.println("║       配置项                 ║                说明                                ║");
        System.out.println("╠═════════════════════════════╬════════════════════════════════════════════════════╣");
        System.out.println("║ index.type = 'IVF_PQ'       ║ 使用 IVF_PQ 索引类型                               ║");
        System.out.println("║ index.column                ║ 要建立索引的向量列名                               ║");
        System.out.println("║ index.num-partitions        ║ IVF 分区数量，推荐: sqrt(n) 到 4*sqrt(n)           ║");
        System.out.println("║ index.num-sub-vectors       ║ PQ 子向量数量，必须能整除向量维度                   ║");
        System.out.println("║ index.num-bits              ║ PQ 编码位数，默认8 (256个聚类中心)                  ║");
        System.out.println("║ vector.metric               ║ 距离度量: L2(欧氏距离)/COSINE(余弦)/DOT(点积)      ║");
        System.out.println("║ vector.nprobes              ║ 检索时探测的分区数，越大越精确但越慢               ║");
        System.out.println("╚═════════════════════════════╩════════════════════════════════════════════════════╝");
        
        System.out.println("\n========== 距离度量类型说明 ==========");
        System.out.println("╔════════════════╦════════════════════════════════════════════════════════════════╗");
        System.out.println("║    度量类型    ║                          说明                                  ║");
        System.out.println("╠════════════════╬════════════════════════════════════════════════════════════════╣");
        System.out.println("║    L2          ║ 欧氏距离，值越小越相似，适合稠密向量                           ║");
        System.out.println("║    COSINE      ║ 余弦距离，范围[0,2]，值越小越相似，适合文本嵌入                ║");
        System.out.println("║    DOT         ║ 负点积，值越小越相似（注意需归一化向量）                       ║");
        System.out.println("╚════════════════╩════════════════════════════════════════════════════════════════╝");
        
        System.out.println("\n✅ 向量检索 IVF_PQ 示例完成！\n");
    }

    @Test
    @DisplayName("9.2 不同索引类型对比示例")
    void testDifferentIndexTypesDetailed() throws Exception {
        System.out.println("========== 不同向量索引类型对比 ==========");
        
        // 使用相对路径，基于项目根目录
        Path basePath = Paths.get(System.getProperty("user.dir"), "test-data");
        
        // ============================================
        // IVF_PQ 索引 - 适合大规模数据，内存占用小
        // ============================================
        System.out.println("【1. IVF_PQ 索引】- 推荐用于大规模数据");
        System.out.println("优点: 内存占用小，检索速度快");
        System.out.println("缺点: 精度相对较低（有量化损失）");
        System.out.println();
        
        String ivfPqSql = String.format(
            "CREATE TABLE ivf_pq_vectors (\n" +
            "    id BIGINT,\n" +
            "    embedding ARRAY<FLOAT>\n" +
            ") WITH (\n" +
            "    'connector' = 'lance',\n" +
            "    'path' = '%s',\n" +
            "    'index.type' = 'IVF_PQ',\n" +
            "    'index.column' = 'embedding',\n" +
            "    'index.num-partitions' = '256',    -- IVF 分区数\n" +
            "    'index.num-sub-vectors' = '16',    -- PQ 子向量数\n" +
            "    'index.num-bits' = '8',            -- 每个子向量的编码位数\n" +
            "    'vector.metric' = 'L2'\n" +
            ")", basePath.resolve("ivf-pq-demo"));
        
        System.out.println(ivfPqSql);
        System.out.println();
        
        // ============================================
        // IVF_HNSW 索引 - 高精度检索
        // ============================================
        System.out.println("【2. IVF_HNSW 索引】- 推荐用于高精度需求");
        System.out.println("优点: 检索精度高");
        System.out.println("缺点: 内存占用较大，构建索引较慢");
        System.out.println();
        
        String ivfHnswSql = String.format(
            "CREATE TABLE ivf_hnsw_vectors (\n" +
            "    id BIGINT,\n" +
            "    embedding ARRAY<FLOAT>\n" +
            ") WITH (\n" +
            "    'connector' = 'lance',\n" +
            "    'path' = '%s',\n" +
            "    'index.type' = 'IVF_HNSW',\n" +
            "    'index.column' = 'embedding',\n" +
            "    'index.num-partitions' = '256',    -- IVF 分区数\n" +
            "    'index.hnsw-m' = '16',             -- HNSW 每层连接数\n" +
            "    'index.hnsw-ef-construction' = '100', -- 构建时的候选集大小\n" +
            "    'vector.metric' = 'COSINE',\n" +
            "    'vector.ef' = '50'                 -- 检索时的候选集大小\n" +
            ")", basePath.resolve("ivf-hnsw-demo"));
        
        System.out.println(ivfHnswSql);
        System.out.println();
        
        // ============================================
        // IVF_FLAT 索引 - 最高精度，暴力检索
        // ============================================
        System.out.println("【3. IVF_FLAT 索引】- 精度最高");
        System.out.println("优点: 检索精度100%（无损）");
        System.out.println("缺点: 检索速度较慢，适合小规模数据");
        System.out.println();
        
        String ivfFlatSql = String.format(
            "CREATE TABLE ivf_flat_vectors (\n" +
            "    id BIGINT,\n" +
            "    embedding ARRAY<FLOAT>\n" +
            ") WITH (\n" +
            "    'connector' = 'lance',\n" +
            "    'path' = '%s',\n" +
            "    'index.type' = 'IVF_FLAT',\n" +
            "    'index.column' = 'embedding',\n" +
            "    'index.num-partitions' = '128',    -- IVF 分区数\n" +
            "    'vector.metric' = 'DOT',\n" +
            "    'vector.nprobes' = '32'            -- 检索时探测的分区数\n" +
            ")", basePath.resolve("ivf-flat-demo"));
        
        System.out.println(ivfFlatSql);
        System.out.println();
        
        // ============================================
        // 索引选择建议
        // ============================================
        System.out.println("========== 索引选择建议 ==========");
        System.out.println("╔═══════════════════╦════════════════╦═══════════════╦════════════════════════════════╗");
        System.out.println("║     索引类型      ║   数据规模     ║   精度要求    ║           适用场景             ║");
        System.out.println("╠═══════════════════╬════════════════╬═══════════════╬════════════════════════════════╣");
        System.out.println("║    IVF_PQ         ║   100万+       ║     中等      ║ 大规模推荐系统、图片检索       ║");
        System.out.println("║    IVF_HNSW       ║   10万-100万   ║     高        ║ 语义搜索、问答系统             ║");
        System.out.println("║    IVF_FLAT       ║   <10万        ║     最高      ║ 小规模高精度场景               ║");
        System.out.println("╚═══════════════════╩════════════════╩═══════════════╩════════════════════════════════╝");
        
        System.out.println("\n✅ 索引类型对比示例完成！\n");
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

-- ================================================================================
-- Flink SQL Lance Connector 测试脚本
-- ================================================================================
-- 本脚本演示如何使用 Flink SQL 操作 Lance 向量数据集
-- 使用方式: 在 Flink SQL Client 中执行以下语句
-- ================================================================================

-- ================================================================================
-- 1. 创建基础向量表
-- ================================================================================

-- 创建一个简单的向量存储表
CREATE TABLE lance_vectors (
    id BIGINT,
    content STRING,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/tmp/lance/vectors',
    'write.batch-size' = '1024',
    'write.mode' = 'overwrite'
);

-- 插入测试数据
INSERT INTO lance_vectors VALUES
    (1, 'Hello World', ARRAY[0.1, 0.2, 0.3, 0.4]),
    (2, 'Machine Learning', ARRAY[0.2, 0.3, 0.4, 0.5]),
    (3, 'Deep Learning', ARRAY[0.3, 0.4, 0.5, 0.6]),
    (4, 'Neural Networks', ARRAY[0.4, 0.5, 0.6, 0.7]),
    (5, 'Vector Database', ARRAY[0.5, 0.6, 0.7, 0.8]);

-- 查询数据
SELECT * FROM lance_vectors;

-- ================================================================================
-- 2. 创建带向量索引配置的表
-- ================================================================================

-- 创建带 IVF_PQ 索引的向量表
CREATE TABLE document_embeddings (
    doc_id BIGINT COMMENT '文档ID',
    title STRING COMMENT '文档标题',
    content STRING COMMENT '文档内容',
    embedding ARRAY<FLOAT> COMMENT '768维文档向量',
    category STRING COMMENT '文档分类',
    create_time TIMESTAMP(3) COMMENT '创建时间'
) WITH (
    'connector' = 'lance',
    'path' = '/tmp/lance/documents',
    -- 写入配置
    'write.batch-size' = '2048',
    'write.mode' = 'append',
    'write.max-rows-per-file' = '100000',
    -- 向量索引配置 (IVF_PQ)
    'index.type' = 'IVF_PQ',
    'index.column' = 'embedding',
    'index.num-partitions' = '256',
    'index.num-sub-vectors' = '16',
    'index.num-bits' = '8',
    -- 向量检索配置
    'vector.column' = 'embedding',
    'vector.metric' = 'COSINE',
    'vector.nprobes' = '20'
);

-- ================================================================================
-- 3. 不同索引类型示例
-- ================================================================================

-- IVF_PQ 索引 (推荐，平衡精度和速度)
CREATE TABLE vectors_ivf_pq (
    id BIGINT,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/tmp/lance/ivf_pq',
    'index.type' = 'IVF_PQ',
    'index.column' = 'embedding',
    'index.num-partitions' = '256',
    'index.num-sub-vectors' = '16',
    'index.num-bits' = '8',
    'vector.metric' = 'L2'
);

-- IVF_HNSW 索引 (高精度)
CREATE TABLE vectors_ivf_hnsw (
    id BIGINT,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/tmp/lance/ivf_hnsw',
    'index.type' = 'IVF_HNSW',
    'index.column' = 'embedding',
    'index.num-partitions' = '256',
    'index.max-level' = '7',
    'index.m' = '16',
    'index.ef-construction' = '100',
    'vector.metric' = 'COSINE'
);

-- IVF_FLAT 索引 (最高精度，适合小数据集)
CREATE TABLE vectors_ivf_flat (
    id BIGINT,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/tmp/lance/ivf_flat',
    'index.type' = 'IVF_FLAT',
    'index.column' = 'embedding',
    'index.num-partitions' = '64',
    'vector.metric' = 'DOT'
);

-- ================================================================================
-- 4. 创建和使用 Lance Catalog
-- ================================================================================

-- 创建 Lance Catalog
CREATE CATALOG lance_catalog WITH (
    'type' = 'lance',
    'warehouse' = '/tmp/lance/warehouse',
    'default-database' = 'default'
);

-- 切换到 Lance Catalog
USE CATALOG lance_catalog;

-- 创建数据库
CREATE DATABASE IF NOT EXISTS vector_db;

-- 使用数据库
USE vector_db;

-- 在 Catalog 中创建表
CREATE TABLE embeddings (
    id BIGINT,
    text STRING,
    vector ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/tmp/lance/warehouse/vector_db/embeddings'
);

-- 列出数据库
SHOW DATABASES;

-- 列出表
SHOW TABLES;

-- ================================================================================
-- 5. 数据操作示例
-- ================================================================================

-- 批量插入数据
INSERT INTO document_embeddings VALUES
    (1, 'Apache Flink入门', 'Flink是一个分布式流处理框架...',
     ARRAY[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8], 'tutorial', TIMESTAMP '2024-01-01 10:00:00'),
    (2, 'Flink SQL详解', '使用SQL语法进行流处理...',
     ARRAY[0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9], 'tutorial', TIMESTAMP '2024-01-02 11:00:00'),
    (3, '向量数据库对比', '常见向量数据库的对比分析...',
     ARRAY[0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0], 'analysis', TIMESTAMP '2024-01-03 12:00:00'),
    (4, 'Lance格式详解', 'Lance是一种高效的向量存储格式...',
     ARRAY[0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1], 'format', TIMESTAMP '2024-01-04 13:00:00'),
    (5, 'RAG系统实战', '基于向量检索的知识库问答系统...',
     ARRAY[0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 0.1, 0.2], 'practice', TIMESTAMP '2024-01-05 14:00:00');

-- 简单查询
SELECT doc_id, title, category FROM document_embeddings;

-- 条件查询
SELECT doc_id, title, category, create_time
FROM document_embeddings
WHERE category = 'tutorial'
ORDER BY create_time DESC;

-- 聚合查询
SELECT category, COUNT(*) as doc_count
FROM document_embeddings
GROUP BY category
ORDER BY doc_count DESC;

-- 时间范围查询
SELECT doc_id, title, create_time
FROM document_embeddings
WHERE create_time >= TIMESTAMP '2024-01-02 00:00:00'
  AND create_time < TIMESTAMP '2024-01-05 00:00:00';

-- ================================================================================
-- 6. 流式处理示例
-- ================================================================================

-- 创建数据生成器表（模拟实时数据）
CREATE TABLE realtime_events (
    event_id BIGINT,
    event_type STRING,
    embedding ARRAY<FLOAT>,
    event_time AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '100',
    'fields.event_id.kind' = 'sequence',
    'fields.event_id.start' = '1',
    'fields.event_id.end' = '1000000',
    'fields.event_type.length' = '10'
);

-- 创建 Lance Sink 表
CREATE TABLE lance_events (
    event_id BIGINT,
    event_type STRING,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/tmp/lance/events',
    'write.batch-size' = '1000',
    'write.mode' = 'append'
);

-- 流式写入
INSERT INTO lance_events
SELECT event_id, event_type, ARRAY[RAND(), RAND(), RAND(), RAND()] as embedding
FROM realtime_events;

-- ================================================================================
-- 7. 向量检索 UDF 使用示例
-- ================================================================================

-- 注册向量检索 UDF
CREATE FUNCTION vector_search AS 'org.apache.flink.connector.lance.table.LanceVectorSearchFunction';

-- 使用 UDF 进行向量检索 (示例语法)
-- SELECT * FROM vector_search(
--     '/tmp/lance/documents',  -- 数据集路径
--     'embedding',             -- 向量列名
--     ARRAY[0.1, 0.2, 0.3],   -- 查询向量
--     10,                      -- 返回数量
--     'L2'                     -- 距离度量
-- );

-- ================================================================================
-- 8. 常用管理命令
-- ================================================================================

-- 查看表结构
DESCRIBE lance_vectors;

-- 查看表详细信息
SHOW CREATE TABLE lance_vectors;

-- 删除表
DROP TABLE IF EXISTS lance_vectors;

-- 删除数据库
DROP DATABASE IF EXISTS vector_db CASCADE;

-- 删除 Catalog
DROP CATALOG IF EXISTS lance_catalog;

-- ================================================================================
-- 配置选项参考
-- ================================================================================
--
-- 基础配置:
--   connector           = 'lance'        -- 连接器类型（必需）
--   path                = '/path'        -- 数据集路径（必需）
--
-- 读取配置:
--   read.batch-size     = 1024           -- 读取批次大小
--   read.columns        = 'col1,col2'    -- 读取的列（逗号分隔）
--   read.filter         = 'id > 10'      -- 过滤条件
--
-- 写入配置:
--   write.batch-size    = 1024           -- 写入批次大小
--   write.mode          = 'append'       -- 写入模式: append/overwrite
--   write.max-rows-per-file = 1000000    -- 每个文件最大行数
--
-- 索引配置:
--   index.type          = 'IVF_PQ'       -- 索引类型: IVF_PQ/IVF_HNSW/IVF_FLAT
--   index.column        = 'embedding'    -- 索引列名
--   index.num-partitions = 256           -- IVF分区数
--   index.num-sub-vectors = 16           -- PQ子向量数
--   index.num-bits      = 8              -- 量化位数
--   index.max-level     = 7              -- HNSW最大层数
--   index.m             = 16             -- HNSW连接数
--   index.ef-construction = 100          -- HNSW构建ef参数
--
-- 向量检索配置:
--   vector.column       = 'embedding'    -- 向量列名
--   vector.metric       = 'L2'           -- 距离度量: L2/COSINE/DOT
--   vector.nprobes      = 20             -- 检索探针数
--   vector.ef           = 100            -- HNSW检索ef参数
--   vector.refine-factor = null          -- 精化因子
--
-- ================================================================================

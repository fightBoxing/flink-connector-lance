# Flink Connector Lance

Apache Flink Connector for Lance 向量数据格式。

## 概述

`flink-connector-lance` 是一个基于 Apache Flink 1.16.1 的 Lance 向量数据格式连接器，支持：

- **Source 功能**：从 Lance 数据集中读取向量数据
- **Sink 功能**：将 Flink 数据流写入 Lance 数据集
- **向量索引构建**：支持 IVF_PQ、IVF_HNSW、IVF_FLAT 索引
- **向量检索能力**：支持 KNN 检索（L2、Cosine、Dot）
- **Flink Table API / SQL 支持**：声明式 SQL 接口

## 快速开始

### Maven 依赖

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-lance</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### DataStream API 使用

#### 读取 Lance 数据

```java
import org.apache.flink.connector.lance.LanceSource;
import org.apache.flink.connector.lance.config.LanceOptions;

// 构建配置
LanceOptions options = LanceOptions.builder()
    .path("/path/to/lance/dataset")
    .readBatchSize(1024)
    .build();

// 创建 Source
LanceSource source = new LanceSource(options, rowType);

// 添加到 Flink 环境
DataStream<RowData> stream = env.addSource(source);
```

#### 使用 Builder 模式

```java
LanceSource source = LanceSource.builder()
    .path("/path/to/lance/dataset")
    .batchSize(512)
    .columns(Arrays.asList("id", "content", "embedding"))
    .filter("id > 100")
    .rowType(rowType)
    .build();
```

#### 写入 Lance 数据

```java
import org.apache.flink.connector.lance.LanceSink;

LanceSink sink = LanceSink.builder()
    .path("/path/to/output/dataset")
    .batchSize(1024)
    .writeMode(LanceOptions.WriteMode.APPEND)
    .maxRowsPerFile(1000000)
    .rowType(rowType)
    .build();

dataStream.addSink(sink);
```

### Table API / SQL 使用

#### 创建 Catalog

```sql
CREATE CATALOG lance_catalog WITH (
    'type' = 'lance',
    'warehouse' = '/path/to/warehouse',
    'default-database' = 'default'
);
```

#### 创建表

```sql
CREATE TABLE lance_table (
    id BIGINT,
    content STRING,
    embedding ARRAY<FLOAT>
) WITH (
    'connector' = 'lance',
    'path' = '/path/to/dataset',
    'read.batch-size' = '1024',
    'write.batch-size' = '1024',
    'write.mode' = 'append'
);
```

#### 查询数据

```sql
SELECT * FROM lance_table WHERE id > 100;
```

#### 写入数据

```sql
INSERT INTO lance_table 
SELECT id, content, embedding FROM source_table;
```

### 向量索引构建

```java
import org.apache.flink.connector.lance.LanceIndexBuilder;
import org.apache.flink.connector.lance.config.LanceOptions.IndexType;
import org.apache.flink.connector.lance.config.LanceOptions.MetricType;

LanceIndexBuilder builder = LanceIndexBuilder.builder()
    .datasetPath("/path/to/dataset")
    .columnName("embedding")
    .indexType(IndexType.IVF_PQ)
    .metricType(MetricType.L2)
    .numPartitions(256)
    .numSubVectors(16)
    .numBits(8)
    .build();

LanceIndexBuilder.IndexBuildResult result = builder.buildIndex();
if (result.isSuccess()) {
    System.out.println("索引构建成功，耗时: " + result.getDurationMillis() + "ms");
}
```

#### 支持的索引类型

| 索引类型 | 描述 | 主要参数 |
|---------|------|---------|
| IVF_PQ | 倒排文件 + 乘积量化 | num_partitions, num_sub_vectors, num_bits |
| IVF_HNSW | 倒排文件 + HNSW | num_partitions, max_level, m, ef_construction |
| IVF_FLAT | 倒排文件 + 暴力搜索 | num_partitions |

### 向量检索

```java
import org.apache.flink.connector.lance.LanceVectorSearch;

LanceVectorSearch search = LanceVectorSearch.builder()
    .datasetPath("/path/to/dataset")
    .columnName("embedding")
    .metricType(MetricType.L2)
    .nprobes(20)
    .build();

search.open();

// 执行 KNN 检索
float[] queryVector = new float[] {0.1f, 0.2f, 0.3f, ...};
List<LanceVectorSearch.SearchResult> results = search.search(queryVector, 10);

for (SearchResult result : results) {
    System.out.println("距离: " + result.getDistance());
    System.out.println("相似度: " + result.getSimilarity());
}

search.close();
```

#### 支持的距离度量

| 度量类型 | 描述 |
|---------|------|
| L2 | 欧氏距离 |
| Cosine | 余弦相似度 |
| Dot | 点积 |

## 配置参数

### Source 配置

| 参数 | 描述 | 默认值 |
|-----|------|-------|
| `path` | Lance 数据集路径 | 必填 |
| `read.batch-size` | 读取批次大小 | 1024 |
| `read.columns` | 读取的列（逗号分隔） | 全部 |
| `read.filter` | 过滤条件 | 无 |

### Sink 配置

| 参数 | 描述 | 默认值 |
|-----|------|-------|
| `path` | 输出路径 | 必填 |
| `write.batch-size` | 写入批次大小 | 1024 |
| `write.mode` | 写入模式（append/overwrite） | append |
| `write.max-rows-per-file` | 每文件最大行数 | 1000000 |

### 索引配置

| 参数 | 描述 | 默认值 |
|-----|------|-------|
| `index.type` | 索引类型 | IVF_PQ |
| `index.column` | 索引列名 | 必填 |
| `index.num-partitions` | 分区数 | 256 |
| `index.num-sub-vectors` | PQ 子向量数 | 自动 |
| `index.num-bits` | 量化位数 | 8 |

### 向量检索配置

| 参数 | 描述 | 默认值 |
|-----|------|-------|
| `vector.column` | 向量列名 | 必填 |
| `vector.metric` | 距离度量 | L2 |
| `vector.nprobes` | 检索探针数 | 20 |
| `vector.ef` | HNSW 搜索宽度 | 100 |
| `vector.refine-factor` | 精细化因子 | 无 |

## 类型映射

| Lance/Arrow 类型 | Flink 类型 |
|-----------------|-----------|
| Int8 | TINYINT |
| Int16 | SMALLINT |
| Int32 | INT |
| Int64 | BIGINT |
| Float32 | FLOAT |
| Float64 | DOUBLE |
| String/LargeString | STRING |
| Boolean | BOOLEAN |
| Binary/LargeBinary | BYTES |
| Date32 | DATE |
| Timestamp | TIMESTAMP |
| FixedSizeList\<Float32\> | ARRAY\<FLOAT\> |
| FixedSizeList\<Float64\> | ARRAY\<DOUBLE\> |

## 项目结构

```
flink-connector-lance/
├── src/main/java/org/apache/flink/connector/lance/
│   ├── LanceSource.java              # Source 实现
│   ├── LanceSink.java                # Sink 实现
│   ├── LanceInputFormat.java         # InputFormat 实现
│   ├── LanceSplit.java               # 分片定义
│   ├── LanceVectorSearch.java        # 向量检索
│   ├── LanceIndexBuilder.java        # 索引构建
│   ├── config/
│   │   └── LanceOptions.java         # 配置管理
│   ├── converter/
│   │   ├── LanceTypeConverter.java   # 类型转换
│   │   └── RowDataConverter.java     # 数据转换
│   └── table/
│       ├── LanceDynamicTableFactory.java
│       ├── LanceDynamicTableSource.java
│       ├── LanceDynamicTableSink.java
│       ├── LanceCatalog.java
│       ├── LanceCatalogFactory.java
│       └── LanceVectorSearchFunction.java
├── src/test/java/...                 # 单元测试和集成测试
└── pom.xml
```

## 构建

```bash
mvn clean package -DskipTests
```

## 运行测试

```bash
mvn test
```

## 依赖版本

- Apache Flink: 1.16.1
- Lance Java SDK: 0.9.0
- Apache Arrow: 14.0.0

## License

Apache License 2.0

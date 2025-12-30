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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * Lance 动态表工厂。
 * 
 * <p>实现 Flink Table API 的 DynamicTableSourceFactory 和 DynamicTableSinkFactory 接口，
 * 支持通过 SQL DDL 创建 Lance 表。
 * 
 * <p>使用示例：
 * <pre>{@code
 * CREATE TABLE lance_table (
 *     id BIGINT,
 *     content STRING,
 *     embedding ARRAY<FLOAT>
 * ) WITH (
 *     'connector' = 'lance',
 *     'path' = '/path/to/dataset'
 * );
 * }</pre>
 */
public class LanceDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "lance";

    // ==================== 配置选项定义 ====================

    public static final ConfigOption<String> PATH = ConfigOptions
            .key("path")
            .stringType()
            .noDefaultValue()
            .withDescription("Lance 数据集路径");

    public static final ConfigOption<Integer> READ_BATCH_SIZE = ConfigOptions
            .key("read.batch-size")
            .intType()
            .defaultValue(1024)
            .withDescription("读取批次大小");

    public static final ConfigOption<String> READ_COLUMNS = ConfigOptions
            .key("read.columns")
            .stringType()
            .noDefaultValue()
            .withDescription("要读取的列列表，逗号分隔");

    public static final ConfigOption<String> READ_FILTER = ConfigOptions
            .key("read.filter")
            .stringType()
            .noDefaultValue()
            .withDescription("数据过滤条件");

    public static final ConfigOption<Integer> WRITE_BATCH_SIZE = ConfigOptions
            .key("write.batch-size")
            .intType()
            .defaultValue(1024)
            .withDescription("写入批次大小");

    public static final ConfigOption<String> WRITE_MODE = ConfigOptions
            .key("write.mode")
            .stringType()
            .defaultValue("append")
            .withDescription("写入模式：append 或 overwrite");

    public static final ConfigOption<Integer> WRITE_MAX_ROWS_PER_FILE = ConfigOptions
            .key("write.max-rows-per-file")
            .intType()
            .defaultValue(1000000)
            .withDescription("每个文件的最大行数");

    public static final ConfigOption<String> INDEX_TYPE = ConfigOptions
            .key("index.type")
            .stringType()
            .defaultValue("IVF_PQ")
            .withDescription("向量索引类型");

    public static final ConfigOption<String> INDEX_COLUMN = ConfigOptions
            .key("index.column")
            .stringType()
            .noDefaultValue()
            .withDescription("索引列名");

    public static final ConfigOption<Integer> INDEX_NUM_PARTITIONS = ConfigOptions
            .key("index.num-partitions")
            .intType()
            .defaultValue(256)
            .withDescription("IVF 分区数");

    public static final ConfigOption<Integer> INDEX_NUM_SUB_VECTORS = ConfigOptions
            .key("index.num-sub-vectors")
            .intType()
            .noDefaultValue()
            .withDescription("PQ 子向量数");

    public static final ConfigOption<String> VECTOR_COLUMN = ConfigOptions
            .key("vector.column")
            .stringType()
            .noDefaultValue()
            .withDescription("向量列名");

    public static final ConfigOption<String> VECTOR_METRIC = ConfigOptions
            .key("vector.metric")
            .stringType()
            .defaultValue("L2")
            .withDescription("距离度量类型：L2, Cosine, Dot");

    public static final ConfigOption<Integer> VECTOR_NPROBES = ConfigOptions
            .key("vector.nprobes")
            .intType()
            .defaultValue(20)
            .withDescription("IVF 检索探针数");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(READ_BATCH_SIZE);
        options.add(READ_COLUMNS);
        options.add(READ_FILTER);
        options.add(WRITE_BATCH_SIZE);
        options.add(WRITE_MODE);
        options.add(WRITE_MAX_ROWS_PER_FILE);
        options.add(INDEX_TYPE);
        options.add(INDEX_COLUMN);
        options.add(INDEX_NUM_PARTITIONS);
        options.add(INDEX_NUM_SUB_VECTORS);
        options.add(VECTOR_COLUMN);
        options.add(VECTOR_METRIC);
        options.add(VECTOR_NPROBES);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig config = helper.getOptions();
        LanceOptions options = buildLanceOptions(config);

        return new LanceDynamicTableSource(
                options,
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType()
        );
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig config = helper.getOptions();
        LanceOptions options = buildLanceOptions(config);

        return new LanceDynamicTableSink(
                options,
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType()
        );
    }

    /**
     * 从配置构建 LanceOptions
     */
    private LanceOptions buildLanceOptions(ReadableConfig config) {
        LanceOptions.Builder builder = LanceOptions.builder();

        // 通用配置
        builder.path(config.get(PATH));

        // Source 配置
        builder.readBatchSize(config.get(READ_BATCH_SIZE));
        config.getOptional(READ_COLUMNS).ifPresent(columns -> {
            if (!columns.isEmpty()) {
                builder.readColumns(java.util.Arrays.asList(columns.split(",")));
            }
        });
        config.getOptional(READ_FILTER).ifPresent(builder::readFilter);

        // Sink 配置
        builder.writeBatchSize(config.get(WRITE_BATCH_SIZE));
        builder.writeMode(LanceOptions.WriteMode.fromValue(config.get(WRITE_MODE)));
        builder.writeMaxRowsPerFile(config.get(WRITE_MAX_ROWS_PER_FILE));

        // 索引配置
        builder.indexType(LanceOptions.IndexType.fromValue(config.get(INDEX_TYPE)));
        config.getOptional(INDEX_COLUMN).ifPresent(builder::indexColumn);
        builder.indexNumPartitions(config.get(INDEX_NUM_PARTITIONS));
        config.getOptional(INDEX_NUM_SUB_VECTORS).ifPresent(builder::indexNumSubVectors);

        // 向量检索配置
        config.getOptional(VECTOR_COLUMN).ifPresent(builder::vectorColumn);
        builder.vectorMetric(LanceOptions.MetricType.fromValue(config.get(VECTOR_METRIC)));
        builder.vectorNprobes(config.get(VECTOR_NPROBES));

        return builder.build();
    }
}

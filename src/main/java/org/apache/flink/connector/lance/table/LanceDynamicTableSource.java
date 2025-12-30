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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.lance.LanceInputFormat;
import org.apache.flink.connector.lance.LanceSource;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Lance 动态表数据源。
 * 
 * <p>实现 ScanTableSource 接口，支持列裁剪和过滤下推。
 */
public class LanceDynamicTableSource implements ScanTableSource, 
        SupportsProjectionPushDown, SupportsFilterPushDown {

    private final LanceOptions options;
    private final DataType physicalDataType;
    private int[] projectedFields;
    private List<String> filters;

    public LanceDynamicTableSource(LanceOptions options, DataType physicalDataType) {
        this.options = options;
        this.physicalDataType = physicalDataType;
        this.projectedFields = null;
        this.filters = new ArrayList<>();
    }

    private LanceDynamicTableSource(LanceDynamicTableSource source) {
        this.options = source.options;
        this.physicalDataType = source.physicalDataType;
        this.projectedFields = source.projectedFields;
        this.filters = new ArrayList<>(source.filters);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        RowType rowType = (RowType) physicalDataType.getLogicalType();
        
        // 如果有列裁剪，构建新的 RowType
        RowType projectedRowType = rowType;
        if (projectedFields != null) {
            List<RowType.RowField> projectedFieldList = new ArrayList<>();
            for (int fieldIndex : projectedFields) {
                projectedFieldList.add(rowType.getFields().get(fieldIndex));
            }
            projectedRowType = new RowType(projectedFieldList);
        }

        // 构建 LanceOptions（应用列裁剪和过滤条件）
        LanceOptions.Builder optionsBuilder = LanceOptions.builder()
                .path(options.getPath())
                .readBatchSize(options.getReadBatchSize())
                .readFilter(buildFilterExpression());

        // 设置要读取的列
        if (projectedFields != null) {
            List<String> columnNames = Arrays.stream(projectedFields)
                    .mapToObj(i -> rowType.getFieldNames().get(i))
                    .collect(Collectors.toList());
            optionsBuilder.readColumns(columnNames);
        }

        LanceOptions finalOptions = optionsBuilder.build();
        final RowType finalRowType = projectedRowType;

        // 使用 DataStreamScanProvider
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
                LanceSource source = new LanceSource(finalOptions, finalRowType);
                return execEnv.addSource(source, "LanceSource");
            }

            @Override
            public boolean isBounded() {
                return true; // Lance 数据集是有界的
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        return new LanceDynamicTableSource(this);
    }

    @Override
    public String asSummaryString() {
        return "Lance Table Source";
    }

    // ==================== SupportsProjectionPushDown ====================

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        // 仅支持顶层字段投影
        this.projectedFields = Arrays.stream(projectedFields)
                .mapToInt(arr -> arr[0])
                .toArray();
    }

    // ==================== SupportsFilterPushDown ====================

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        // 将 Flink 表达式转换为 Lance 过滤条件
        List<ResolvedExpression> acceptedFilters = new ArrayList<>();
        List<ResolvedExpression> remainingFilters = new ArrayList<>();

        for (ResolvedExpression filter : filters) {
            String lanceFilter = convertToLanceFilter(filter);
            if (lanceFilter != null) {
                this.filters.add(lanceFilter);
                acceptedFilters.add(filter);
            } else {
                remainingFilters.add(filter);
            }
        }

        return Result.of(acceptedFilters, remainingFilters);
    }

    /**
     * 将 Flink 表达式转换为 Lance 过滤条件
     */
    private String convertToLanceFilter(ResolvedExpression expression) {
        // 简化实现：尝试将表达式转换为字符串形式
        // 实际生产环境需要更完整的表达式解析
        try {
            String exprStr = expression.toString();
            // Lance 支持类似 SQL 的过滤语法
            return exprStr;
        } catch (Exception e) {
            // 无法转换的表达式返回 null
            return null;
        }
    }

    /**
     * 构建过滤表达式
     */
    private String buildFilterExpression() {
        if (filters.isEmpty()) {
            return options.getReadFilter();
        }

        String combinedFilter = String.join(" AND ", filters);
        String originalFilter = options.getReadFilter();

        if (originalFilter != null && !originalFilter.isEmpty()) {
            return "(" + originalFilter + ") AND (" + combinedFilter + ")";
        }

        return combinedFilter;
    }

    /**
     * 获取配置选项
     */
    public LanceOptions getOptions() {
        return options;
    }

    /**
     * 获取物理数据类型
     */
    public DataType getPhysicalDataType() {
        return physicalDataType;
    }
}

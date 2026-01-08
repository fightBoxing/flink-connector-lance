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

package org.apache.flink.connector.lance.aggregate;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 聚合执行器。
 * 
 * <p>在数据源端执行聚合计算，支持 COUNT、SUM、AVG、MIN、MAX 等聚合函数。
 */
public class AggregateExecutor implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AggregateExecutor.class);

    private final AggregateInfo aggregateInfo;
    private final RowType sourceRowType;

    // 聚合状态（按分组键）
    private transient Map<GroupKey, AggregateState> aggregateStates;
    private transient boolean initialized;

    public AggregateExecutor(AggregateInfo aggregateInfo, RowType sourceRowType) {
        this.aggregateInfo = aggregateInfo;
        this.sourceRowType = sourceRowType;
    }

    /**
     * 初始化聚合执行器
     */
    public void init() {
        this.aggregateStates = new HashMap<>();
        this.initialized = true;
        LOG.info("初始化聚合执行器: {}", aggregateInfo);
    }

    /**
     * 累积一行数据到聚合状态
     */
    public void accumulate(RowData row) {
        if (!initialized) {
            init();
        }

        // 提取分组键
        GroupKey groupKey = extractGroupKey(row);
        
        // 获取或创建聚合状态
        AggregateState state = aggregateStates.computeIfAbsent(groupKey, 
                k -> new AggregateState(aggregateInfo.getAggregateCalls().size()));

        // 更新每个聚合函数的状态
        List<AggregateInfo.AggregateCall> calls = aggregateInfo.getAggregateCalls();
        for (int i = 0; i < calls.size(); i++) {
            AggregateInfo.AggregateCall call = calls.get(i);
            accumulateCall(state, i, call, row);
        }
    }

    /**
     * 累积单个聚合函数
     */
    private void accumulateCall(AggregateState state, int index, 
                                 AggregateInfo.AggregateCall call, RowData row) {
        switch (call.getFunction()) {
            case COUNT:
                if (call.isCountStar()) {
                    // COUNT(*)
                    state.incrementCount(index);
                } else {
                    // COUNT(column) - 只计数非 NULL 值
                    int fieldIndex = getFieldIndex(call.getColumn());
                    if (fieldIndex >= 0 && !row.isNullAt(fieldIndex)) {
                        state.incrementCount(index);
                    }
                }
                break;

            case COUNT_DISTINCT:
                if (call.getColumn() != null) {
                    int fieldIndex = getFieldIndex(call.getColumn());
                    if (fieldIndex >= 0 && !row.isNullAt(fieldIndex)) {
                        Object value = extractValue(row, fieldIndex);
                        state.addDistinctValue(index, value);
                    }
                }
                break;

            case SUM:
                if (call.getColumn() != null) {
                    int fieldIndex = getFieldIndex(call.getColumn());
                    if (fieldIndex >= 0 && !row.isNullAt(fieldIndex)) {
                        Number value = extractNumericValue(row, fieldIndex);
                        if (value != null) {
                            state.addSum(index, value.doubleValue());
                        }
                    }
                }
                break;

            case AVG:
                if (call.getColumn() != null) {
                    int fieldIndex = getFieldIndex(call.getColumn());
                    if (fieldIndex >= 0 && !row.isNullAt(fieldIndex)) {
                        Number value = extractNumericValue(row, fieldIndex);
                        if (value != null) {
                            state.addForAvg(index, value.doubleValue());
                        }
                    }
                }
                break;

            case MIN:
                if (call.getColumn() != null) {
                    int fieldIndex = getFieldIndex(call.getColumn());
                    if (fieldIndex >= 0 && !row.isNullAt(fieldIndex)) {
                        Comparable<?> value = extractComparableValue(row, fieldIndex);
                        if (value != null) {
                            state.updateMin(index, value);
                        }
                    }
                }
                break;

            case MAX:
                if (call.getColumn() != null) {
                    int fieldIndex = getFieldIndex(call.getColumn());
                    if (fieldIndex >= 0 && !row.isNullAt(fieldIndex)) {
                        Comparable<?> value = extractComparableValue(row, fieldIndex);
                        if (value != null) {
                            state.updateMax(index, value);
                        }
                    }
                }
                break;
        }
    }

    /**
     * 获取聚合结果
     */
    public List<RowData> getResults() {
        if (!initialized || aggregateStates.isEmpty()) {
            // 如果没有数据，返回默认聚合结果
            return getDefaultResults();
        }

        List<RowData> results = new ArrayList<>();
        List<AggregateInfo.AggregateCall> calls = aggregateInfo.getAggregateCalls();
        List<String> groupByCols = aggregateInfo.getGroupByColumns();

        for (Map.Entry<GroupKey, AggregateState> entry : aggregateStates.entrySet()) {
            GroupKey groupKey = entry.getKey();
            AggregateState state = entry.getValue();

            // 创建结果行：分组列 + 聚合列
            int totalFields = groupByCols.size() + calls.size();
            GenericRowData resultRow = new GenericRowData(totalFields);

            // 填充分组列
            for (int i = 0; i < groupByCols.size(); i++) {
                resultRow.setField(i, groupKey.getValues()[i]);
            }

            // 填充聚合结果
            for (int i = 0; i < calls.size(); i++) {
                AggregateInfo.AggregateCall call = calls.get(i);
                Object aggResult = getAggregateResult(state, i, call);
                resultRow.setField(groupByCols.size() + i, aggResult);
            }

            results.add(resultRow);
        }

        LOG.info("聚合执行完成，生成 {} 个结果行", results.size());
        return results;
    }

    /**
     * 获取无数据时的默认聚合结果
     */
    private List<RowData> getDefaultResults() {
        // 如果有 GROUP BY，没有数据就没有结果
        if (aggregateInfo.hasGroupBy()) {
            return new ArrayList<>();
        }

        // 没有 GROUP BY，返回默认聚合值
        List<AggregateInfo.AggregateCall> calls = aggregateInfo.getAggregateCalls();
        GenericRowData resultRow = new GenericRowData(calls.size());

        for (int i = 0; i < calls.size(); i++) {
            AggregateInfo.AggregateCall call = calls.get(i);
            switch (call.getFunction()) {
                case COUNT:
                case COUNT_DISTINCT:
                    resultRow.setField(i, 0L);
                    break;
                default:
                    resultRow.setField(i, null);
                    break;
            }
        }

        List<RowData> results = new ArrayList<>();
        results.add(resultRow);
        return results;
    }

    /**
     * 获取单个聚合函数的结果
     */
    private Object getAggregateResult(AggregateState state, int index, 
                                       AggregateInfo.AggregateCall call) {
        switch (call.getFunction()) {
            case COUNT:
                return state.getCount(index);
            case COUNT_DISTINCT:
                return (long) state.getDistinctCount(index);
            case SUM:
                Double sum = state.getSum(index);
                return sum != null ? sum : null;
            case AVG:
                Double avg = state.getAvg(index);
                return avg != null ? avg : null;
            case MIN:
                return state.getMin(index);
            case MAX:
                return state.getMax(index);
            default:
                return null;
        }
    }

    /**
     * 提取分组键
     */
    private GroupKey extractGroupKey(RowData row) {
        List<String> groupByCols = aggregateInfo.getGroupByColumns();
        if (groupByCols.isEmpty()) {
            return GroupKey.EMPTY;
        }

        Object[] keyValues = new Object[groupByCols.size()];
        for (int i = 0; i < groupByCols.size(); i++) {
            int fieldIndex = getFieldIndex(groupByCols.get(i));
            if (fieldIndex >= 0) {
                keyValues[i] = extractValue(row, fieldIndex);
            }
        }
        return new GroupKey(keyValues);
    }

    /**
     * 获取字段索引
     */
    private int getFieldIndex(String columnName) {
        List<String> fieldNames = sourceRowType.getFieldNames();
        return fieldNames.indexOf(columnName);
    }

    /**
     * 提取字段值
     */
    private Object extractValue(RowData row, int fieldIndex) {
        if (row.isNullAt(fieldIndex)) {
            return null;
        }

        LogicalType fieldType = sourceRowType.getTypeAt(fieldIndex);
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return row.getBoolean(fieldIndex);
            case TINYINT:
                return row.getByte(fieldIndex);
            case SMALLINT:
                return row.getShort(fieldIndex);
            case INTEGER:
                return row.getInt(fieldIndex);
            case BIGINT:
                return row.getLong(fieldIndex);
            case FLOAT:
                return row.getFloat(fieldIndex);
            case DOUBLE:
                return row.getDouble(fieldIndex);
            case CHAR:
            case VARCHAR:
                // 保留 StringData 类型，用于分组键和结果输出
                return row.getString(fieldIndex);
            case DECIMAL:
                DecimalType decType = (DecimalType) fieldType;
                DecimalData decData = row.getDecimal(fieldIndex, decType.getPrecision(), decType.getScale());
                return decData != null ? decData.toBigDecimal() : null;
            default:
                return null;
        }
    }

    /**
     * 提取数值类型字段值
     */
    private Number extractNumericValue(RowData row, int fieldIndex) {
        if (row.isNullAt(fieldIndex)) {
            return null;
        }

        LogicalType fieldType = sourceRowType.getTypeAt(fieldIndex);
        switch (fieldType.getTypeRoot()) {
            case TINYINT:
                return row.getByte(fieldIndex);
            case SMALLINT:
                return row.getShort(fieldIndex);
            case INTEGER:
                return row.getInt(fieldIndex);
            case BIGINT:
                return row.getLong(fieldIndex);
            case FLOAT:
                return row.getFloat(fieldIndex);
            case DOUBLE:
                return row.getDouble(fieldIndex);
            case DECIMAL:
                DecimalType decType = (DecimalType) fieldType;
                DecimalData decData = row.getDecimal(fieldIndex, decType.getPrecision(), decType.getScale());
                return decData != null ? decData.toBigDecimal() : null;
            default:
                return null;
        }
    }

    /**
     * 提取可比较类型字段值
     */
    @SuppressWarnings("unchecked")
    private Comparable<?> extractComparableValue(RowData row, int fieldIndex) {
        Object value = extractValue(row, fieldIndex);
        if (value instanceof Comparable) {
            return (Comparable<?>) value;
        }
        return null;
    }

    /**
     * 重置聚合状态
     */
    public void reset() {
        if (aggregateStates != null) {
            aggregateStates.clear();
        }
    }

    /**
     * 分组键
     */
    private static class GroupKey implements Serializable {
        private static final long serialVersionUID = 1L;
        
        static final GroupKey EMPTY = new GroupKey(new Object[0]);

        private final Object[] values;
        private final int hashCode;

        GroupKey(Object[] values) {
            this.values = values;
            this.hashCode = Objects.hash((Object[]) values);
        }

        Object[] getValues() {
            return values;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GroupKey groupKey = (GroupKey) o;
            return java.util.Arrays.equals(values, groupKey.values);
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    /**
     * 聚合状态
     */
    private static class AggregateState implements Serializable {
        private static final long serialVersionUID = 1L;

        private final long[] counts;
        private final double[] sums;
        private final long[] avgCounts;  // 用于计算 AVG
        private final Comparable<?>[] mins;
        private final Comparable<?>[] maxs;
        private final Set<Object>[] distinctSets;

        @SuppressWarnings("unchecked")
        AggregateState(int numAggregates) {
            this.counts = new long[numAggregates];
            this.sums = new double[numAggregates];
            this.avgCounts = new long[numAggregates];
            this.mins = new Comparable<?>[numAggregates];
            this.maxs = new Comparable<?>[numAggregates];
            this.distinctSets = new Set[numAggregates];
        }

        void incrementCount(int index) {
            counts[index]++;
        }

        long getCount(int index) {
            return counts[index];
        }

        void addDistinctValue(int index, Object value) {
            if (distinctSets[index] == null) {
                distinctSets[index] = new HashSet<>();
            }
            distinctSets[index].add(value);
        }

        int getDistinctCount(int index) {
            return distinctSets[index] != null ? distinctSets[index].size() : 0;
        }

        void addSum(int index, double value) {
            sums[index] += value;
            counts[index]++;  // 标记有值
        }

        Double getSum(int index) {
            return counts[index] > 0 ? sums[index] : null;
        }

        void addForAvg(int index, double value) {
            sums[index] += value;
            avgCounts[index]++;
        }

        Double getAvg(int index) {
            return avgCounts[index] > 0 ? sums[index] / avgCounts[index] : null;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        void updateMin(int index, Comparable<?> value) {
            if (mins[index] == null || ((Comparable) value).compareTo(mins[index]) < 0) {
                mins[index] = value;
            }
        }

        Comparable<?> getMin(int index) {
            return mins[index];
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        void updateMax(int index, Comparable<?> value) {
            if (maxs[index] == null || ((Comparable) value).compareTo(maxs[index]) > 0) {
                maxs[index] = value;
            }
        }

        Comparable<?> getMax(int index) {
            return maxs[index];
        }
    }

    /**
     * 构建聚合结果的 RowType
     */
    public RowType buildResultRowType() {
        List<String> groupByCols = aggregateInfo.getGroupByColumns();
        List<AggregateInfo.AggregateCall> calls = aggregateInfo.getAggregateCalls();

        List<RowType.RowField> fields = new ArrayList<>();

        // 分组列
        for (String col : groupByCols) {
            int fieldIndex = getFieldIndex(col);
            if (fieldIndex >= 0) {
                LogicalType fieldType = sourceRowType.getTypeAt(fieldIndex);
                fields.add(new RowType.RowField(col, fieldType));
            }
        }

        // 聚合结果列
        for (AggregateInfo.AggregateCall call : calls) {
            String alias = call.getAlias() != null ? call.getAlias() : 
                    call.getFunction().name().toLowerCase() + "_" + 
                    (call.getColumn() != null ? call.getColumn() : "star");
            LogicalType resultType = getAggregateResultType(call);
            fields.add(new RowType.RowField(alias, resultType));
        }

        return new RowType(fields);
    }

    /**
     * 获取聚合函数的结果类型
     */
    private LogicalType getAggregateResultType(AggregateInfo.AggregateCall call) {
        switch (call.getFunction()) {
            case COUNT:
            case COUNT_DISTINCT:
                return new BigIntType();
            case SUM:
            case AVG:
                return new DoubleType();
            case MIN:
            case MAX:
                if (call.getColumn() != null) {
                    int fieldIndex = getFieldIndex(call.getColumn());
                    if (fieldIndex >= 0) {
                        return sourceRowType.getTypeAt(fieldIndex);
                    }
                }
                return new DoubleType();
            default:
                return new DoubleType();
        }
    }
}

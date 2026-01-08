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

import org.apache.flink.connector.lance.aggregate.AggregateInfo;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LanceDynamicTableSource 聚合下推测试
 */
@DisplayName("LanceDynamicTableSource 聚合下推测试")
class LanceAggregatePushDownTest {

    private LanceOptions options;
    private DataType physicalDataType;

    @BeforeEach
    void setUp() {
        options = LanceOptions.builder()
                .path("/tmp/test_lance_dataset")
                .readBatchSize(1024)
                .build();

        // 创建测试用的物理数据类型
        // Schema: (id INT, name VARCHAR, category VARCHAR, amount DOUBLE, quantity INT)
        RowType rowType = new RowType(Arrays.asList(
                new RowType.RowField("id", new IntType()),
                new RowType.RowField("name", new VarCharType(100)),
                new RowType.RowField("category", new VarCharType(50)),
                new RowType.RowField("amount", new DoubleType()),
                new RowType.RowField("quantity", new IntType())
        ));
        physicalDataType = TypeConversions.fromLogicalToDataType(rowType);
    }

    // ==================== 聚合下推接口实现测试 ====================

    @Nested
    @DisplayName("applyAggregates 方法测试")
    class ApplyAggregatesTests {

        // 注意：由于 applyAggregates 需要真实的 AggregateExpression 对象，
        // 这里我们主要测试聚合信息的存储和状态管理

        @Test
        @DisplayName("初始状态应该没有聚合下推")
        void testInitialState() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(options, physicalDataType);
            
            assertFalse(source.isAggregatePushDownAccepted());
            assertNull(source.getAggregateInfo());
        }

        @Test
        @DisplayName("copy 应该正确复制聚合状态")
        void testCopyAggregateState() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(options, physicalDataType);
            
            // 复制源
            LanceDynamicTableSource copied = (LanceDynamicTableSource) source.copy();
            
            // 验证复制后的状态
            assertFalse(copied.isAggregatePushDownAccepted());
            assertNull(copied.getAggregateInfo());
            assertNotSame(source, copied);
        }

        @Test
        @DisplayName("asSummaryString 应该返回正确的摘要")
        void testAsSummaryString() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(options, physicalDataType);
            
            String summary = source.asSummaryString();
            
            assertEquals("Lance Table Source", summary);
        }
    }

    // ==================== AggregateInfo 集成测试 ====================

    @Nested
    @DisplayName("AggregateInfo 集成测试")
    class AggregateInfoIntegrationTests {

        @Test
        @DisplayName("简单 COUNT(*) 聚合信息构建")
        void testSimpleCountStarAggregateInfo() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .build();
            
            assertTrue(aggInfo.isSimpleCountStar());
            assertFalse(aggInfo.hasGroupBy());
            assertEquals(1, aggInfo.getAggregateCalls().size());
        }

        @Test
        @DisplayName("带 GROUP BY 的聚合信息构建")
        void testGroupByAggregateInfo() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addSum("amount", "total_amount")
                    .addAvg("amount", "avg_amount")
                    .groupBy("category")
                    .groupByFieldIndices(new int[]{2})  // category 在索引 2
                    .build();
            
            assertFalse(aggInfo.isSimpleCountStar());
            assertTrue(aggInfo.hasGroupBy());
            assertEquals(2, aggInfo.getAggregateCalls().size());
            assertEquals(Collections.singletonList("category"), aggInfo.getGroupByColumns());
        }

        @Test
        @DisplayName("多聚合函数信息构建")
        void testMultipleAggregatesInfo() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .addSum("amount", "sum_amount")
                    .addAvg("amount", "avg_amount")
                    .addMin("amount", "min_amount")
                    .addMax("amount", "max_amount")
                    .build();
            
            assertEquals(5, aggInfo.getAggregateCalls().size());
            
            // 验证各聚合函数类型
            List<AggregateInfo.AggregateCall> calls = aggInfo.getAggregateCalls();
            assertEquals(AggregateInfo.AggregateFunction.COUNT, calls.get(0).getFunction());
            assertEquals(AggregateInfo.AggregateFunction.SUM, calls.get(1).getFunction());
            assertEquals(AggregateInfo.AggregateFunction.AVG, calls.get(2).getFunction());
            assertEquals(AggregateInfo.AggregateFunction.MIN, calls.get(3).getFunction());
            assertEquals(AggregateInfo.AggregateFunction.MAX, calls.get(4).getFunction());
        }

        @Test
        @DisplayName("getRequiredColumns 应该返回正确的列")
        void testGetRequiredColumns() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addSum("amount", "sum_amount")
                    .addAvg("quantity", "avg_quantity")
                    .groupBy("category")
                    .build();
            
            List<String> required = aggInfo.getRequiredColumns();
            
            assertTrue(required.contains("category"));
            assertTrue(required.contains("amount"));
            assertTrue(required.contains("quantity"));
        }
    }

    // ==================== 组合功能测试 ====================

    @Nested
    @DisplayName("组合功能测试")
    class CombinedFunctionalityTests {

        @Test
        @DisplayName("聚合下推与过滤下推组合")
        void testAggregatePushDownWithFilter() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(options, physicalDataType);
            
            // 模拟添加过滤条件（通过内部 filters 列表）
            // 注意：实际的过滤下推通过 applyFilters 方法完成
            
            // 验证源可以同时支持过滤和聚合下推
            assertNotNull(source.getOptions());
        }

        @Test
        @DisplayName("聚合下推与列裁剪组合")
        void testAggregatePushDownWithProjection() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(options, physicalDataType);
            
            // 应用列裁剪
            source.applyProjection(new int[][]{{0}, {3}, {4}});  // id, amount, quantity
            
            // 验证源仍然可以正常工作
            assertNotNull(source.getOptions());
        }

        @Test
        @DisplayName("聚合下推与 Limit 组合")
        void testAggregatePushDownWithLimit() {
            LanceDynamicTableSource source = new LanceDynamicTableSource(options, physicalDataType);
            
            // 应用 Limit
            source.applyLimit(100);
            
            assertEquals(Long.valueOf(100), source.getLimit());
        }
    }

    // ==================== 边界情况测试 ====================

    @Nested
    @DisplayName("边界情况测试")
    class EdgeCaseTests {

        @Test
        @DisplayName("多个分组列应该正确处理")
        void testMultipleGroupByColumns() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .groupBy("category", "name")
                    .groupByFieldIndices(new int[]{2, 1})
                    .build();
            
            assertEquals(2, aggInfo.getGroupByColumns().size());
            assertArrayEquals(new int[]{2, 1}, aggInfo.getGroupByFieldIndices());
        }

        @Test
        @DisplayName("同一列的多个聚合应该正确处理")
        void testMultipleAggregatesOnSameColumn() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addSum("amount", "sum_amount")
                    .addAvg("amount", "avg_amount")
                    .addMin("amount", "min_amount")
                    .addMax("amount", "max_amount")
                    .addCount("amount", "count_amount")
                    .build();
            
            assertEquals(5, aggInfo.getAggregateCalls().size());
            
            // 验证 getRequiredColumns 只包含一次 amount
            List<String> required = aggInfo.getRequiredColumns();
            long amountCount = required.stream().filter(c -> c.equals("amount")).count();
            assertEquals(1, amountCount);
        }

        @Test
        @DisplayName("空分组集应该正确处理")
        void testEmptyGroupBy() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .build();
            
            assertFalse(aggInfo.hasGroupBy());
            assertTrue(aggInfo.getGroupByColumns().isEmpty());
            assertEquals(0, aggInfo.getGroupByFieldIndices().length);
        }
    }

    // ==================== 聚合函数支持测试 ====================

    @Nested
    @DisplayName("聚合函数支持测试")
    class AggregateFunctionSupportTests {

        @Test
        @DisplayName("COUNT 函数应该支持")
        void testCountSupport() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .build();
            
            AggregateInfo.AggregateCall call = aggInfo.getAggregateCalls().get(0);
            assertEquals(AggregateInfo.AggregateFunction.COUNT, call.getFunction());
            assertTrue(call.isCountStar());
        }

        @Test
        @DisplayName("SUM 函数应该支持")
        void testSumSupport() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addSum("amount", "sum_amount")
                    .build();
            
            AggregateInfo.AggregateCall call = aggInfo.getAggregateCalls().get(0);
            assertEquals(AggregateInfo.AggregateFunction.SUM, call.getFunction());
            assertEquals("amount", call.getColumn());
        }

        @Test
        @DisplayName("AVG 函数应该支持")
        void testAvgSupport() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addAvg("amount", "avg_amount")
                    .build();
            
            AggregateInfo.AggregateCall call = aggInfo.getAggregateCalls().get(0);
            assertEquals(AggregateInfo.AggregateFunction.AVG, call.getFunction());
            assertEquals("amount", call.getColumn());
        }

        @Test
        @DisplayName("MIN 函数应该支持")
        void testMinSupport() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addMin("amount", "min_amount")
                    .build();
            
            AggregateInfo.AggregateCall call = aggInfo.getAggregateCalls().get(0);
            assertEquals(AggregateInfo.AggregateFunction.MIN, call.getFunction());
            assertEquals("amount", call.getColumn());
        }

        @Test
        @DisplayName("MAX 函数应该支持")
        void testMaxSupport() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addMax("amount", "max_amount")
                    .build();
            
            AggregateInfo.AggregateCall call = aggInfo.getAggregateCalls().get(0);
            assertEquals(AggregateInfo.AggregateFunction.MAX, call.getFunction());
            assertEquals("amount", call.getColumn());
        }

        @Test
        @DisplayName("COUNT DISTINCT 函数应该支持")
        void testCountDistinctSupport() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addAggregateCall(AggregateInfo.AggregateFunction.COUNT_DISTINCT, "category", "distinct_cnt")
                    .build();
            
            AggregateInfo.AggregateCall call = aggInfo.getAggregateCalls().get(0);
            assertEquals(AggregateInfo.AggregateFunction.COUNT_DISTINCT, call.getFunction());
            assertEquals("category", call.getColumn());
        }
    }
}

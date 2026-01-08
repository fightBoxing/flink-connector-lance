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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * AggregateExecutor 单元测试
 */
@DisplayName("AggregateExecutor 单元测试")
class AggregateExecutorTest {

    private RowType sourceRowType;

    @BeforeEach
    void setUp() {
        // 创建测试用的 RowType: (id INT, name VARCHAR, category VARCHAR, amount DOUBLE, quantity INT)
        sourceRowType = RowType.of(
                new IntType(),
                new VarCharType(100),
                new VarCharType(50),
                new DoubleType(),
                new IntType()
        );
        sourceRowType = new RowType(Arrays.asList(
                new RowType.RowField("id", new IntType()),
                new RowType.RowField("name", new VarCharType(100)),
                new RowType.RowField("category", new VarCharType(50)),
                new RowType.RowField("amount", new DoubleType()),
                new RowType.RowField("quantity", new IntType())
        ));
    }

    /**
     * 创建测试数据行
     */
    private RowData createRow(int id, String name, String category, double amount, int quantity) {
        GenericRowData row = new GenericRowData(5);
        row.setField(0, id);
        row.setField(1, StringData.fromString(name));
        row.setField(2, StringData.fromString(category));
        row.setField(3, amount);
        row.setField(4, quantity);
        return row;
    }

    // ==================== COUNT 聚合测试 ====================

    @Nested
    @DisplayName("COUNT 聚合测试")
    class CountAggregateTests {

        @Test
        @DisplayName("COUNT(*) 应该正确计数所有行")
        void testCountStar() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            // 累积 5 行数据
            executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
            executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
            executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));
            executor.accumulate(createRow(4, "David", "B", 180.0, 18));
            executor.accumulate(createRow(5, "Eve", "C", 220.0, 22));
            
            List<RowData> results = executor.getResults();
            
            assertEquals(1, results.size());
            assertEquals(5L, results.get(0).getLong(0));  // COUNT(*)
        }

        @Test
        @DisplayName("COUNT(column) 应该正确计数非空值")
        void testCountColumn() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCount("name", "name_count")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
            executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
            executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));
            
            List<RowData> results = executor.getResults();
            
            assertEquals(1, results.size());
            assertEquals(3L, results.get(0).getLong(0));
        }

        @Test
        @DisplayName("空数据集的 COUNT(*) 应该返回 0")
        void testCountStarEmpty() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            List<RowData> results = executor.getResults();
            
            assertEquals(1, results.size());
            assertEquals(0L, results.get(0).getLong(0));
        }
    }

    // ==================== SUM 聚合测试 ====================

    @Nested
    @DisplayName("SUM 聚合测试")
    class SumAggregateTests {

        @Test
        @DisplayName("SUM 应该正确求和")
        void testSum() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addSum("amount", "total_amount")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
            executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
            executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));
            
            List<RowData> results = executor.getResults();
            
            assertEquals(1, results.size());
            assertEquals(450.0, results.get(0).getDouble(0), 0.001);
        }

        @Test
        @DisplayName("空数据集的 SUM 应该返回 null")
        void testSumEmpty() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addSum("amount", "total_amount")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            List<RowData> results = executor.getResults();
            
            assertEquals(1, results.size());
            assertTrue(results.get(0).isNullAt(0));
        }
    }

    // ==================== AVG 聚合测试 ====================

    @Nested
    @DisplayName("AVG 聚合测试")
    class AvgAggregateTests {

        @Test
        @DisplayName("AVG 应该正确计算平均值")
        void testAvg() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addAvg("amount", "avg_amount")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
            executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
            executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));
            
            List<RowData> results = executor.getResults();
            
            assertEquals(1, results.size());
            assertEquals(150.0, results.get(0).getDouble(0), 0.001);  // (100+200+150)/3
        }

        @Test
        @DisplayName("空数据集的 AVG 应该返回 null")
        void testAvgEmpty() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addAvg("amount", "avg_amount")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            List<RowData> results = executor.getResults();
            
            assertEquals(1, results.size());
            assertTrue(results.get(0).isNullAt(0));
        }
    }

    // ==================== MIN/MAX 聚合测试 ====================

    @Nested
    @DisplayName("MIN/MAX 聚合测试")
    class MinMaxAggregateTests {

        @Test
        @DisplayName("MIN 应该返回最小值")
        void testMin() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addMin("amount", "min_amount")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
            executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
            executor.accumulate(createRow(3, "Charlie", "A", 50.0, 15));
            
            List<RowData> results = executor.getResults();
            
            assertEquals(1, results.size());
            assertEquals(50.0, results.get(0).getDouble(0), 0.001);
        }

        @Test
        @DisplayName("MAX 应该返回最大值")
        void testMax() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addMax("amount", "max_amount")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
            executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
            executor.accumulate(createRow(3, "Charlie", "A", 50.0, 15));
            
            List<RowData> results = executor.getResults();
            
            assertEquals(1, results.size());
            assertEquals(200.0, results.get(0).getDouble(0), 0.001);
        }

        @Test
        @DisplayName("空数据集的 MIN/MAX 应该返回 null")
        void testMinMaxEmpty() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addMin("amount", "min_amount")
                    .addMax("amount", "max_amount")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            List<RowData> results = executor.getResults();
            
            assertEquals(1, results.size());
            assertTrue(results.get(0).isNullAt(0));  // MIN
            assertTrue(results.get(0).isNullAt(1));  // MAX
        }
    }

    // ==================== GROUP BY 测试 ====================

    @Nested
    @DisplayName("GROUP BY 聚合测试")
    class GroupByAggregateTests {

        @Test
        @DisplayName("带 GROUP BY 的 COUNT 应该按分组计数")
        void testGroupByCount() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .groupBy("category")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
            executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
            executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));
            executor.accumulate(createRow(4, "David", "B", 180.0, 18));
            executor.accumulate(createRow(5, "Eve", "A", 220.0, 22));
            
            List<RowData> results = executor.getResults();
            
            assertEquals(2, results.size());  // 2 个分组: A 和 B
            
            // 验证每个分组的计数
            long countA = 0, countB = 0;
            for (RowData row : results) {
                String category = row.getString(0).toString();
                long count = row.getLong(1);
                if ("A".equals(category)) {
                    countA = count;
                } else if ("B".equals(category)) {
                    countB = count;
                }
            }
            assertEquals(3, countA);  // A 有 3 条
            assertEquals(2, countB);  // B 有 2 条
        }

        @Test
        @DisplayName("带 GROUP BY 的 SUM 应该按分组求和")
        void testGroupBySum() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addSum("amount", "total_amount")
                    .groupBy("category")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
            executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
            executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));
            
            List<RowData> results = executor.getResults();
            
            assertEquals(2, results.size());
            
            // 验证每个分组的求和
            for (RowData row : results) {
                String category = row.getString(0).toString();
                double sum = row.getDouble(1);
                if ("A".equals(category)) {
                    assertEquals(250.0, sum, 0.001);  // 100 + 150
                } else if ("B".equals(category)) {
                    assertEquals(200.0, sum, 0.001);  // 200
                }
            }
        }

        @Test
        @DisplayName("空数据集带 GROUP BY 应该返回空结果")
        void testGroupByEmpty() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .groupBy("category")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            List<RowData> results = executor.getResults();
            
            assertTrue(results.isEmpty());
        }
    }

    // ==================== 多聚合函数测试 ====================

    @Nested
    @DisplayName("多聚合函数测试")
    class MultipleAggregatesTests {

        @Test
        @DisplayName("多个聚合函数应该同时工作")
        void testMultipleAggregates() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .addSum("amount", "sum_amount")
                    .addAvg("amount", "avg_amount")
                    .addMin("amount", "min_amount")
                    .addMax("amount", "max_amount")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
            executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
            executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));
            
            List<RowData> results = executor.getResults();
            
            assertEquals(1, results.size());
            RowData result = results.get(0);
            
            assertEquals(3L, result.getLong(0));           // COUNT(*)
            assertEquals(450.0, result.getDouble(1), 0.001); // SUM
            assertEquals(150.0, result.getDouble(2), 0.001); // AVG
            assertEquals(100.0, result.getDouble(3), 0.001); // MIN
            assertEquals(200.0, result.getDouble(4), 0.001); // MAX
        }

        @Test
        @DisplayName("多聚合函数带 GROUP BY 应该正确工作")
        void testMultipleAggregatesWithGroupBy() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .addSum("amount", "sum_amount")
                    .addAvg("amount", "avg_amount")
                    .groupBy("category")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
            executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
            executor.accumulate(createRow(3, "Charlie", "A", 200.0, 15));
            
            List<RowData> results = executor.getResults();
            
            assertEquals(2, results.size());
            
            for (RowData row : results) {
                String category = row.getString(0).toString();
                long count = row.getLong(1);
                double sum = row.getDouble(2);
                double avg = row.getDouble(3);
                
                if ("A".equals(category)) {
                    assertEquals(2, count);
                    assertEquals(300.0, sum, 0.001);
                    assertEquals(150.0, avg, 0.001);
                } else if ("B".equals(category)) {
                    assertEquals(1, count);
                    assertEquals(200.0, sum, 0.001);
                    assertEquals(200.0, avg, 0.001);
                }
            }
        }
    }

    // ==================== 重置测试 ====================

    @Nested
    @DisplayName("重置测试")
    class ResetTests {

        @Test
        @DisplayName("reset 应该清空聚合状态")
        void testReset() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            executor.accumulate(createRow(1, "Alice", "A", 100.0, 10));
            executor.accumulate(createRow(2, "Bob", "B", 200.0, 20));
            
            // 重置
            executor.reset();
            
            // 重新初始化并累积新数据
            executor.init();
            executor.accumulate(createRow(3, "Charlie", "A", 150.0, 15));
            
            List<RowData> results = executor.getResults();
            
            assertEquals(1, results.size());
            assertEquals(1L, results.get(0).getLong(0));  // 只有重置后的 1 条
        }
    }

    // ==================== 结果类型测试 ====================

    @Nested
    @DisplayName("结果类型测试")
    class ResultTypeTests {

        @Test
        @DisplayName("buildResultRowType 应该返回正确的结果类型")
        void testBuildResultRowType() {
            AggregateInfo aggInfo = AggregateInfo.builder()
                    .addCountStar("cnt")
                    .addSum("amount", "sum_amount")
                    .groupBy("category")
                    .build();
            
            AggregateExecutor executor = new AggregateExecutor(aggInfo, sourceRowType);
            executor.init();
            
            RowType resultType = executor.buildResultRowType();
            
            assertNotNull(resultType);
            assertEquals(3, resultType.getFieldCount());
            
            // 第一个字段是分组列 category
            assertEquals("category", resultType.getFieldNames().get(0));
            
            // 第二个字段是 COUNT 结果
            assertEquals("cnt", resultType.getFieldNames().get(1));
            assertTrue(resultType.getTypeAt(1) instanceof BigIntType);
            
            // 第三个字段是 SUM 结果
            assertEquals("sum_amount", resultType.getFieldNames().get(2));
            assertTrue(resultType.getTypeAt(2) instanceof DoubleType);
        }
    }
}

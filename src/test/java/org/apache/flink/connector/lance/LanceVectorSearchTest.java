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

package org.apache.flink.connector.lance;

import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.connector.lance.config.LanceOptions.MetricType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * LanceVectorSearch 单元测试。
 */
class LanceVectorSearchTest {

    @TempDir
    Path tempDir;

    private String datasetPath;

    @BeforeEach
    void setUp() {
        datasetPath = tempDir.resolve("test_search_dataset").toString();
    }

    @Test
    @DisplayName("测试向量检索配置构建")
    void testVectorSearchConfiguration() {
        LanceVectorSearch search = LanceVectorSearch.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .metricType(MetricType.L2)
                .nprobes(20)
                .ef(100)
                .refineFactor(10)
                .build();

        assertThat(search).isNotNull();
    }

    @Test
    @DisplayName("测试不同距离度量类型")
    void testDifferentMetricTypes() {
        // L2 距离
        LanceVectorSearch l2Search = LanceVectorSearch.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .metricType(MetricType.L2)
                .build();
        assertThat(l2Search).isNotNull();

        // Cosine 相似度
        LanceVectorSearch cosineSearch = LanceVectorSearch.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .metricType(MetricType.COSINE)
                .build();
        assertThat(cosineSearch).isNotNull();

        // Dot 点积
        LanceVectorSearch dotSearch = LanceVectorSearch.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .metricType(MetricType.DOT)
                .build();
        assertThat(dotSearch).isNotNull();
    }

    @Test
    @DisplayName("测试缺少数据集路径时抛出异常")
    void testMissingDatasetPath() {
        assertThatThrownBy(() -> LanceVectorSearch.builder()
                .columnName("embedding")
                .metricType(MetricType.L2)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("数据集路径不能为空");
    }

    @Test
    @DisplayName("测试缺少列名时抛出异常")
    void testMissingColumnName() {
        assertThatThrownBy(() -> LanceVectorSearch.builder()
                .datasetPath(datasetPath)
                .metricType(MetricType.L2)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("列名不能为空");
    }

    @Test
    @DisplayName("测试无效的 nprobes 值")
    void testInvalidNprobes() {
        assertThatThrownBy(() -> LanceVectorSearch.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .nprobes(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nprobes 必须大于 0");
    }

    @Test
    @DisplayName("测试默认向量检索配置值")
    void testDefaultVectorSearchConfiguration() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .vectorColumn("embedding")
                .build();

        // 验证默认值
        assertThat(options.getVectorMetric()).isEqualTo(MetricType.L2);
        assertThat(options.getVectorNprobes()).isEqualTo(20);
        assertThat(options.getVectorEf()).isEqualTo(100);
        assertThat(options.getVectorRefineFactor()).isNull();
    }

    @Test
    @DisplayName("测试从 LanceOptions 创建向量检索器")
    void testFromOptions() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .vectorColumn("embedding")
                .vectorMetric(MetricType.COSINE)
                .vectorNprobes(30)
                .vectorEf(150)
                .vectorRefineFactor(5)
                .build();

        LanceVectorSearch search = LanceVectorSearch.fromOptions(options);

        assertThat(search).isNotNull();
    }

    @Test
    @DisplayName("测试检索结果")
    void testSearchResult() {
        // 创建模拟的 RowData
        LanceVectorSearch.SearchResult result = new LanceVectorSearch.SearchResult(null, 0.5);

        assertThat(result.getDistance()).isEqualTo(0.5);
        assertThat(result.getSimilarity()).isGreaterThan(0);
        assertThat(result.getSimilarity()).isLessThanOrEqualTo(1.0);
    }

    @Test
    @DisplayName("测试检索结果相似度计算")
    void testSearchResultSimilarity() {
        // 距离为 0 时，相似度应为 1.0
        LanceVectorSearch.SearchResult perfectMatch = new LanceVectorSearch.SearchResult(null, 0.0);
        assertThat(perfectMatch.getSimilarity()).isEqualTo(1.0);

        // 距离为 1 时，相似度应为 0.5
        LanceVectorSearch.SearchResult halfMatch = new LanceVectorSearch.SearchResult(null, 1.0);
        assertThat(halfMatch.getSimilarity()).isEqualTo(0.5);

        // 距离越大，相似度越小
        LanceVectorSearch.SearchResult farResult = new LanceVectorSearch.SearchResult(null, 10.0);
        assertThat(farResult.getSimilarity()).isLessThan(0.5);
    }

    @Test
    @DisplayName("测试检索结果相等性")
    void testSearchResultEquality() {
        LanceVectorSearch.SearchResult result1 = new LanceVectorSearch.SearchResult(null, 0.5);
        LanceVectorSearch.SearchResult result2 = new LanceVectorSearch.SearchResult(null, 0.5);
        LanceVectorSearch.SearchResult result3 = new LanceVectorSearch.SearchResult(null, 1.0);

        assertThat(result1).isEqualTo(result2);
        assertThat(result1.hashCode()).isEqualTo(result2.hashCode());
        assertThat(result1).isNotEqualTo(result3);
    }

    @Test
    @DisplayName("测试配置校验 - 无效的向量检索参数")
    void testInvalidVectorSearchParams() {
        assertThatThrownBy(() -> LanceOptions.builder()
                .path(datasetPath)
                .vectorColumn("embedding")
                .vectorNprobes(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nprobes");

        assertThatThrownBy(() -> LanceOptions.builder()
                .path(datasetPath)
                .vectorColumn("embedding")
                .vectorEf(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ef");

        assertThatThrownBy(() -> LanceOptions.builder()
                .path(datasetPath)
                .vectorColumn("embedding")
                .vectorRefineFactor(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("refine-factor");
    }

    @Test
    @DisplayName("测试距离度量类型值")
    void testMetricTypeValues() {
        assertThat(MetricType.L2.getValue()).isEqualTo("L2");
        assertThat(MetricType.COSINE.getValue()).isEqualTo("Cosine");
        assertThat(MetricType.DOT.getValue()).isEqualTo("Dot");
    }

    @Test
    @DisplayName("测试检索结果 toString")
    void testSearchResultToString() {
        LanceVectorSearch.SearchResult result = new LanceVectorSearch.SearchResult(null, 0.5);
        String str = result.toString();

        assertThat(str).contains("SearchResult");
        assertThat(str).contains("distance=0.5");
    }
}

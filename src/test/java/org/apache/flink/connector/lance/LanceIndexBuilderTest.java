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
import org.apache.flink.connector.lance.config.LanceOptions.IndexType;
import org.apache.flink.connector.lance.config.LanceOptions.MetricType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * LanceIndexBuilder 单元测试。
 */
class LanceIndexBuilderTest {

    @TempDir
    Path tempDir;

    private String datasetPath;

    @BeforeEach
    void setUp() {
        datasetPath = tempDir.resolve("test_index_dataset").toString();
    }

    @Test
    @DisplayName("测试 IVF_PQ 索引配置构建")
    void testIvfPqIndexConfiguration() {
        LanceIndexBuilder builder = LanceIndexBuilder.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .indexType(IndexType.IVF_PQ)
                .numPartitions(128)
                .numSubVectors(16)
                .numBits(8)
                .metricType(MetricType.L2)
                .build();

        // 验证配置 - 通过构建成功验证
        assertThat(builder).isNotNull();
    }

    @Test
    @DisplayName("测试 IVF_HNSW 索引配置构建")
    void testIvfHnswIndexConfiguration() {
        LanceIndexBuilder builder = LanceIndexBuilder.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .indexType(IndexType.IVF_HNSW)
                .numPartitions(64)
                .maxLevel(5)
                .m(24)
                .efConstruction(200)
                .metricType(MetricType.COSINE)
                .build();

        assertThat(builder).isNotNull();
    }

    @Test
    @DisplayName("测试 IVF_FLAT 索引配置构建")
    void testIvfFlatIndexConfiguration() {
        LanceIndexBuilder builder = LanceIndexBuilder.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .indexType(IndexType.IVF_FLAT)
                .numPartitions(256)
                .metricType(MetricType.DOT)
                .build();

        assertThat(builder).isNotNull();
    }

    @Test
    @DisplayName("测试索引类型枚举")
    void testIndexTypeEnum() {
        assertThat(IndexType.fromValue("IVF_PQ")).isEqualTo(IndexType.IVF_PQ);
        assertThat(IndexType.fromValue("ivf_pq")).isEqualTo(IndexType.IVF_PQ);
        assertThat(IndexType.fromValue("IVF_HNSW")).isEqualTo(IndexType.IVF_HNSW);
        assertThat(IndexType.fromValue("IVF_FLAT")).isEqualTo(IndexType.IVF_FLAT);

        assertThat(IndexType.IVF_PQ.getValue()).isEqualTo("IVF_PQ");
        assertThat(IndexType.IVF_HNSW.getValue()).isEqualTo("IVF_HNSW");
        assertThat(IndexType.IVF_FLAT.getValue()).isEqualTo("IVF_FLAT");
    }

    @Test
    @DisplayName("测试无效的索引类型")
    void testInvalidIndexType() {
        assertThatThrownBy(() -> IndexType.fromValue("INVALID"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("不支持的索引类型");
    }

    @Test
    @DisplayName("测试距离度量类型枚举")
    void testMetricTypeEnum() {
        assertThat(MetricType.fromValue("L2")).isEqualTo(MetricType.L2);
        assertThat(MetricType.fromValue("l2")).isEqualTo(MetricType.L2);
        assertThat(MetricType.fromValue("Cosine")).isEqualTo(MetricType.COSINE);
        assertThat(MetricType.fromValue("cosine")).isEqualTo(MetricType.COSINE);
        assertThat(MetricType.fromValue("Dot")).isEqualTo(MetricType.DOT);
        assertThat(MetricType.fromValue("dot")).isEqualTo(MetricType.DOT);

        assertThat(MetricType.L2.getValue()).isEqualTo("L2");
        assertThat(MetricType.COSINE.getValue()).isEqualTo("Cosine");
        assertThat(MetricType.DOT.getValue()).isEqualTo("Dot");
    }

    @Test
    @DisplayName("测试无效的距离度量类型")
    void testInvalidMetricType() {
        assertThatThrownBy(() -> MetricType.fromValue("INVALID"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("不支持的距离度量类型");
    }

    @Test
    @DisplayName("测试缺少数据集路径时抛出异常")
    void testMissingDatasetPath() {
        assertThatThrownBy(() -> LanceIndexBuilder.builder()
                .columnName("embedding")
                .indexType(IndexType.IVF_PQ)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("数据集路径不能为空");
    }

    @Test
    @DisplayName("测试缺少列名时抛出异常")
    void testMissingColumnName() {
        assertThatThrownBy(() -> LanceIndexBuilder.builder()
                .datasetPath(datasetPath)
                .indexType(IndexType.IVF_PQ)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("列名不能为空");
    }

    @Test
    @DisplayName("测试无效的分区数")
    void testInvalidNumPartitions() {
        assertThatThrownBy(() -> LanceIndexBuilder.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .numPartitions(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("分区数必须大于 0");
    }

    @Test
    @DisplayName("测试无效的子向量数")
    void testInvalidNumSubVectors() {
        assertThatThrownBy(() -> LanceIndexBuilder.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .numSubVectors(-1)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("子向量数必须大于 0");
    }

    @Test
    @DisplayName("测试无效的量化位数")
    void testInvalidNumBits() {
        assertThatThrownBy(() -> LanceIndexBuilder.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .numBits(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("量化位数必须在 1-16 之间");

        assertThatThrownBy(() -> LanceIndexBuilder.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .numBits(17)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("量化位数必须在 1-16 之间");
    }

    @Test
    @DisplayName("测试默认索引配置值")
    void testDefaultIndexConfiguration() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .indexColumn("embedding")
                .build();

        // 验证默认值
        assertThat(options.getIndexType()).isEqualTo(IndexType.IVF_PQ);
        assertThat(options.getIndexNumPartitions()).isEqualTo(256);
        assertThat(options.getIndexNumBits()).isEqualTo(8);
        assertThat(options.getIndexMaxLevel()).isEqualTo(7);
        assertThat(options.getIndexM()).isEqualTo(16);
        assertThat(options.getIndexEfConstruction()).isEqualTo(100);
    }

    @Test
    @DisplayName("测试从 LanceOptions 创建索引构建器")
    void testFromOptions() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .indexColumn("embedding")
                .indexType(IndexType.IVF_HNSW)
                .indexNumPartitions(64)
                .vectorMetric(MetricType.COSINE)
                .build();

        LanceIndexBuilder builder = LanceIndexBuilder.fromOptions(options);

        assertThat(builder).isNotNull();
    }

    @Test
    @DisplayName("测试索引构建结果")
    void testIndexBuildResult() {
        LanceIndexBuilder.IndexBuildResult result = new LanceIndexBuilder.IndexBuildResult(
                true,
                IndexType.IVF_PQ,
                "embedding",
                datasetPath,
                1000,
                null
        );

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.getIndexType()).isEqualTo(IndexType.IVF_PQ);
        assertThat(result.getColumnName()).isEqualTo("embedding");
        assertThat(result.getDatasetPath()).isEqualTo(datasetPath);
        assertThat(result.getDurationMillis()).isEqualTo(1000);
        assertThat(result.getErrorMessage()).isNull();
    }

    @Test
    @DisplayName("测试索引构建失败结果")
    void testIndexBuildFailureResult() {
        LanceIndexBuilder.IndexBuildResult result = new LanceIndexBuilder.IndexBuildResult(
                false,
                IndexType.IVF_PQ,
                "embedding",
                datasetPath,
                500,
                "列不存在"
        );

        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getErrorMessage()).isEqualTo("列不存在");
    }

    @Test
    @DisplayName("测试替换索引选项")
    void testReplaceIndexOption() {
        LanceIndexBuilder builder = LanceIndexBuilder.builder()
                .datasetPath(datasetPath)
                .columnName("embedding")
                .indexType(IndexType.IVF_PQ)
                .replace(true)
                .build();

        assertThat(builder).isNotNull();
    }
}

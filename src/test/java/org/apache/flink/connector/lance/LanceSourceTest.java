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
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * LanceSource 单元测试。
 */
class LanceSourceTest {

    @TempDir
    Path tempDir;

    private String datasetPath;
    private RowType rowType;

    @BeforeEach
    void setUp() {
        datasetPath = tempDir.resolve("test_dataset").toString();
        
        // 创建测试 RowType
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("id", new BigIntType()));
        fields.add(new RowType.RowField("content", new VarCharType()));
        fields.add(new RowType.RowField("embedding", new ArrayType(new FloatType())));
        rowType = new RowType(fields);
    }

    @Test
    @DisplayName("测试 LanceSource 配置构建")
    void testSourceConfiguration() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .readBatchSize(512)
                .readColumns(Arrays.asList("id", "content"))
                .readFilter("id > 10")
                .build();

        LanceSource source = new LanceSource(options, rowType);

        assertThat(source.getOptions().getPath()).isEqualTo(datasetPath);
        assertThat(source.getOptions().getReadBatchSize()).isEqualTo(512);
        assertThat(source.getOptions().getReadColumns()).containsExactly("id", "content");
        assertThat(source.getOptions().getReadFilter()).isEqualTo("id > 10");
        assertThat(source.getRowType()).isEqualTo(rowType);
    }

    @Test
    @DisplayName("测试 LanceSource Builder 模式")
    void testSourceBuilder() {
        LanceSource source = LanceSource.builder()
                .path(datasetPath)
                .batchSize(256)
                .columns(Arrays.asList("id"))
                .filter("id < 100")
                .rowType(rowType)
                .build();

        assertThat(source.getOptions().getPath()).isEqualTo(datasetPath);
        assertThat(source.getOptions().getReadBatchSize()).isEqualTo(256);
        assertThat(source.getSelectedColumns()).containsExactly("id");
    }

    @Test
    @DisplayName("测试 LanceSource Builder 缺少路径时抛出异常")
    void testSourceBuilderMissingPath() {
        assertThatThrownBy(() -> LanceSource.builder()
                .rowType(rowType)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("路径不能为空");
    }

    @Test
    @DisplayName("测试 LanceSplit 创建")
    void testLanceSplit() {
        LanceSplit split = new LanceSplit(0, 1, datasetPath, 1000);

        assertThat(split.getSplitNumber()).isEqualTo(0);
        assertThat(split.getFragmentId()).isEqualTo(1);
        assertThat(split.getDatasetPath()).isEqualTo(datasetPath);
        assertThat(split.getRowCount()).isEqualTo(1000);
    }

    @Test
    @DisplayName("测试 LanceSplit 相等性")
    void testLanceSplitEquality() {
        LanceSplit split1 = new LanceSplit(0, 1, datasetPath, 1000);
        LanceSplit split2 = new LanceSplit(0, 1, datasetPath, 1000);
        LanceSplit split3 = new LanceSplit(1, 2, datasetPath, 2000);

        assertThat(split1).isEqualTo(split2);
        assertThat(split1.hashCode()).isEqualTo(split2.hashCode());
        assertThat(split1).isNotEqualTo(split3);
    }

    @Test
    @DisplayName("测试 LanceInputFormat 配置")
    void testInputFormatConfiguration() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .readBatchSize(128)
                .build();

        LanceInputFormat inputFormat = new LanceInputFormat(options, rowType);

        assertThat(inputFormat.getOptions().getPath()).isEqualTo(datasetPath);
        assertThat(inputFormat.getOptions().getReadBatchSize()).isEqualTo(128);
        assertThat(inputFormat.getRowType()).isEqualTo(rowType);
    }

    @Test
    @DisplayName("测试默认配置值")
    void testDefaultConfiguration() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .build();

        // 验证默认值
        assertThat(options.getReadBatchSize()).isEqualTo(1024);
        assertThat(options.getReadColumns()).isEmpty();
        assertThat(options.getReadFilter()).isNull();
    }

    @Test
    @DisplayName("测试配置校验 - 无效的批次大小")
    void testInvalidBatchSize() {
        assertThatThrownBy(() -> LanceOptions.builder()
                .path(datasetPath)
                .readBatchSize(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch-size");
    }

    @Test
    @DisplayName("测试向量类型的 RowType")
    void testVectorRowType() {
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("id", new BigIntType()));
        fields.add(new RowType.RowField("embedding", new ArrayType(new FloatType())));
        RowType vectorRowType = new RowType(fields);

        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .build();

        LanceSource source = new LanceSource(options, vectorRowType);

        assertThat(source.getRowType().getFieldCount()).isEqualTo(2);
        assertThat(source.getRowType().getTypeAt(1)).isInstanceOf(ArrayType.class);
    }
}

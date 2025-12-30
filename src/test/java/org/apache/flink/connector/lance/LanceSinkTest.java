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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * LanceSink 单元测试。
 */
class LanceSinkTest {

    @TempDir
    Path tempDir;

    private String datasetPath;
    private RowType rowType;

    @BeforeEach
    void setUp() {
        datasetPath = tempDir.resolve("test_sink_dataset").toString();
        
        // 创建测试 RowType
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("id", new BigIntType()));
        fields.add(new RowType.RowField("content", new VarCharType()));
        fields.add(new RowType.RowField("embedding", new ArrayType(new FloatType())));
        rowType = new RowType(fields);
    }

    @Test
    @DisplayName("测试 LanceSink 配置构建")
    void testSinkConfiguration() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .writeBatchSize(512)
                .writeMode(LanceOptions.WriteMode.APPEND)
                .writeMaxRowsPerFile(500000)
                .build();

        LanceSink sink = new LanceSink(options, rowType);

        assertThat(sink.getOptions().getPath()).isEqualTo(datasetPath);
        assertThat(sink.getOptions().getWriteBatchSize()).isEqualTo(512);
        assertThat(sink.getOptions().getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);
        assertThat(sink.getOptions().getWriteMaxRowsPerFile()).isEqualTo(500000);
        assertThat(sink.getRowType()).isEqualTo(rowType);
    }

    @Test
    @DisplayName("测试 LanceSink Builder 模式")
    void testSinkBuilder() {
        LanceSink sink = LanceSink.builder()
                .path(datasetPath)
                .batchSize(256)
                .writeMode(LanceOptions.WriteMode.OVERWRITE)
                .maxRowsPerFile(100000)
                .rowType(rowType)
                .build();

        assertThat(sink.getOptions().getPath()).isEqualTo(datasetPath);
        assertThat(sink.getOptions().getWriteBatchSize()).isEqualTo(256);
        assertThat(sink.getOptions().getWriteMode()).isEqualTo(LanceOptions.WriteMode.OVERWRITE);
        assertThat(sink.getOptions().getWriteMaxRowsPerFile()).isEqualTo(100000);
    }

    @Test
    @DisplayName("测试 LanceSink Builder 缺少路径时抛出异常")
    void testSinkBuilderMissingPath() {
        assertThatThrownBy(() -> LanceSink.builder()
                .rowType(rowType)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("路径不能为空");
    }

    @Test
    @DisplayName("测试 LanceSink Builder 缺少 RowType 时抛出异常")
    void testSinkBuilderMissingRowType() {
        assertThatThrownBy(() -> LanceSink.builder()
                .path(datasetPath)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RowType");
    }

    @Test
    @DisplayName("测试默认 Sink 配置值")
    void testDefaultSinkConfiguration() {
        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .build();

        // 验证默认值
        assertThat(options.getWriteBatchSize()).isEqualTo(1024);
        assertThat(options.getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);
        assertThat(options.getWriteMaxRowsPerFile()).isEqualTo(1000000);
    }

    @Test
    @DisplayName("测试写入模式枚举")
    void testWriteMode() {
        assertThat(LanceOptions.WriteMode.fromValue("append"))
                .isEqualTo(LanceOptions.WriteMode.APPEND);
        assertThat(LanceOptions.WriteMode.fromValue("APPEND"))
                .isEqualTo(LanceOptions.WriteMode.APPEND);
        assertThat(LanceOptions.WriteMode.fromValue("overwrite"))
                .isEqualTo(LanceOptions.WriteMode.OVERWRITE);
        assertThat(LanceOptions.WriteMode.fromValue("OVERWRITE"))
                .isEqualTo(LanceOptions.WriteMode.OVERWRITE);
    }

    @Test
    @DisplayName("测试无效的写入模式")
    void testInvalidWriteMode() {
        assertThatThrownBy(() -> LanceOptions.WriteMode.fromValue("invalid"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("不支持的写入模式");
    }

    @Test
    @DisplayName("测试配置校验 - 无效的写入批次大小")
    void testInvalidWriteBatchSize() {
        assertThatThrownBy(() -> LanceOptions.builder()
                .path(datasetPath)
                .writeBatchSize(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batch-size");
    }

    @Test
    @DisplayName("测试配置校验 - 无效的每文件最大行数")
    void testInvalidMaxRowsPerFile() {
        assertThatThrownBy(() -> LanceOptions.builder()
                .path(datasetPath)
                .writeMaxRowsPerFile(-1)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-rows-per-file");
    }

    @Test
    @DisplayName("测试向量类型写入配置")
    void testVectorWriteConfiguration() {
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("id", new BigIntType()));
        fields.add(new RowType.RowField("embedding", new ArrayType(new FloatType())));
        RowType vectorRowType = new RowType(fields);

        LanceOptions options = LanceOptions.builder()
                .path(datasetPath)
                .writeBatchSize(100)
                .build();

        LanceSink sink = new LanceSink(options, vectorRowType);

        assertThat(sink.getRowType().getFieldCount()).isEqualTo(2);
        assertThat(sink.getRowType().getTypeAt(1)).isInstanceOf(ArrayType.class);
    }

    @Test
    @DisplayName("测试 APPEND 和 OVERWRITE 模式配置")
    void testWriteModeConfiguration() {
        // APPEND 模式
        LanceOptions appendOptions = LanceOptions.builder()
                .path(datasetPath)
                .writeMode(LanceOptions.WriteMode.APPEND)
                .build();
        assertThat(appendOptions.getWriteMode()).isEqualTo(LanceOptions.WriteMode.APPEND);
        assertThat(appendOptions.getWriteMode().getValue()).isEqualTo("append");

        // OVERWRITE 模式
        LanceOptions overwriteOptions = LanceOptions.builder()
                .path(datasetPath)
                .writeMode(LanceOptions.WriteMode.OVERWRITE)
                .build();
        assertThat(overwriteOptions.getWriteMode()).isEqualTo(LanceOptions.WriteMode.OVERWRITE);
        assertThat(overwriteOptions.getWriteMode().getValue()).isEqualTo("overwrite");
    }
}

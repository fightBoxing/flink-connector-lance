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

import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * LanceTypeConverter 单元测试。
 */
class LanceTypeConverterTest {

    @Test
    @DisplayName("测试 Arrow Int 类型到 Flink 类型的映射")
    void testArrowIntToFlinkType() {
        // Int8 -> TINYINT
        Field int8Field = new Field("int8", FieldType.nullable(new ArrowType.Int(8, true)), null);
        LogicalType int8Type = LanceTypeConverter.arrowTypeToFlinkType(int8Field);
        assertThat(int8Type).isInstanceOf(TinyIntType.class);

        // Int16 -> SMALLINT
        Field int16Field = new Field("int16", FieldType.nullable(new ArrowType.Int(16, true)), null);
        LogicalType int16Type = LanceTypeConverter.arrowTypeToFlinkType(int16Field);
        assertThat(int16Type).isInstanceOf(SmallIntType.class);

        // Int32 -> INT
        Field int32Field = new Field("int32", FieldType.nullable(new ArrowType.Int(32, true)), null);
        LogicalType int32Type = LanceTypeConverter.arrowTypeToFlinkType(int32Field);
        assertThat(int32Type).isInstanceOf(IntType.class);

        // Int64 -> BIGINT
        Field int64Field = new Field("int64", FieldType.nullable(new ArrowType.Int(64, true)), null);
        LogicalType int64Type = LanceTypeConverter.arrowTypeToFlinkType(int64Field);
        assertThat(int64Type).isInstanceOf(BigIntType.class);
    }

    @Test
    @DisplayName("测试 Arrow 浮点类型到 Flink 类型的映射")
    void testArrowFloatToFlinkType() {
        // Float32 -> FLOAT
        Field float32Field = new Field("float32", 
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
        LogicalType float32Type = LanceTypeConverter.arrowTypeToFlinkType(float32Field);
        assertThat(float32Type).isInstanceOf(FloatType.class);

        // Float64 -> DOUBLE
        Field float64Field = new Field("float64", 
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
        LogicalType float64Type = LanceTypeConverter.arrowTypeToFlinkType(float64Field);
        assertThat(float64Type).isInstanceOf(DoubleType.class);
    }

    @Test
    @DisplayName("测试 Arrow 字符串类型到 Flink 类型的映射")
    void testArrowStringToFlinkType() {
        // String -> STRING
        Field stringField = new Field("str", FieldType.nullable(ArrowType.Utf8.INSTANCE), null);
        LogicalType stringType = LanceTypeConverter.arrowTypeToFlinkType(stringField);
        assertThat(stringType).isInstanceOf(VarCharType.class);

        // LargeString -> STRING
        Field largeStringField = new Field("large_str", FieldType.nullable(ArrowType.LargeUtf8.INSTANCE), null);
        LogicalType largeStringType = LanceTypeConverter.arrowTypeToFlinkType(largeStringField);
        assertThat(largeStringType).isInstanceOf(VarCharType.class);
    }

    @Test
    @DisplayName("测试 Arrow Boolean 类型到 Flink 类型的映射")
    void testArrowBoolToFlinkType() {
        Field boolField = new Field("bool", FieldType.nullable(ArrowType.Bool.INSTANCE), null);
        LogicalType boolType = LanceTypeConverter.arrowTypeToFlinkType(boolField);
        assertThat(boolType).isInstanceOf(BooleanType.class);
    }

    @Test
    @DisplayName("测试 Arrow Binary 类型到 Flink 类型的映射")
    void testArrowBinaryToFlinkType() {
        Field binaryField = new Field("binary", FieldType.nullable(ArrowType.Binary.INSTANCE), null);
        LogicalType binaryType = LanceTypeConverter.arrowTypeToFlinkType(binaryField);
        assertThat(binaryType).isInstanceOf(VarBinaryType.class);
    }

    @Test
    @DisplayName("测试 Arrow Date 类型到 Flink 类型的映射")
    void testArrowDateToFlinkType() {
        Field dateField = new Field("date", 
                FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null);
        LogicalType dateType = LanceTypeConverter.arrowTypeToFlinkType(dateField);
        assertThat(dateType).isInstanceOf(DateType.class);
    }

    @Test
    @DisplayName("测试 Arrow Timestamp 类型到 Flink 类型的映射")
    void testArrowTimestampToFlinkType() {
        // 毫秒精度
        Field tsMilliField = new Field("ts_milli", 
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null);
        LogicalType tsMilliType = LanceTypeConverter.arrowTypeToFlinkType(tsMilliField);
        assertThat(tsMilliType).isInstanceOf(TimestampType.class);
        assertThat(((TimestampType) tsMilliType).getPrecision()).isEqualTo(3);

        // 微秒精度
        Field tsMicroField = new Field("ts_micro", 
                FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), null);
        LogicalType tsMicroType = LanceTypeConverter.arrowTypeToFlinkType(tsMicroField);
        assertThat(tsMicroType).isInstanceOf(TimestampType.class);
        assertThat(((TimestampType) tsMicroType).getPrecision()).isEqualTo(6);
    }

    @Test
    @DisplayName("测试 Arrow FixedSizeList (向量) 类型到 Flink 类型的映射")
    void testArrowVectorToFlinkType() {
        // FixedSizeList<Float32> -> ARRAY<FLOAT>
        ArrowType elementType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        Field elementField = new Field("item", FieldType.notNullable(elementType), null);
        List<Field> children = Arrays.asList(elementField);
        
        Field vectorField = new Field("embedding", 
                FieldType.nullable(new ArrowType.FixedSizeList(128)), children);
        
        LogicalType vectorType = LanceTypeConverter.arrowTypeToFlinkType(vectorField);
        assertThat(vectorType).isInstanceOf(ArrayType.class);
        assertThat(((ArrayType) vectorType).getElementType()).isInstanceOf(FloatType.class);
    }

    @Test
    @DisplayName("测试 Flink 类型到 Arrow 类型的映射")
    void testFlinkTypeToArrowType() {
        // TINYINT -> Int8
        Field tinyIntField = LanceTypeConverter.flinkTypeToArrowField("tinyint", new TinyIntType());
        assertThat(tinyIntField.getType()).isInstanceOf(ArrowType.Int.class);
        assertThat(((ArrowType.Int) tinyIntField.getType()).getBitWidth()).isEqualTo(8);

        // INT -> Int32
        Field intField = LanceTypeConverter.flinkTypeToArrowField("int", new IntType());
        assertThat(intField.getType()).isInstanceOf(ArrowType.Int.class);
        assertThat(((ArrowType.Int) intField.getType()).getBitWidth()).isEqualTo(32);

        // BIGINT -> Int64
        Field bigIntField = LanceTypeConverter.flinkTypeToArrowField("bigint", new BigIntType());
        assertThat(bigIntField.getType()).isInstanceOf(ArrowType.Int.class);
        assertThat(((ArrowType.Int) bigIntField.getType()).getBitWidth()).isEqualTo(64);

        // FLOAT -> Float32
        Field floatField = LanceTypeConverter.flinkTypeToArrowField("float", new FloatType());
        assertThat(floatField.getType()).isInstanceOf(ArrowType.FloatingPoint.class);
        assertThat(((ArrowType.FloatingPoint) floatField.getType()).getPrecision())
                .isEqualTo(FloatingPointPrecision.SINGLE);

        // DOUBLE -> Float64
        Field doubleField = LanceTypeConverter.flinkTypeToArrowField("double", new DoubleType());
        assertThat(doubleField.getType()).isInstanceOf(ArrowType.FloatingPoint.class);
        assertThat(((ArrowType.FloatingPoint) doubleField.getType()).getPrecision())
                .isEqualTo(FloatingPointPrecision.DOUBLE);

        // STRING -> Utf8
        Field stringField = LanceTypeConverter.flinkTypeToArrowField("string", new VarCharType());
        assertThat(stringField.getType()).isInstanceOf(ArrowType.Utf8.class);

        // BOOLEAN -> Bool
        Field boolField = LanceTypeConverter.flinkTypeToArrowField("bool", new BooleanType());
        assertThat(boolField.getType()).isInstanceOf(ArrowType.Bool.class);
    }

    @Test
    @DisplayName("测试 Arrow Schema 到 Flink RowType 的转换")
    void testArrowSchemaToFlinkRowType() {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null));
        fields.add(new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null));
        fields.add(new Field("score", 
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
        
        Schema arrowSchema = new Schema(fields);
        RowType rowType = LanceTypeConverter.toFlinkRowType(arrowSchema);

        assertThat(rowType.getFieldCount()).isEqualTo(3);
        assertThat(rowType.getFieldNames()).containsExactly("id", "name", "score");
        assertThat(rowType.getTypeAt(0)).isInstanceOf(BigIntType.class);
        assertThat(rowType.getTypeAt(1)).isInstanceOf(VarCharType.class);
        assertThat(rowType.getTypeAt(2)).isInstanceOf(DoubleType.class);
    }

    @Test
    @DisplayName("测试 Flink RowType 到 Arrow Schema 的转换")
    void testFlinkRowTypeToArrowSchema() {
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("id", new BigIntType(false)));
        fields.add(new RowType.RowField("content", new VarCharType()));
        fields.add(new RowType.RowField("embedding", new ArrayType(new FloatType())));
        
        RowType rowType = new RowType(fields);
        Schema arrowSchema = LanceTypeConverter.toArrowSchema(rowType);

        assertThat(arrowSchema.getFields()).hasSize(3);
        assertThat(arrowSchema.getFields().get(0).getName()).isEqualTo("id");
        assertThat(arrowSchema.getFields().get(1).getName()).isEqualTo("content");
        assertThat(arrowSchema.getFields().get(2).getName()).isEqualTo("embedding");
    }

    @Test
    @DisplayName("测试向量字段创建")
    void testCreateVectorField() {
        // Float32 向量
        Field float32Vector = LanceTypeConverter.createVectorField("embedding", 128, false);
        assertThat(float32Vector.getName()).isEqualTo("embedding");
        assertThat(float32Vector.getType()).isInstanceOf(ArrowType.FixedSizeList.class);
        assertThat(((ArrowType.FixedSizeList) float32Vector.getType()).getListSize()).isEqualTo(128);
        assertThat(float32Vector.isNullable()).isFalse();

        // Float64 向量
        Field float64Vector = LanceTypeConverter.createFloat64VectorField("embedding64", 256, true);
        assertThat(float64Vector.getName()).isEqualTo("embedding64");
        assertThat(((ArrowType.FixedSizeList) float64Vector.getType()).getListSize()).isEqualTo(256);
        assertThat(float64Vector.isNullable()).isTrue();
    }

    @Test
    @DisplayName("测试向量字段检测")
    void testIsVectorField() {
        // 创建向量字段
        Field vectorField = LanceTypeConverter.createVectorField("embedding", 128, false);
        assertThat(LanceTypeConverter.isVectorField(vectorField)).isTrue();
        assertThat(LanceTypeConverter.getVectorDimension(vectorField)).isEqualTo(128);

        // 非向量字段
        Field intField = new Field("id", FieldType.notNullable(new ArrowType.Int(64, true)), null);
        assertThat(LanceTypeConverter.isVectorField(intField)).isFalse();
        assertThat(LanceTypeConverter.getVectorDimension(intField)).isEqualTo(-1);
    }

    @Test
    @DisplayName("测试不支持的类型异常")
    void testUnsupportedTypeException() {
        // 不支持的 Arrow 类型
        Field unsupportedField = new Field("unsupported", 
                FieldType.nullable(new ArrowType.Duration(TimeUnit.SECOND)), null);
        
        assertThatThrownBy(() -> LanceTypeConverter.arrowTypeToFlinkType(unsupportedField))
                .isInstanceOf(LanceTypeConverter.UnsupportedTypeException.class)
                .hasMessageContaining("不支持的 Arrow 类型");
    }

    @Test
    @DisplayName("测试双向转换的一致性")
    void testRoundTripConversion() {
        // 创建 Flink RowType
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("id", new BigIntType(false)));
        fields.add(new RowType.RowField("name", new VarCharType()));
        fields.add(new RowType.RowField("score", new DoubleType()));
        fields.add(new RowType.RowField("active", new BooleanType()));
        
        RowType originalRowType = new RowType(fields);

        // Flink -> Arrow -> Flink
        Schema arrowSchema = LanceTypeConverter.toArrowSchema(originalRowType);
        RowType convertedRowType = LanceTypeConverter.toFlinkRowType(arrowSchema);

        // 验证字段数量和名称
        assertThat(convertedRowType.getFieldCount()).isEqualTo(originalRowType.getFieldCount());
        assertThat(convertedRowType.getFieldNames()).isEqualTo(originalRowType.getFieldNames());

        // 验证类型（类型类应该匹配）
        for (int i = 0; i < originalRowType.getFieldCount(); i++) {
            assertThat(convertedRowType.getTypeAt(i).getClass())
                    .isEqualTo(originalRowType.getTypeAt(i).getClass());
        }
    }
}

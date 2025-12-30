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

package org.apache.flink.connector.lance.converter;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * RowData 与 Arrow 数据之间的转换器。
 * 
 * <p>负责 Arrow VectorSchemaRoot 与 Flink RowData 之间的双向转换。
 */
public class RowDataConverter implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RowDataConverter.class);

    private final RowType rowType;
    private final String[] fieldNames;
    private final LogicalType[] fieldTypes;

    public RowDataConverter(RowType rowType) {
        this.rowType = rowType;
        this.fieldNames = rowType.getFieldNames().toArray(new String[0]);
        this.fieldTypes = rowType.getFields().stream()
                .map(RowType.RowField::getType)
                .toArray(LogicalType[]::new);
    }

    /**
     * 将 Arrow VectorSchemaRoot 转换为 RowData 列表
     *
     * @param root Arrow VectorSchemaRoot
     * @return RowData 列表
     */
    public List<RowData> toRowDataList(VectorSchemaRoot root) {
        List<RowData> rows = new ArrayList<>();
        int rowCount = root.getRowCount();
        
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            GenericRowData rowData = new GenericRowData(fieldTypes.length);
            
            for (int fieldIndex = 0; fieldIndex < fieldTypes.length; fieldIndex++) {
                String fieldName = fieldNames[fieldIndex];
                FieldVector vector = root.getVector(fieldName);
                
                if (vector == null) {
                    rowData.setField(fieldIndex, null);
                    continue;
                }
                
                Object value = readValue(vector, rowIndex, fieldTypes[fieldIndex]);
                rowData.setField(fieldIndex, value);
            }
            
            rows.add(rowData);
        }
        
        return rows;
    }

    /**
     * 将 RowData 列表写入 Arrow VectorSchemaRoot
     *
     * @param rows RowData 列表
     * @param root Arrow VectorSchemaRoot
     */
    public void toVectorSchemaRoot(List<RowData> rows, VectorSchemaRoot root) {
        root.allocateNew();
        
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            RowData rowData = rows.get(rowIndex);
            
            for (int fieldIndex = 0; fieldIndex < fieldTypes.length; fieldIndex++) {
                String fieldName = fieldNames[fieldIndex];
                FieldVector vector = root.getVector(fieldName);
                
                if (vector == null) {
                    continue;
                }
                
                Object value = getFieldValue(rowData, fieldIndex, fieldTypes[fieldIndex]);
                writeValue(vector, rowIndex, value, fieldTypes[fieldIndex]);
            }
        }
        
        root.setRowCount(rows.size());
    }

    /**
     * 创建 VectorSchemaRoot
     *
     * @param allocator 内存分配器
     * @return VectorSchemaRoot
     */
    public VectorSchemaRoot createVectorSchemaRoot(BufferAllocator allocator) {
        Schema arrowSchema = LanceTypeConverter.toArrowSchema(rowType);
        return VectorSchemaRoot.create(arrowSchema, allocator);
    }

    /**
     * 从 Arrow Vector 中读取值
     */
    private Object readValue(FieldVector vector, int index, LogicalType logicalType) {
        if (vector.isNull(index)) {
            return null;
        }

        if (logicalType instanceof TinyIntType) {
            return ((TinyIntVector) vector).get(index);
        } else if (logicalType instanceof SmallIntType) {
            return ((SmallIntVector) vector).get(index);
        } else if (logicalType instanceof IntType) {
            return ((IntVector) vector).get(index);
        } else if (logicalType instanceof BigIntType) {
            return ((BigIntVector) vector).get(index);
        } else if (logicalType instanceof FloatType) {
            return ((Float4Vector) vector).get(index);
        } else if (logicalType instanceof DoubleType) {
            return ((Float8Vector) vector).get(index);
        } else if (logicalType instanceof VarCharType) {
            byte[] bytes = ((VarCharVector) vector).get(index);
            return StringData.fromBytes(bytes);
        } else if (logicalType instanceof BooleanType) {
            return ((BitVector) vector).get(index) == 1;
        } else if (logicalType instanceof VarBinaryType) {
            return ((VarBinaryVector) vector).get(index);
        } else if (logicalType instanceof BinaryType) {
            return ((FixedSizeBinaryVector) vector).get(index);
        } else if (logicalType instanceof DateType) {
            int daysSinceEpoch = ((DateDayVector) vector).get(index);
            return daysSinceEpoch;
        } else if (logicalType instanceof TimestampType) {
            return readTimestamp(vector, index, (TimestampType) logicalType);
        } else if (logicalType instanceof ArrayType) {
            return readArray(vector, index, (ArrayType) logicalType);
        } else if (logicalType instanceof RowType) {
            return readStruct(vector, index, (RowType) logicalType);
        }

        throw new LanceTypeConverter.UnsupportedTypeException(
                "不支持读取类型: " + logicalType.getClass().getSimpleName());
    }

    /**
     * 读取时间戳值
     */
    private TimestampData readTimestamp(FieldVector vector, int index, TimestampType tsType) {
        long value;
        int precision = tsType.getPrecision();

        if (vector instanceof TimeStampSecVector) {
            value = ((TimeStampSecVector) vector).get(index);
            return TimestampData.fromEpochMillis(value * 1000);
        } else if (vector instanceof TimeStampMilliVector) {
            value = ((TimeStampMilliVector) vector).get(index);
            return TimestampData.fromEpochMillis(value);
        } else if (vector instanceof TimeStampMicroVector) {
            value = ((TimeStampMicroVector) vector).get(index);
            return TimestampData.fromEpochMillis(value / 1000, (int) ((value % 1000) * 1000));
        } else if (vector instanceof TimeStampNanoVector) {
            value = ((TimeStampNanoVector) vector).get(index);
            return TimestampData.fromEpochMillis(value / 1000000, (int) (value % 1000000));
        }

        throw new LanceTypeConverter.UnsupportedTypeException(
                "不支持的时间戳 Vector 类型: " + vector.getClass().getSimpleName());
    }

    /**
     * 读取数组值
     */
    private ArrayData readArray(FieldVector vector, int index, ArrayType arrayType) {
        LogicalType elementType = arrayType.getElementType();
        
        if (vector instanceof FixedSizeListVector) {
            FixedSizeListVector listVector = (FixedSizeListVector) vector;
            int listSize = listVector.getListSize();
            FieldVector dataVector = listVector.getDataVector();
            int startIndex = index * listSize;
            
            return readArrayData(dataVector, startIndex, listSize, elementType);
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            int startIndex = listVector.getElementStartIndex(index);
            int endIndex = listVector.getElementEndIndex(index);
            int listSize = endIndex - startIndex;
            FieldVector dataVector = listVector.getDataVector();
            
            return readArrayData(dataVector, startIndex, listSize, elementType);
        }

        throw new LanceTypeConverter.UnsupportedTypeException(
                "不支持的数组 Vector 类型: " + vector.getClass().getSimpleName());
    }

    /**
     * 读取数组数据
     */
    private ArrayData readArrayData(FieldVector dataVector, int startIndex, int size, LogicalType elementType) {
        if (elementType instanceof FloatType) {
            Float4Vector float4Vector = (Float4Vector) dataVector;
            Float[] values = new Float[size];
            for (int i = 0; i < size; i++) {
                if (float4Vector.isNull(startIndex + i)) {
                    values[i] = null;
                } else {
                    values[i] = float4Vector.get(startIndex + i);
                }
            }
            return new GenericArrayData(values);
        } else if (elementType instanceof DoubleType) {
            Double8Vector double8Vector = (Double8Vector) dataVector;
            Double[] values = new Double[size];
            for (int i = 0; i < size; i++) {
                if (double8Vector.isNull(startIndex + i)) {
                    values[i] = null;
                } else {
                    values[i] = double8Vector.get(startIndex + i);
                }
            }
            return new GenericArrayData(values);
        } else if (elementType instanceof IntType) {
            IntVector intVector = (IntVector) dataVector;
            Integer[] values = new Integer[size];
            for (int i = 0; i < size; i++) {
                if (intVector.isNull(startIndex + i)) {
                    values[i] = null;
                } else {
                    values[i] = intVector.get(startIndex + i);
                }
            }
            return new GenericArrayData(values);
        } else if (elementType instanceof BigIntType) {
            BigIntVector bigIntVector = (BigIntVector) dataVector;
            Long[] values = new Long[size];
            for (int i = 0; i < size; i++) {
                if (bigIntVector.isNull(startIndex + i)) {
                    values[i] = null;
                } else {
                    values[i] = bigIntVector.get(startIndex + i);
                }
            }
            return new GenericArrayData(values);
        } else if (elementType instanceof VarCharType) {
            VarCharVector varCharVector = (VarCharVector) dataVector;
            StringData[] values = new StringData[size];
            for (int i = 0; i < size; i++) {
                if (varCharVector.isNull(startIndex + i)) {
                    values[i] = null;
                } else {
                    values[i] = StringData.fromBytes(varCharVector.get(startIndex + i));
                }
            }
            return new GenericArrayData(values);
        }

        throw new LanceTypeConverter.UnsupportedTypeException(
                "不支持的数组元素类型: " + elementType.getClass().getSimpleName());
    }

    /**
     * 内部类，用于处理 Double 类型的 Vector（Float8Vector 的别名）
     */
    private static class Double8Vector {
        private final Float8Vector vector;

        Double8Vector(FieldVector vector) {
            this.vector = (Float8Vector) vector;
        }

        boolean isNull(int index) {
            return vector.isNull(index);
        }

        double get(int index) {
            return vector.get(index);
        }
    }

    /**
     * 读取结构体值
     */
    private RowData readStruct(FieldVector vector, int index, RowType rowType) {
        StructVector structVector = (StructVector) vector;
        List<RowType.RowField> fields = rowType.getFields();
        GenericRowData rowData = new GenericRowData(fields.size());

        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            FieldVector childVector = structVector.getChild(field.getName());
            if (childVector == null) {
                rowData.setField(i, null);
            } else {
                Object value = readValue(childVector, index, field.getType());
                rowData.setField(i, value);
            }
        }

        return rowData;
    }

    /**
     * 从 RowData 中获取字段值
     */
    private Object getFieldValue(RowData rowData, int index, LogicalType logicalType) {
        if (rowData.isNullAt(index)) {
            return null;
        }

        if (logicalType instanceof TinyIntType) {
            return rowData.getByte(index);
        } else if (logicalType instanceof SmallIntType) {
            return rowData.getShort(index);
        } else if (logicalType instanceof IntType) {
            return rowData.getInt(index);
        } else if (logicalType instanceof BigIntType) {
            return rowData.getLong(index);
        } else if (logicalType instanceof FloatType) {
            return rowData.getFloat(index);
        } else if (logicalType instanceof DoubleType) {
            return rowData.getDouble(index);
        } else if (logicalType instanceof VarCharType) {
            return rowData.getString(index);
        } else if (logicalType instanceof BooleanType) {
            return rowData.getBoolean(index);
        } else if (logicalType instanceof VarBinaryType || logicalType instanceof BinaryType) {
            return rowData.getBinary(index);
        } else if (logicalType instanceof DateType) {
            return rowData.getInt(index);
        } else if (logicalType instanceof TimestampType) {
            TimestampType tsType = (TimestampType) logicalType;
            return rowData.getTimestamp(index, tsType.getPrecision());
        } else if (logicalType instanceof ArrayType) {
            return rowData.getArray(index);
        } else if (logicalType instanceof RowType) {
            RowType nestedRowType = (RowType) logicalType;
            return rowData.getRow(index, nestedRowType.getFieldCount());
        }

        throw new LanceTypeConverter.UnsupportedTypeException(
                "不支持获取类型: " + logicalType.getClass().getSimpleName());
    }

    /**
     * 将值写入 Arrow Vector
     */
    private void writeValue(FieldVector vector, int index, Object value, LogicalType logicalType) {
        if (value == null) {
            setNull(vector, index);
            return;
        }

        if (logicalType instanceof TinyIntType) {
            ((TinyIntVector) vector).setSafe(index, (byte) value);
        } else if (logicalType instanceof SmallIntType) {
            ((SmallIntVector) vector).setSafe(index, (short) value);
        } else if (logicalType instanceof IntType) {
            ((IntVector) vector).setSafe(index, (int) value);
        } else if (logicalType instanceof BigIntType) {
            ((BigIntVector) vector).setSafe(index, (long) value);
        } else if (logicalType instanceof FloatType) {
            ((Float4Vector) vector).setSafe(index, (float) value);
        } else if (logicalType instanceof DoubleType) {
            ((Float8Vector) vector).setSafe(index, (double) value);
        } else if (logicalType instanceof VarCharType) {
            StringData stringData = (StringData) value;
            ((VarCharVector) vector).setSafe(index, stringData.toBytes());
        } else if (logicalType instanceof BooleanType) {
            ((BitVector) vector).setSafe(index, (boolean) value ? 1 : 0);
        } else if (logicalType instanceof VarBinaryType) {
            ((VarBinaryVector) vector).setSafe(index, (byte[]) value);
        } else if (logicalType instanceof BinaryType) {
            ((FixedSizeBinaryVector) vector).setSafe(index, (byte[]) value);
        } else if (logicalType instanceof DateType) {
            ((DateDayVector) vector).setSafe(index, (int) value);
        } else if (logicalType instanceof TimestampType) {
            writeTimestamp(vector, index, (TimestampData) value, (TimestampType) logicalType);
        } else if (logicalType instanceof ArrayType) {
            writeArray(vector, index, (ArrayData) value, (ArrayType) logicalType);
        } else if (logicalType instanceof RowType) {
            writeStruct(vector, index, (RowData) value, (RowType) logicalType);
        } else {
            throw new LanceTypeConverter.UnsupportedTypeException(
                    "不支持写入类型: " + logicalType.getClass().getSimpleName());
        }
    }

    /**
     * 设置 null 值
     */
    private void setNull(FieldVector vector, int index) {
        if (vector instanceof TinyIntVector) {
            ((TinyIntVector) vector).setNull(index);
        } else if (vector instanceof SmallIntVector) {
            ((SmallIntVector) vector).setNull(index);
        } else if (vector instanceof IntVector) {
            ((IntVector) vector).setNull(index);
        } else if (vector instanceof BigIntVector) {
            ((BigIntVector) vector).setNull(index);
        } else if (vector instanceof Float4Vector) {
            ((Float4Vector) vector).setNull(index);
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setNull(index);
        } else if (vector instanceof VarCharVector) {
            ((VarCharVector) vector).setNull(index);
        } else if (vector instanceof BitVector) {
            ((BitVector) vector).setNull(index);
        } else if (vector instanceof VarBinaryVector) {
            ((VarBinaryVector) vector).setNull(index);
        } else if (vector instanceof FixedSizeBinaryVector) {
            ((FixedSizeBinaryVector) vector).setNull(index);
        } else if (vector instanceof DateDayVector) {
            ((DateDayVector) vector).setNull(index);
        } else if (vector instanceof TimeStampSecVector) {
            ((TimeStampSecVector) vector).setNull(index);
        } else if (vector instanceof TimeStampMilliVector) {
            ((TimeStampMilliVector) vector).setNull(index);
        } else if (vector instanceof TimeStampMicroVector) {
            ((TimeStampMicroVector) vector).setNull(index);
        } else if (vector instanceof TimeStampNanoVector) {
            ((TimeStampNanoVector) vector).setNull(index);
        } else if (vector instanceof FixedSizeListVector) {
            ((FixedSizeListVector) vector).setNull(index);
        } else if (vector instanceof ListVector) {
            ((ListVector) vector).setNull(index);
        } else if (vector instanceof StructVector) {
            ((StructVector) vector).setNull(index);
        }
    }

    /**
     * 写入时间戳值
     */
    private void writeTimestamp(FieldVector vector, int index, TimestampData tsData, TimestampType tsType) {
        long millis = tsData.getMillisecond();
        int nanos = tsData.getNanoOfMillisecond();

        if (vector instanceof TimeStampSecVector) {
            ((TimeStampSecVector) vector).setSafe(index, millis / 1000);
        } else if (vector instanceof TimeStampMilliVector) {
            ((TimeStampMilliVector) vector).setSafe(index, millis);
        } else if (vector instanceof TimeStampMicroVector) {
            long micros = millis * 1000 + nanos / 1000;
            ((TimeStampMicroVector) vector).setSafe(index, micros);
        } else if (vector instanceof TimeStampNanoVector) {
            long totalNanos = millis * 1000000 + nanos;
            ((TimeStampNanoVector) vector).setSafe(index, totalNanos);
        } else {
            throw new LanceTypeConverter.UnsupportedTypeException(
                    "不支持的时间戳 Vector 类型: " + vector.getClass().getSimpleName());
        }
    }

    /**
     * 写入数组值
     */
    private void writeArray(FieldVector vector, int index, ArrayData arrayData, ArrayType arrayType) {
        LogicalType elementType = arrayType.getElementType();
        int size = arrayData.size();

        if (vector instanceof FixedSizeListVector) {
            FixedSizeListVector listVector = (FixedSizeListVector) vector;
            int listSize = listVector.getListSize();
            
            if (size != listSize) {
                throw new IllegalArgumentException(
                        "数组大小 " + size + " 与 FixedSizeList 大小 " + listSize + " 不匹配");
            }
            
            FieldVector dataVector = listVector.getDataVector();
            int startIndex = index * listSize;
            
            writeArrayData(dataVector, startIndex, arrayData, elementType);
            listVector.setNotNull(index);
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            listVector.startNewValue(index);
            
            FieldVector dataVector = listVector.getDataVector();
            int startIndex = listVector.getElementStartIndex(index);
            
            writeArrayData(dataVector, startIndex, arrayData, elementType);
            listVector.endValue(index, size);
        } else {
            throw new LanceTypeConverter.UnsupportedTypeException(
                    "不支持的数组 Vector 类型: " + vector.getClass().getSimpleName());
        }
    }

    /**
     * 写入数组数据
     */
    private void writeArrayData(FieldVector dataVector, int startIndex, ArrayData arrayData, LogicalType elementType) {
        int size = arrayData.size();

        if (elementType instanceof FloatType) {
            Float4Vector float4Vector = (Float4Vector) dataVector;
            for (int i = 0; i < size; i++) {
                if (arrayData.isNullAt(i)) {
                    float4Vector.setNull(startIndex + i);
                } else {
                    float4Vector.setSafe(startIndex + i, arrayData.getFloat(i));
                }
            }
        } else if (elementType instanceof DoubleType) {
            Float8Vector float8Vector = (Float8Vector) dataVector;
            for (int i = 0; i < size; i++) {
                if (arrayData.isNullAt(i)) {
                    float8Vector.setNull(startIndex + i);
                } else {
                    float8Vector.setSafe(startIndex + i, arrayData.getDouble(i));
                }
            }
        } else if (elementType instanceof IntType) {
            IntVector intVector = (IntVector) dataVector;
            for (int i = 0; i < size; i++) {
                if (arrayData.isNullAt(i)) {
                    intVector.setNull(startIndex + i);
                } else {
                    intVector.setSafe(startIndex + i, arrayData.getInt(i));
                }
            }
        } else if (elementType instanceof BigIntType) {
            BigIntVector bigIntVector = (BigIntVector) dataVector;
            for (int i = 0; i < size; i++) {
                if (arrayData.isNullAt(i)) {
                    bigIntVector.setNull(startIndex + i);
                } else {
                    bigIntVector.setSafe(startIndex + i, arrayData.getLong(i));
                }
            }
        } else if (elementType instanceof VarCharType) {
            VarCharVector varCharVector = (VarCharVector) dataVector;
            for (int i = 0; i < size; i++) {
                if (arrayData.isNullAt(i)) {
                    varCharVector.setNull(startIndex + i);
                } else {
                    StringData stringData = arrayData.getString(i);
                    varCharVector.setSafe(startIndex + i, stringData.toBytes());
                }
            }
        } else {
            throw new LanceTypeConverter.UnsupportedTypeException(
                    "不支持的数组元素类型: " + elementType.getClass().getSimpleName());
        }
    }

    /**
     * 写入结构体值
     */
    private void writeStruct(FieldVector vector, int index, RowData rowData, RowType rowType) {
        StructVector structVector = (StructVector) vector;
        List<RowType.RowField> fields = rowType.getFields();

        for (int i = 0; i < fields.size(); i++) {
            RowType.RowField field = fields.get(i);
            FieldVector childVector = structVector.getChild(field.getName());
            if (childVector != null) {
                Object value = getFieldValue(rowData, i, field.getType());
                writeValue(childVector, index, value, field.getType());
            }
        }

        structVector.setIndexDefined(index);
    }

    /**
     * 将 float 数组转换为 ArrayData
     *
     * @param vector float 数组
     * @return ArrayData
     */
    public static ArrayData toArrayData(float[] vector) {
        if (vector == null) {
            return null;
        }
        Float[] boxed = new Float[vector.length];
        for (int i = 0; i < vector.length; i++) {
            boxed[i] = vector[i];
        }
        return new GenericArrayData(boxed);
    }

    /**
     * 将 double 数组转换为 ArrayData
     *
     * @param vector double 数组
     * @return ArrayData
     */
    public static ArrayData toArrayData(double[] vector) {
        if (vector == null) {
            return null;
        }
        Double[] boxed = new Double[vector.length];
        for (int i = 0; i < vector.length; i++) {
            boxed[i] = vector[i];
        }
        return new GenericArrayData(boxed);
    }

    /**
     * 将 ArrayData 转换为 float 数组
     *
     * @param arrayData ArrayData
     * @return float 数组
     */
    public static float[] toFloatArray(ArrayData arrayData) {
        if (arrayData == null) {
            return null;
        }
        int size = arrayData.size();
        float[] result = new float[size];
        for (int i = 0; i < size; i++) {
            result[i] = arrayData.getFloat(i);
        }
        return result;
    }

    /**
     * 将 ArrayData 转换为 double 数组
     *
     * @param arrayData ArrayData
     * @return double 数组
     */
    public static double[] toDoubleArray(ArrayData arrayData) {
        if (arrayData == null) {
            return null;
        }
        int size = arrayData.size();
        double[] result = new double[size];
        for (int i = 0; i < size; i++) {
            result[i] = arrayData.getDouble(i);
        }
        return result;
    }

    /**
     * 获取 RowType
     */
    public RowType getRowType() {
        return rowType;
    }

    /**
     * 获取字段名数组
     */
    public String[] getFieldNames() {
        return fieldNames;
    }

    /**
     * 获取字段类型数组
     */
    public LogicalType[] getFieldTypes() {
        return fieldTypes;
    }
}

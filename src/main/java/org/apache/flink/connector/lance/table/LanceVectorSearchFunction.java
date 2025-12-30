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

import org.apache.flink.connector.lance.LanceVectorSearch;
import org.apache.flink.connector.lance.config.LanceOptions;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Lance 向量检索 UDF。
 * 
 * <p>实现 TableFunction，支持在 SQL 中执行向量检索。
 * 
 * <p>使用示例：
 * <pre>{@code
 * -- 注册 UDF
 * CREATE TEMPORARY FUNCTION vector_search AS 
 *     'org.apache.flink.connector.lance.table.LanceVectorSearchFunction'
 *     LANGUAGE JAVA USING JAR '/path/to/flink-connector-lance.jar';
 * 
 * -- 使用 UDF 进行向量检索
 * SELECT * FROM TABLE(
 *     vector_search('/path/to/dataset', 'embedding', ARRAY[0.1, 0.2, 0.3], 10, 'L2')
 * );
 * }</pre>
 */
@FunctionHint(
    output = @DataTypeHint("ROW<id BIGINT, content STRING, embedding ARRAY<FLOAT>, _distance DOUBLE>")
)
public class LanceVectorSearchFunction extends TableFunction<Row> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(LanceVectorSearchFunction.class);

    private transient LanceVectorSearch vectorSearch;
    private String currentDatasetPath;
    private String currentColumnName;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        LOG.info("打开 LanceVectorSearchFunction");
    }

    @Override
    public void close() throws Exception {
        LOG.info("关闭 LanceVectorSearchFunction");
        
        if (vectorSearch != null) {
            try {
                vectorSearch.close();
            } catch (Exception e) {
                LOG.warn("关闭向量检索器失败", e);
            }
            vectorSearch = null;
        }
        
        super.close();
    }

    /**
     * 执行向量检索
     *
     * @param datasetPath 数据集路径
     * @param columnName 向量列名
     * @param queryVector 查询向量
     * @param k 返回的最近邻数量
     * @param metric 距离度量类型：L2, Cosine, Dot
     */
    public void eval(String datasetPath, String columnName, Float[] queryVector, Integer k, String metric) {
        try {
            // 检查是否需要重新初始化向量检索器
            if (vectorSearch == null || 
                !datasetPath.equals(currentDatasetPath) || 
                !columnName.equals(currentColumnName)) {
                
                if (vectorSearch != null) {
                    vectorSearch.close();
                }
                
                LanceOptions.MetricType metricType = LanceOptions.MetricType.fromValue(
                        metric != null ? metric : "L2"
                );
                
                vectorSearch = LanceVectorSearch.builder()
                        .datasetPath(datasetPath)
                        .columnName(columnName)
                        .metricType(metricType)
                        .build();
                
                vectorSearch.open();
                
                currentDatasetPath = datasetPath;
                currentColumnName = columnName;
            }
            
            // 转换查询向量
            float[] query = new float[queryVector.length];
            for (int i = 0; i < queryVector.length; i++) {
                query[i] = queryVector[i] != null ? queryVector[i] : 0.0f;
            }
            
            // 执行检索
            int topK = k != null ? k : 10;
            List<LanceVectorSearch.SearchResult> results = vectorSearch.search(query, topK);
            
            // 输出结果
            for (LanceVectorSearch.SearchResult result : results) {
                RowData rowData = result.getRowData();
                double distance = result.getDistance();
                
                // 构建输出 Row
                Row outputRow = convertToRow(rowData, distance);
                if (outputRow != null) {
                    collect(outputRow);
                }
            }
            
        } catch (Exception e) {
            LOG.error("向量检索失败", e);
            throw new RuntimeException("向量检索失败: " + e.getMessage(), e);
        }
    }

    /**
     * 简化的向量检索（使用默认参数）
     *
     * @param datasetPath 数据集路径
     * @param columnName 向量列名
     * @param queryVector 查询向量
     * @param k 返回的最近邻数量
     */
    public void eval(String datasetPath, String columnName, Float[] queryVector, Integer k) {
        eval(datasetPath, columnName, queryVector, k, "L2");
    }

    /**
     * 最简化的向量检索
     *
     * @param datasetPath 数据集路径
     * @param columnName 向量列名
     * @param queryVector 查询向量
     */
    public void eval(String datasetPath, String columnName, Float[] queryVector) {
        eval(datasetPath, columnName, queryVector, 10, "L2");
    }

    /**
     * 将 RowData 转换为 Row
     */
    private Row convertToRow(RowData rowData, double distance) {
        if (rowData == null) {
            return null;
        }
        
        if (rowData instanceof GenericRowData) {
            GenericRowData genericRowData = (GenericRowData) rowData;
            int arity = genericRowData.getArity();
            
            // 创建包含距离字段的新 Row
            Object[] values = new Object[arity + 1];
            for (int i = 0; i < arity; i++) {
                Object field = genericRowData.getField(i);
                values[i] = convertField(field);
            }
            values[arity] = distance;
            
            return Row.of(values);
        }
        
        return null;
    }

    /**
     * 转换字段值
     */
    private Object convertField(Object field) {
        if (field == null) {
            return null;
        }
        
        if (field instanceof StringData) {
            return ((StringData) field).toString();
        }
        
        if (field instanceof ArrayData) {
            ArrayData arrayData = (ArrayData) field;
            int size = arrayData.size();
            Float[] result = new Float[size];
            for (int i = 0; i < size; i++) {
                if (arrayData.isNullAt(i)) {
                    result[i] = null;
                } else {
                    result[i] = arrayData.getFloat(i);
                }
            }
            return result;
        }
        
        return field;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .outputTypeStrategy(TypeStrategies.explicit(
                        DataTypes.ROW(
                                DataTypes.FIELD("id", DataTypes.BIGINT()),
                                DataTypes.FIELD("content", DataTypes.STRING()),
                                DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT())),
                                DataTypes.FIELD("_distance", DataTypes.DOUBLE())
                        )
                ))
                .build();
    }
}

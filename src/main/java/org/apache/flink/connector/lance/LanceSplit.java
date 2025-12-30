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

import org.apache.flink.core.io.InputSplit;

import java.io.Serializable;
import java.util.Objects;

/**
 * Lance 数据分片。
 * 
 * <p>代表 Lance 数据集中的一个 Fragment，用于并行读取数据。
 */
public class LanceSplit implements InputSplit, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 分片编号
     */
    private final int splitNumber;

    /**
     * Fragment ID
     */
    private final int fragmentId;

    /**
     * 数据集路径
     */
    private final String datasetPath;

    /**
     * Fragment 中的行数（估计值）
     */
    private final long rowCount;

    /**
     * 创建 LanceSplit
     *
     * @param splitNumber 分片编号
     * @param fragmentId Fragment ID
     * @param datasetPath 数据集路径
     * @param rowCount 行数
     */
    public LanceSplit(int splitNumber, int fragmentId, String datasetPath, long rowCount) {
        this.splitNumber = splitNumber;
        this.fragmentId = fragmentId;
        this.datasetPath = datasetPath;
        this.rowCount = rowCount;
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }

    /**
     * 获取 Fragment ID
     */
    public int getFragmentId() {
        return fragmentId;
    }

    /**
     * 获取数据集路径
     */
    public String getDatasetPath() {
        return datasetPath;
    }

    /**
     * 获取行数
     */
    public long getRowCount() {
        return rowCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LanceSplit that = (LanceSplit) o;
        return splitNumber == that.splitNumber &&
                fragmentId == that.fragmentId &&
                rowCount == that.rowCount &&
                Objects.equals(datasetPath, that.datasetPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitNumber, fragmentId, datasetPath, rowCount);
    }

    @Override
    public String toString() {
        return "LanceSplit{" +
                "splitNumber=" + splitNumber +
                ", fragmentId=" + fragmentId +
                ", datasetPath='" + datasetPath + '\'' +
                ", rowCount=" + rowCount +
                '}';
    }
}

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

import org.apache.flink.connector.lance.converter.LanceTypeConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import com.lancedb.lance.Dataset;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Lance Catalog 实现。
 * 
 * <p>实现 Flink Catalog 接口，支持管理 Lance 数据集作为 Flink 表。
 * 
 * <p>使用示例：
 * <pre>{@code
 * CREATE CATALOG lance_catalog WITH (
 *     'type' = 'lance',
 *     'warehouse' = '/path/to/warehouse',
 *     'default-database' = 'default'
 * );
 * }</pre>
 */
public class LanceCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(LanceCatalog.class);

    public static final String DEFAULT_DATABASE = "default";

    private final String warehouse;
    private transient BufferAllocator allocator;

    /**
     * 创建 LanceCatalog
     *
     * @param name Catalog 名称
     * @param defaultDatabase 默认数据库名称
     * @param warehouse 仓库路径
     */
    public LanceCatalog(String name, String defaultDatabase, String warehouse) {
        super(name, defaultDatabase);
        this.warehouse = warehouse;
    }

    @Override
    public void open() throws CatalogException {
        LOG.info("打开 Lance Catalog: {}, 仓库路径: {}", getName(), warehouse);
        
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        
        // 确保仓库目录存在
        Path warehousePath = Paths.get(warehouse);
        if (!Files.exists(warehousePath)) {
            try {
                Files.createDirectories(warehousePath);
            } catch (IOException e) {
                throw new CatalogException("无法创建仓库目录: " + warehouse, e);
            }
        }
        
        // 确保默认数据库存在
        Path defaultDbPath = warehousePath.resolve(getDefaultDatabase());
        if (!Files.exists(defaultDbPath)) {
            try {
                Files.createDirectories(defaultDbPath);
            } catch (IOException e) {
                throw new CatalogException("无法创建默认数据库目录: " + defaultDbPath, e);
            }
        }
    }

    @Override
    public void close() throws CatalogException {
        LOG.info("关闭 Lance Catalog: {}", getName());
        
        if (allocator != null) {
            try {
                allocator.close();
            } catch (Exception e) {
                LOG.warn("关闭分配器失败", e);
            }
            allocator = null;
        }
    }

    // ==================== Database 操作 ====================

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            Path warehousePath = Paths.get(warehouse);
            if (!Files.exists(warehousePath)) {
                return Collections.emptyList();
            }
            
            return Files.list(warehousePath)
                    .filter(Files::isDirectory)
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException("列举数据库失败", e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        
        return new CatalogDatabaseImpl(Collections.emptyMap(), "Lance Database: " + databaseName);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        Path dbPath = Paths.get(warehouse, databaseName);
        return Files.exists(dbPath) && Files.isDirectory(dbPath);
    }

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (databaseExists(name)) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(getName(), name);
            }
            return;
        }
        
        Path dbPath = Paths.get(warehouse, name);
        try {
            Files.createDirectories(dbPath);
            LOG.info("创建数据库: {}", name);
        } catch (IOException e) {
            throw new CatalogException("创建数据库失败: " + name, e);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        if (!databaseExists(name)) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(getName(), name);
            }
            return;
        }
        
        Path dbPath = Paths.get(warehouse, name);
        try {
            List<String> tables = listTables(name);
            if (!tables.isEmpty() && !cascade) {
                throw new DatabaseNotEmptyException(getName(), name);
            }
            
            // 删除数据库目录
            deleteDirectory(dbPath);
            LOG.info("删除数据库: {}", name);
        } catch (IOException e) {
            throw new CatalogException("删除数据库失败: " + name, e);
        }
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(name)) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(getName(), name);
            }
            return;
        }
        // Lance 数据库不支持修改属性
        LOG.warn("Lance Catalog 不支持修改数据库属性");
    }

    // ==================== Table 操作 ====================

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
        
        try {
            Path dbPath = Paths.get(warehouse, databaseName);
            return Files.list(dbPath)
                    .filter(Files::isDirectory)
                    .filter(path -> Files.exists(path.resolve("_versions"))) // Lance 数据集标识
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException("列举表失败", e);
        }
    }

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        // Lance 不支持视图
        return Collections.emptyList();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }
        
        String datasetPath = getDatasetPath(tablePath);
        
        try {
            Dataset dataset = Dataset.open(datasetPath, allocator);
            try {
                // 从 Lance Schema 推断 Flink Schema
                org.apache.arrow.vector.types.pojo.Schema arrowSchema = dataset.getSchema();
                RowType rowType = LanceTypeConverter.toFlinkRowType(arrowSchema);
                
                // 构建 CatalogTable
                Schema.Builder schemaBuilder = Schema.newBuilder();
                for (RowType.RowField field : rowType.getFields()) {
                    DataType dataType = LanceTypeConverter.toDataType(field.getType());
                    schemaBuilder.column(field.getName(), dataType);
                }
                
                Map<String, String> options = new HashMap<>();
                options.put("connector", LanceDynamicTableFactory.IDENTIFIER);
                options.put("path", datasetPath);
                
                return CatalogTable.of(
                        schemaBuilder.build(),
                        "Lance Table: " + tablePath.getFullName(),
                        Collections.emptyList(),
                        options
                );
            } finally {
                dataset.close();
            }
        } catch (Exception e) {
            throw new CatalogException("获取表信息失败: " + tablePath, e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            return false;
        }
        
        String datasetPath = getDatasetPath(tablePath);
        Path path = Paths.get(datasetPath);
        
        // 检查是否是有效的 Lance 数据集
        return Files.exists(path) && Files.isDirectory(path) && 
               Files.exists(path.resolve("_versions"));
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
            return;
        }
        
        String datasetPath = getDatasetPath(tablePath);
        try {
            deleteDirectory(Paths.get(datasetPath));
            LOG.info("删除表: {}", tablePath);
        } catch (IOException e) {
            throw new CatalogException("删除表失败: " + tablePath, e);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
            return;
        }
        
        ObjectPath newTablePath = new ObjectPath(tablePath.getDatabaseName(), newTableName);
        if (tableExists(newTablePath)) {
            throw new TableAlreadyExistException(getName(), newTablePath);
        }
        
        String oldPath = getDatasetPath(tablePath);
        String newPath = getDatasetPath(newTablePath);
        
        try {
            Files.move(Paths.get(oldPath), Paths.get(newPath));
            LOG.info("重命名表: {} -> {}", tablePath, newTablePath);
        } catch (IOException e) {
            throw new CatalogException("重命名表失败: " + tablePath, e);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }
        
        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(getName(), tablePath);
            }
            return;
        }
        
        // 表的实际创建在第一次写入时完成
        // 这里只记录表的元数据
        LOG.info("注册表: {}（实际数据集将在写入时创建）", tablePath);
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(getName(), tablePath);
            }
            return;
        }
        
        // Lance 不支持修改表结构
        throw new CatalogException("Lance Catalog 不支持修改表结构");
    }

    // ==================== 分区操作（Lance 不支持分区）====================

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition, boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
        throw new CatalogException("Lance Catalog 不支持分区操作");
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog 不支持分区操作");
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog 不支持分区操作");
    }

    // ==================== 函数操作（Lance 不支持用户自定义函数）====================

    @Override
    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog 不支持用户自定义函数");
    }

    @Override
    public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog 不支持用户自定义函数");
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new CatalogException("Lance Catalog 不支持用户自定义函数");
    }

    // ==================== 统计信息操作 ====================

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        // 不支持
    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        // 不支持
    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        // 不支持
    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        // 不支持
    }

    // ==================== 工具方法 ====================

    /**
     * 获取数据集路径
     */
    private String getDatasetPath(ObjectPath tablePath) {
        return Paths.get(warehouse, tablePath.getDatabaseName(), tablePath.getObjectName()).toString();
    }

    /**
     * 递归删除目录
     */
    private void deleteDirectory(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            Files.list(path).forEach(child -> {
                try {
                    deleteDirectory(child);
                } catch (IOException e) {
                    LOG.warn("删除文件失败: {}", child, e);
                }
            });
        }
        Files.deleteIfExists(path);
    }

    /**
     * 获取仓库路径
     */
    public String getWarehouse() {
        return warehouse;
    }
}

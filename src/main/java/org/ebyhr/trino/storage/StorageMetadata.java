/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ebyhr.trino.storage;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.connector.TableFunctionApplicationResult;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import org.ebyhr.trino.storage.ptf.ListTableFunction.QueryFunctionHandle;
import org.ebyhr.trino.storage.ptf.ReadFileTableFunction.ReadFunctionHandle;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.ebyhr.trino.storage.ptf.ListTableFunction.COLUMNS_METADATA;
import static org.ebyhr.trino.storage.ptf.ListTableFunction.COLUMN_HANDLES;
import static org.ebyhr.trino.storage.ptf.ListTableFunction.LIST_SCHEMA_NAME;

public class StorageMetadata
        implements ConnectorMetadata
{
    private final StorageClient storageClient;

    @Inject
    public StorageMetadata(StorageClient storageClient)
    {
        this.storageClient = requireNonNull(storageClient, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return listSchemaNames();
    }

    public List<String> listSchemaNames()
    {
        return List.copyOf(storageClient.getSchemaNames());
    }

    @Override
    public StorageTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

        StorageTable table = storageClient.getTable(session, tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return new StorageTableHandle(table.getMode(), tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        StorageTableHandle storageTableHandle = (StorageTableHandle) table;
        SchemaTablePair tableName = new SchemaTablePair(storageTableHandle.getSchemaName(), storageTableHandle.getTableName());

        return getStorageTableMetadata(session, tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameOrNull)
    {
        return listTablesImpl(session, schemaNameOrNull).map(pair -> pair.toSchemaTableName()).toList();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        StorageTableHandle storageTableHandle = (StorageTableHandle) tableHandle;

        StorageTable table = storageClient.getTable(session, storageTableHandle.getSchemaName(), storageTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(storageTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new StorageColumnHandle(column.getName(), column.getType()));
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTablePair tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getStorageTableMetadata(session, tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName.toSchemaTableName(), tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        return listTables(session, prefix).stream()
                .map(table -> TableColumnsMetadata.forTable(
                        table.toSchemaTableName(),
                        requireNonNull(getStorageTableMetadata(session, table), "tableMetadata is null")
                                .getColumns()))
                .iterator();
    }

    private ConnectorTableMetadata getStorageTableMetadata(ConnectorSession session, SchemaTablePair tableName)
    {
        if (tableName.schemaName().equals(LIST_SCHEMA_NAME)) {
            return new ConnectorTableMetadata(tableName.toSchemaTableName(), COLUMNS_METADATA);
        }

        if (!listSchemaNames().contains(tableName.schemaName())) {
            return null;
        }

        StorageTable table = storageClient.getTable(session, tableName.schemaName(), tableName.tableName());
        if (table == null) {
            return null;
        }

        return new ConnectorTableMetadata(tableName.toSchemaTableName(), table.getColumnsMetadata());
    }

    /**
     * Simplified variant of {@link SchemaTableName} that doesn't case-fold.
     */
    private static record SchemaTablePair(String schemaName, String tableName)
    {
        /**
         * Convert to {@link SchemaTableName}
         *
         * NOTE: This will case-fold the schema- and table names, so they may no longer be possible to resolve afterwards.
         */
        public SchemaTableName toSchemaTableName()
        {
            return new SchemaTableName(schemaName(), tableName());
        }
    }

    private List<SchemaTablePair> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchema().isPresent() && prefix.getTable().isPresent()) {
            return List.of(new SchemaTablePair(prefix.getSchema().get(), prefix.getTable().get()));
        }
        return listTablesImpl(session, prefix.getSchema()).toList();
    }

    private Stream<SchemaTablePair> listTablesImpl(ConnectorSession session, Optional<String> schemaNameOrNull)
    {
        List<String> schemaNames;
        if (schemaNameOrNull.isPresent()) {
            schemaNames = List.of(schemaNameOrNull.get());
        }
        else {
            schemaNames = storageClient.getSchemaNames();
        }

        return schemaNames.stream().flatMap(schemaName -> storageClient.getTableNames(schemaName).stream()
                .map(tableName -> new SchemaTablePair(LIST_SCHEMA_NAME, LIST_SCHEMA_NAME)));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((StorageColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle)
    {
        if (handle instanceof ReadFunctionHandle catFunctionHandle) {
            return Optional.of(new TableFunctionApplicationResult<>(
                    catFunctionHandle.getTableHandle(),
                    catFunctionHandle.getColumns().stream()
                            .map(column -> new StorageColumnHandle(column.getName(), column.getType()))
                            .collect(toImmutableList())));
        }
        if (handle instanceof QueryFunctionHandle queryFunctionHandle) {
            return Optional.of(new TableFunctionApplicationResult<>(queryFunctionHandle.getTableHandle(), COLUMN_HANDLES));
        }
        return Optional.empty();
    }
}

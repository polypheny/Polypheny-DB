/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.catalog.snapshot;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalKey;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.logistic.Pattern;

public interface LogicalRelSnapshot {

    /**
     * Get all tables of the specified schema which fit to the specified filters.
     *
     * @param name Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    @NonNull List<LogicalTable> getTables( @Nullable Pattern namespace, @Nullable Pattern name );

    @NonNull List<LogicalTable> getTables( long namespaceId, @Nullable Pattern name );

    @NonNull Optional<LogicalTable> getTables( @Nullable String namespace, @NonNull String name );

    @NonNull List<LogicalTable> getTablesFromNamespace( long namespace );


    /**
     * Returns the table with the given name in the specified schema.
     *
     * @param tableName The name of the table
     * @return The table
     */
    @NonNull Optional<LogicalTable> getTable( long namespaceId, String tableName );

    @NonNull Optional<LogicalTable> getTable( String namespaceName, String tableName );


    /**
     * Gets a collection of all keys.
     *
     * @return The keys
     */
    @NonNull List<LogicalKey> getKeys();


    /**
     * Get all keys for a given table.
     *
     * @param tableId The id of the table for which the keys are returned
     * @return The collection of keys
     */
    @NonNull List<LogicalKey> getTableKeys( long tableId );


    /**
     * Get all columns of the specified table.
     *
     * @param tableId The id of the table
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    @NonNull List<LogicalColumn> getColumns( long tableId );

    /**
     * Get all columns of the specified database which fit to the specified filter patterns.
     * <code>getAllocColumns(xid, databaseName, null, null, null)</code> returns all columns of the database.
     *
     * @param tableName Pattern for the table name. null returns all.
     * @param columnName Pattern for the column name. null returns all.
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    @NonNull List<LogicalColumn> getColumns( @Nullable Pattern tableName, @Nullable Pattern columnName );

    /**
     * Returns the column with the specified id.
     *
     * @param columnId The id of the column
     * @return A CatalogColumn
     */
    @NonNull Optional<LogicalColumn> getColumn( long columnId );

    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param tableId The id of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     */
    @NonNull Optional<LogicalColumn> getColumn( long tableId, String columnName );

    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param namespace
     * @param tableName The name of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     */
    @NonNull Optional<LogicalColumn> getColumn( long namespace, String tableName, String columnName );

    /**
     * Returns a specified primary key
     *
     * @param key The id of the primary key
     * @return The primary key
     */
    @NonNull
    Optional<LogicalPrimaryKey> getPrimaryKey( long key );

    /**
     * Check whether a key is a primary key
     *
     * @param keyId The id of the key
     * @return Whether the key is a primary key
     */
    boolean isPrimaryKey( long keyId );

    /**
     * Check whether a key is a foreign key
     *
     * @param keyId The id of the key
     * @return Whether the key is a foreign key
     */
    boolean isForeignKey( long keyId );

    /**
     * Check whether a key is an index
     *
     * @param keyId The id of the key
     * @return Whether the key is an index
     */
    boolean isIndex( long keyId );

    /**
     * Check whether a key is a constraint
     *
     * @param keyId The id of the key
     * @return Whether the key is a constraint
     */
    boolean isConstraint( long keyId );

    /**
     * Returns all (imported) foreign keys of a specified table
     *
     * @param tableId The id of the table
     * @return List of foreign keys
     */
    @NonNull List<LogicalForeignKey> getForeignKeys( long tableId );

    /**
     * Returns all foreign keys that reference the specified table (exported keys).
     *
     * @param tableId The id of the table
     * @return List of foreign keys
     */
    @NonNull List<LogicalForeignKey> getExportedKeys( long tableId );

    /**
     * Get all constraints of the specified table
     *
     * @param tableId The id of the table
     * @return List of constraints
     */
    @NonNull List<LogicalConstraint> getConstraints( long tableId );


    /**
     * Gets a collection of constraints for a given key.
     *
     * @param key The key for which the collection is returned
     * @return The collection of constraints
     */
    @NonNull List<LogicalConstraint> getConstraints( LogicalKey key );

    /**
     * Returns the constraint with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param constraintName The name of the constraint
     * @return The constraint
     */
    @NonNull Optional<LogicalConstraint> getConstraint( long tableId, String constraintName );

    /**
     * Return the foreign key with the specified name from the specified table
     *
     * @param tableId The id of the table
     * @param foreignKeyName The name of the foreign key
     * @return The foreign key
     */
    @NonNull Optional<LogicalForeignKey> getForeignKey( long tableId, String foreignKeyName );

    List<LogicalIndex> getIndexes();

    /**
     * Gets a collection of index for the given key.
     *
     * @param key The key for which the collection is returned
     * @return The collection of indexes
     */
    @NonNull List<LogicalIndex> getIndexes( LogicalKey key );

    /**
     * Gets a collection of foreign keys for a given {@link Catalog Key}.
     *
     * @param key The key for which the collection is returned
     * @return The collection foreign keys
     */
    @NonNull List<LogicalIndex> getForeignKeys( LogicalKey key );

    /**
     * Returns all indexes of a table
     *
     * @param tableId The id of the table
     * @param onlyUnique true if only indexes for unique values are returned. false if all indexes are returned.
     * @return List of indexes
     */
    @NonNull List<LogicalIndex> getIndexes( long tableId, boolean onlyUnique );

    /**
     * Returns the index with the specified name in the specified table
     *
     * @param tableId The id of the table
     * @param indexName The name of the index
     * @return The Index
     */
    @NonNull Optional<LogicalIndex> getIndex( long tableId, String indexName );

    /**
     * Returns the index with the specified id
     *
     * @param indexId The id of the index
     * @return The Index
     */
    @NonNull Optional<LogicalIndex> getIndex( long indexId );

    @NonNull Optional<LogicalTable> getTable( long id );


    AlgNode getNodeInfo( long id );

    List<LogicalView> getConnectedViews( long id );

    @NotNull Optional<LogicalKey> getKeys( long[] columnIds );

    @NotNull Optional<LogicalKey> getKey( long id );

    @NotNull List<LogicalConstraint> getConstraints();

    @NotNull List<LogicalPrimaryKey> getPrimaryKeys();

}

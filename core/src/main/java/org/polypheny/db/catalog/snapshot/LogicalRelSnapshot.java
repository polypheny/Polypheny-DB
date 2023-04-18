/*
 * Copyright 2019-2023 The Polypheny Project
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
import javax.annotation.Nullable;
import lombok.NonNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalKey;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalView;
import org.polypheny.db.catalog.logistic.Pattern;

public interface LogicalRelSnapshot {
    //// RELATIONAL

    /**
     * Get all tables of the specified schema which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param namespace
     * @param name Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    List<LogicalTable> getTables( @Nullable Pattern namespace, @Nullable Pattern name );

    List<LogicalTable> getTables( long namespaceId, @Nullable Pattern name );

    LogicalTable getTables( @Nullable String namespace, @NonNull String name );

    List<LogicalTable> getTablesFromNamespace( long namespace );


    /**
     * Returns the table with the given name in the specified schema.
     *
     * @param tableName The name of the table
     * @return The table
     */
    LogicalTable getTable( long namespaceId, String tableName );

    LogicalTable getTable( String namespaceName, String tableName );


    /**
     * Gets a collection of all keys.
     *
     * @return The keys
     */
    List<LogicalKey> getKeys();


    /**
     * Get all keys for a given table.
     *
     * @param tableId The id of the table for which the keys are returned
     * @return The collection of keys
     */
    List<LogicalKey> getTableKeys( long tableId );


    /**
     * Get all columns of the specified table.
     *
     * @param tableId The id of the table
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    List<LogicalColumn> getColumns( long tableId );

    /**
     * Get all columns of the specified database which fit to the specified filter patterns.
     * <code>getAllocColumns(xid, databaseName, null, null, null)</code> returns all columns of the database.
     *
     * @param tableName Pattern for the table name. null returns all.
     * @param columnName Pattern for the column name. null returns all.
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    List<LogicalColumn> getColumns( @Nullable Pattern tableName, @Nullable Pattern columnName );

    /**
     * Returns the column with the specified id.
     *
     * @param columnId The id of the column
     * @return A CatalogColumn
     */
    LogicalColumn getColumn( long columnId );

    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param tableId The id of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     */
    LogicalColumn getColumn( long tableId, String columnName );

    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param tableName The name of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     */
    LogicalColumn getColumn( String tableName, String columnName );

    /**
     * Checks if there is a column with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param columnName The name to check for
     * @return true if there is a column with this name, false if not.
     */
    boolean checkIfExistsColumn( long tableId, String columnName );

    /**
     * Returns a specified primary key
     *
     * @param key The id of the primary key
     * @return The primary key
     */
    LogicalPrimaryKey getPrimaryKey( long key );

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
    List<LogicalForeignKey> getForeignKeys( long tableId );

    /**
     * Returns all foreign keys that reference the specified table (exported keys).
     *
     * @param tableId The id of the table
     * @return List of foreign keys
     */
    List<LogicalForeignKey> getExportedKeys( long tableId );

    /**
     * Get all constraints of the specified table
     *
     * @param tableId The id of the table
     * @return List of constraints
     */
    List<CatalogConstraint> getConstraints( long tableId );


    /**
     * Gets a collection of constraints for a given key.
     *
     * @param key The key for which the collection is returned
     * @return The collection of constraints
     */
    List<CatalogConstraint> getConstraints( LogicalKey key );

    /**
     * Returns the constraint with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param constraintName The name of the constraint
     * @return The constraint
     */
    CatalogConstraint getConstraint( long tableId, String constraintName );

    /**
     * Return the foreign key with the specified name from the specified table
     *
     * @param tableId The id of the table
     * @param foreignKeyName The name of the foreign key
     * @return The foreign key
     */
    LogicalForeignKey getForeignKey( long tableId, String foreignKeyName );

    List<LogicalIndex> getIndexes();

    /**
     * Gets a collection of index for the given key.
     *
     * @param key The key for which the collection is returned
     * @return The collection of indexes
     */
    List<LogicalIndex> getIndexes( LogicalKey key );

    /**
     * Gets a collection of foreign keys for a given {@link Catalog Key}.
     *
     * @param key The key for which the collection is returned
     * @return The collection foreign keys
     */
    List<LogicalIndex> getForeignKeys( LogicalKey key );

    /**
     * Returns all indexes of a table
     *
     * @param tableId The id of the table
     * @param onlyUnique true if only indexes for unique values are returned. false if all indexes are returned.
     * @return List of indexes
     */
    List<LogicalIndex> getIndexes( long tableId, boolean onlyUnique );

    /**
     * Returns the index with the specified name in the specified table
     *
     * @param tableId The id of the table
     * @param indexName The name of the index
     * @return The Index
     */
    LogicalIndex getIndex( long tableId, String indexName );

    /**
     * Checks if there is an index with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param indexName The name to check for
     * @return true if there is an index with this name, false if not.
     */
    boolean checkIfExistsIndex( long tableId, String indexName );

    /**
     * Returns the index with the specified id
     *
     * @param indexId The id of the index
     * @return The Index
     */
    LogicalIndex getIndex( long indexId );


    LogicalTable getTable( long id );


    boolean checkIfExistsEntity( String name );


    AlgNode getNodeInfo( long id );

    List<LogicalView> getConnectedViews( long id );

    LogicalKey getKeys( long[] columnIds );

    LogicalKey getKey( long id );

}

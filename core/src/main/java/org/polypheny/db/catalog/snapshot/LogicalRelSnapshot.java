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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyException;
import org.polypheny.db.catalog.exceptions.UnknownIndexException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.logistic.Pattern;

public interface LogicalRelSnapshot {
    //// RELATIONAL

    /**
     * Get all tables of the specified schema which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param name Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    public abstract List<LogicalTable> getTables( @Nullable Pattern name );

    /**
     * Returns the table with the given name in the specified schema.
     *
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    public abstract LogicalTable getTable( String tableName );


    /**
     * Gets a collection of all keys.
     *
     * @return The keys
     */
    public abstract List<CatalogKey> getKeys();


    /**
     * Get all keys for a given table.
     *
     * @param tableId The id of the table for which the keys are returned
     * @return The collection of keys
     */
    public abstract List<CatalogKey> getTableKeys( long tableId );


    /**
     * Get all columns of the specified table.
     *
     * @param tableId The id of the table
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    public abstract List<LogicalColumn> getColumns( long tableId );

    /**
     * Get all columns of the specified database which fit to the specified filter patterns.
     * <code>getColumns(xid, databaseName, null, null, null)</code> returns all columns of the database.
     *
     * @param tableName Pattern for the table name. null returns all.
     * @param columnName Pattern for the column name. null returns all.
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    public abstract List<LogicalColumn> getColumns( @Nullable Pattern tableName, @Nullable Pattern columnName );

    /**
     * Returns the column with the specified id.
     *
     * @param columnId The id of the column
     * @return A CatalogColumn
     */
    public abstract LogicalColumn getColumn( long columnId );

    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param tableId The id of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     * @throws UnknownColumnException If there is no column with this name in the specified table of the database and schema.
     */
    public abstract LogicalColumn getColumn( long tableId, String columnName ) throws UnknownColumnException;

    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param tableName The name of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     */
    public abstract LogicalColumn getColumn( String tableName, String columnName ) throws UnknownColumnException, UnknownSchemaException, UnknownTableException;

    /**
     * Checks if there is a column with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param columnName The name to check for
     * @return true if there is a column with this name, false if not.
     */
    public abstract boolean checkIfExistsColumn( long tableId, String columnName );

    /**
     * Returns a specified primary key
     *
     * @param key The id of the primary key
     * @return The primary key
     */
    public abstract CatalogPrimaryKey getPrimaryKey( long key );

    /**
     * Check whether a key is a primary key
     *
     * @param keyId The id of the key
     * @return Whether the key is a primary key
     */
    public abstract boolean isPrimaryKey( long keyId );

    /**
     * Check whether a key is a foreign key
     *
     * @param keyId The id of the key
     * @return Whether the key is a foreign key
     */
    public abstract boolean isForeignKey( long keyId );

    /**
     * Check whether a key is an index
     *
     * @param keyId The id of the key
     * @return Whether the key is an index
     */
    public abstract boolean isIndex( long keyId );

    /**
     * Check whether a key is a constraint
     *
     * @param keyId The id of the key
     * @return Whether the key is a constraint
     */
    public abstract boolean isConstraint( long keyId );

    /**
     * Returns all (imported) foreign keys of a specified table
     *
     * @param tableId The id of the table
     * @return List of foreign keys
     */
    public abstract List<CatalogForeignKey> getForeignKeys( long tableId );

    /**
     * Returns all foreign keys that reference the specified table (exported keys).
     *
     * @param tableId The id of the table
     * @return List of foreign keys
     */
    public abstract List<CatalogForeignKey> getExportedKeys( long tableId );

    /**
     * Get all constraints of the specified table
     *
     * @param tableId The id of the table
     * @return List of constraints
     */
    public abstract List<CatalogConstraint> getConstraints( long tableId );


    /**
     * Gets a collection of constraints for a given key.
     *
     * @param key The key for which the collection is returned
     * @return The collection of constraints
     */
    public abstract List<CatalogConstraint> getConstraints( CatalogKey key );

    /**
     * Returns the constraint with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param constraintName The name of the constraint
     * @return The constraint
     */
    public abstract CatalogConstraint getConstraint( long tableId, String constraintName ) throws UnknownConstraintException;

    /**
     * Return the foreign key with the specified name from the specified table
     *
     * @param tableId The id of the table
     * @param foreignKeyName The name of the foreign key
     * @return The foreign key
     */
    public abstract CatalogForeignKey getForeignKey( long tableId, String foreignKeyName ) throws UnknownForeignKeyException;

    public abstract List<CatalogIndex> getIndexes();

    /**
     * Gets a collection of index for the given key.
     *
     * @param key The key for which the collection is returned
     * @return The collection of indexes
     */
    public abstract List<CatalogIndex> getIndexes( CatalogKey key );

    /**
     * Gets a collection of foreign keys for a given {@link Catalog Key}.
     *
     * @param key The key for which the collection is returned
     * @return The collection foreign keys
     */
    public abstract List<CatalogIndex> getForeignKeys( CatalogKey key );

    /**
     * Returns all indexes of a table
     *
     * @param tableId The id of the table
     * @param onlyUnique true if only indexes for unique values are returned. false if all indexes are returned.
     * @return List of indexes
     */
    public abstract List<CatalogIndex> getIndexes( long tableId, boolean onlyUnique );

    /**
     * Returns the index with the specified name in the specified table
     *
     * @param tableId The id of the table
     * @param indexName The name of the index
     * @return The Index
     */
    public abstract CatalogIndex getIndex( long tableId, String indexName ) throws UnknownIndexException;

    /**
     * Checks if there is an index with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param indexName The name to check for
     * @return true if there is an index with this name, false if not.
     */
    public abstract boolean checkIfExistsIndex( long tableId, String indexName );

    /**
     * Returns the index with the specified id
     *
     * @param indexId The id of the index
     * @return The Index
     */
    public abstract CatalogIndex getIndex( long indexId );


    LogicalTable getTable( long id );


    boolean checkIfExistsEntity( String newName );

}

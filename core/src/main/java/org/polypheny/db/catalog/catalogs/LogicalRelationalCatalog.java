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

package org.polypheny.db.catalog.catalogs;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogView;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyException;
import org.polypheny.db.catalog.exceptions.UnknownIndexException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.IndexType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.type.PolyType;

public interface LogicalRelationalCatalog extends LogicalCatalog {

    /**
     * Get all tables of the specified schema which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param name Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    public abstract List<LogicalTable> getTables( @Nullable Pattern name );

    /**
     * Returns the table with the given id
     *
     * @param tableId The id of the table
     * @return The table
     */
    public abstract LogicalTable getTable( long tableId );

    /**
     * Returns the table with the given name in the specified schema.
     *
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    public abstract LogicalTable getTable( String tableName ) throws UnknownTableException;

    /**
     * Returns the table which is associated with a given partitionId
     *
     * @param partitionId to use for lookup
     * @return CatalogEntity that contains partitionId
     */
    public abstract LogicalTable getTableFromPartition( long partitionId );

    /**
     * Adds a table to a specified schema.
     *
     * @param name The name of the table to add
     * @param entityType The table type
     * @param modifiable Whether the content of the table can be modified
     * @return The id of the inserted table
     */
    public abstract long addTable( String name, EntityType entityType, boolean modifiable );


    /**
     * Adds a view to a specified schema.
     *
     * @param name The name of the view to add
     * @param namespaceId The id of the schema
     * @param entityType The table type
     * @param modifiable Whether the content of the table can be modified
     * @param definition {@link AlgNode} used to create Views
     * @param underlyingTables all tables and columns used within the view
     * @param fieldList all columns used within the View
     * @return The id of the inserted table
     */
    public abstract long addView( String name, long namespaceId, EntityType entityType, boolean modifiable, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList, String query, QueryLanguage language );

    /**
     * Adds a materialized view to a specified schema.
     *
     * @param name of the view to add
     * @param namespaceId id of the schema
     * @param entityType type of table
     * @param modifiable Whether the content of the table can be modified
     * @param definition {@link AlgNode} used to create Views
     * @param algCollation relCollation used for materialized view
     * @param underlyingTables all tables and columns used within the view
     * @param fieldList all columns used within the View
     * @param materializedCriteria Information like freshness and last updated
     * @param query used to define materialized view
     * @param language query language used to define materialized view
     * @param ordered if materialized view is ordered or not
     * @return id of the inserted materialized view
     */
    public abstract long addMaterializedView( String name, long namespaceId, EntityType entityType, boolean modifiable, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList, MaterializedCriteria materializedCriteria, String query, QueryLanguage language, boolean ordered ) throws GenericCatalogException;

    /**
     * Renames a table
     *
     * @param tableId The if of the table to rename
     * @param name New name of the table
     */
    public abstract void renameTable( long tableId, String name );

    /**
     * Delete the specified table. Columns need to be deleted before.
     *
     * @param tableId The id of the table to delete
     */
    public abstract void deleteTable( long tableId );

    /**
     * Change owner of a table
     *
     * @param tableId The if of the table
     * @param ownerId ID of the new owner
     */
    public abstract void setTableOwner( long tableId, long ownerId );

    /**
     * Set the primary key of a table
     *
     * @param tableId The id of the table
     * @param keyId The id of the key to set as primary key. Set null to set no primary key.
     */
    public abstract void setPrimaryKey( long tableId, Long keyId );


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
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @param columnNamePattern Pattern for the column name. null returns all.
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    public abstract List<LogicalColumn> getColumns( @Nullable Pattern tableNamePattern, @Nullable Pattern columnNamePattern );

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
     * Adds a column.
     *
     * @param name The name of the column
     * @param tableId The id of the corresponding table
     * @param position The ordinal position of the column (starting with 1)
     * @param type The type of the column
     * @param length The length of the field (if applicable, else null)
     * @param scale The number of digits after the decimal point (if applicable, else null)
     * @param nullable Weather the column can contain null values
     * @param collation The collation of the field (if applicable, else null)
     * @return The id of the inserted column
     */
    public abstract long addColumn( String name, long tableId, int position, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality, boolean nullable, Collation collation );

    /**
     * Renames a column
     *
     * @param columnId The if of the column to rename
     * @param name New name of the column
     */
    public abstract void renameColumn( long columnId, String name );

    /**
     * Change the position of the column.
     *
     * @param columnId The id of the column for which to change the position
     * @param position The new position of the column
     */
    public abstract void setColumnPosition( long columnId, int position );

    /**
     * Change the data type of a column.
     *
     * @param columnId The id of the column
     * @param type The new type of the column
     */
    public abstract void setColumnType( long columnId, PolyType type, PolyType collectionsType, Integer length, Integer precision, Integer dimension, Integer cardinality ) throws GenericCatalogException;

    /**
     * Change nullability of the column (weather the column allows null values).
     *
     * @param columnId The id of the column
     * @param nullable True if the column should allow null values, false if not.
     */
    public abstract void setNullable( long columnId, boolean nullable ) throws GenericCatalogException;

    /**
     * Set the collation of a column.
     * If the column already has the specified collation set, this method is a NoOp.
     *
     * @param columnId The id of the column
     * @param collation The collation to set
     */
    public abstract void setCollation( long columnId, Collation collation );

    /**
     * Checks if there is a column with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param columnName The name to check for
     * @return true if there is a column with this name, false if not.
     */
    public abstract boolean checkIfExistsColumn( long tableId, String columnName );

    /**
     * Delete the specified column. This also deletes a default value in case there is one defined for this column.
     *
     * @param columnId The id of the column to delete
     */
    public abstract void deleteColumn( long columnId );

    /**
     * Adds a default value for a column. If there already is a default values, it being replaced.
     *
     * @param columnId The id of the column
     * @param type The type of the default value
     * @param defaultValue True if the column should allow null values, false if not.
     */
    public abstract void setDefaultValue( long columnId, PolyType type, String defaultValue );

    /**
     * Deletes an existing default value of a column. NoOp if there is no default value defined.
     *
     * @param columnId The id of the column
     */
    public abstract void deleteDefaultValue( long columnId );

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
     * Adds a primary key
     *
     * @param tableId The id of the table
     * @param columnIds The id of key which will be part of the primary keys
     */
    public abstract void addPrimaryKey( long tableId, List<Long> columnIds ) throws GenericCatalogException;

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

    /**
     * Adds a unique foreign key constraint.
     *
     * @param tableId The id of the table
     * @param columnIds The id of the columns which are part of the foreign key
     * @param referencesTableId The if of the referenced table
     * @param referencesIds The id of columns forming the key referenced by this key
     * @param constraintName The name of the constraint
     * @param onUpdate The option for updates
     * @param onDelete The option for deletes
     */
    public abstract void addForeignKey( long tableId, List<Long> columnIds, long referencesTableId, List<Long> referencesIds, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete ) throws GenericCatalogException;

    /**
     * Adds a unique constraint.
     *
     * @param tableId The id of the table
     * @param constraintName The name of the constraint
     * @param columnIds A list of column ids
     */
    public abstract void addUniqueConstraint( long tableId, String constraintName, List<Long> columnIds ) throws GenericCatalogException;

    /**
     * Deletes the specified primary key (including the entry in the key table). If there is an index on this key, make sure to delete it first.
     *
     * @param tableId The id of the key to drop
     */
    public abstract void deletePrimaryKey( long tableId ) throws GenericCatalogException;

    /**
     * Delete the specified foreign key (does not delete the referenced key).
     *
     * @param foreignKeyId The id of the foreign key to delete
     */
    public abstract void deleteForeignKey( long foreignKeyId ) throws GenericCatalogException;

    /**
     * Delete the specified constraint.
     * For deleting foreign keys, use {@link #deleteForeignKey(long)}.
     *
     * @param constraintId The id of the constraint to delete
     */
    public abstract void deleteConstraint( long constraintId ) throws GenericCatalogException;


    /**
     * Deletes all the dependencies of a view. This is used when deleting a view.
     *
     * @param catalogView view for which to delete its dependencies
     */
    public abstract void deleteViewDependencies( CatalogView catalogView );

    /**
     * Updates the last time a materialized view has been refreshed.
     *
     * @param materializedViewId id of the materialized view
     */
    public abstract void updateMaterializedViewRefreshTime( long materializedViewId );


    /**
     * Returns all tables which are in need of special periodic treatment.
     *
     * @return List of tables which need to be periodically processed
     */
    public abstract List<LogicalTable> getTablesForPeriodicProcessing();


    /**
     * Flags the table for deletion.
     * This method should be executed on a partitioned table before we run a DROP TABLE statement.
     *
     * @param tableId table to be flagged for deletion
     * @param flag true if it should be flagged, false if flag should be removed
     */
    public abstract void flagTableForDeletion( long tableId, boolean flag );

    /**
     * Is used to detect if a table is flagged for deletion.
     * Effectively checks if a drop of this table is currently in progress.
     * This is needed to ensure that there aren't any constraints when recursively removing a table and all placements and partitions.
     *
     * @param tableId table to be checked
     * @return If table is flagged for deletion or not
     */
    public abstract boolean isTableFlaggedForDeletion( long tableId );

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

    /**
     * Returns list of all indexes
     *
     * @return List of indexes
     */
    public abstract List<CatalogIndex> getIndexes();

    /**
     * Adds an index over the specified columns
     *
     * @param tableId The id of the table
     * @param columnIds A list of column ids
     * @param unique Weather the index is unique
     * @param method Name of the index method (e.g. btree_unique)
     * @param methodDisplayName Display name of the index method (e.g. BTREE)
     * @param adapterId ID of the data store where the index is located (0 for Polypheny-DB itself)
     * @param type The type of index (manual, automatic)
     * @param indexName The name of the index
     * @return The id of the created index
     */
    public abstract long addIndex( long tableId, List<Long> columnIds, boolean unique, String method, String methodDisplayName, long adapterId, IndexType type, String indexName ) throws GenericCatalogException;

    /**
     * Set physical index name.
     *
     * @param indexId The id of the index
     * @param physicalName The physical name to be set
     */
    public abstract void setIndexPhysicalName( long indexId, String physicalName );

    /**
     * Delete the specified index
     *
     * @param indexId The id of the index to drop
     */
    public abstract void deleteIndex( long indexId );

}

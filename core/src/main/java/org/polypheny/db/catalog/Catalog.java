/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.catalog;


import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.polypheny.db.catalog.entity.*;
import org.polypheny.db.catalog.exceptions.*;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;


public abstract class Catalog {

    protected final PropertyChangeSupport listeners = new PropertyChangeSupport( this );
    public boolean isPersistent = false;
    public static Catalog INSTANCE = null;
    public static boolean resetCatalog;
    public static boolean memoryCatalog;
    public static boolean testMode;


    public static Catalog setAndGetInstance( Catalog catalog ) {
        if ( INSTANCE != null ) {
            throw new RuntimeException( "Setting the Catalog, when already set is not permitted." );
        }
        INSTANCE = catalog;
        return INSTANCE;
    }


    public static Catalog getInstance() {
        if ( INSTANCE == null ) {
            throw new RuntimeException( "Catalog was not set correctly on Polypheny-DB start-up" );
        }
        return INSTANCE;
    }


    public abstract void commit() throws NoTablePrimaryKeyException;

    public abstract void rollback();


    /**
     * Adds a listener which gets notified on store update
     *
     * @param listener which gets added
     */
    public void addObserver( PropertyChangeListener listener ) {
        listeners.addPropertyChangeListener( listener );
    }


    /**
     * Removes a registered observer
     *
     * @param listener which gets removed
     */
    public void removeObserver( PropertyChangeListener listener ) {
        listeners.removePropertyChangeListener( listener );
    }


    /**
     * Validates that all columns have a valid placement,
     * else deletes them.
     * TODO DL check
     */
    public abstract void validateColumns();

    /**
     * Restores all columnPlacements in the dedicated store
     */
    public abstract void restoreColumnPlacements( Transaction trx ) throws GenericCatalogException;


    protected final boolean isValidIdentifier( final String str ) {
        return str.length() <= 100 && str.matches( "^[a-z_][a-z0-9_]*$" ) && !str.isEmpty();
    }


    public abstract long addDatabase( String name, int ownerId, String ownerName, long defaultSchemaId, String defaultSchemaName );

    public abstract void deleteDatabase( long databaseId ) throws UnknownDatabaseException;

    /**
     * Get all databases
     *
     * @param pattern A pattern for the database name
     * @return List of databases
     */
    public abstract List<CatalogDatabase> getDatabases( Pattern pattern ) throws GenericCatalogException;

    /**
     * Returns the database with the given name.
     *
     * @param databaseName The name of the database
     * @return The database
     * @throws UnknownDatabaseException If there is no database with this name.
     */
    public abstract CatalogDatabase getDatabase( String databaseName ) throws GenericCatalogException, UnknownDatabaseException;

    /**
     * Returns the database with the given name.
     *
     * @param databaseId The id of the database
     * @return The database
     * @throws UnknownDatabaseException If there is no database with this name.
     */
    public abstract CatalogDatabase getDatabase( long databaseId ) throws GenericCatalogException, UnknownDatabaseException;

    /**
     * Get all schemas which fit to the specified filter pattern.
     * <code>getSchemas(xid, null, null)</code> returns all schemas of all databases.
     *
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogSchema> getSchemas( Pattern databaseNamePattern, Pattern schemaNamePattern ) throws GenericCatalogException, UnknownSchemaException;

    /**
     * Get all schemas of the specified database which fit to the specified filter pattern.
     * <code>getSchemas(xid, databaseName, null)</code> returns all schemas of the database.
     *
     * @param databaseId The id of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogSchema> getSchemas( long databaseId, Pattern schemaNamePattern ) throws GenericCatalogException, UnknownSchemaException;

    public abstract CatalogSchema getSchema( long schemaId ) throws UnknownSchemaException;

    /**
     * Returns the schema with the given name in the specified database.
     *
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @return The schema
     * @throws UnknownSchemaException If there is no schema with this name in the specified database.
     */
    public abstract CatalogSchema getSchema( String databaseName, String schemaName ) throws GenericCatalogException, UnknownSchemaException, UnknownCollationException, UnknownDatabaseException, UnknownSchemaTypeException;

    /**
     * Returns the schema with the given name in the specified database.
     *
     * @param databaseId The id of the database
     * @param schemaName The name of the schema
     * @return The schema
     * @throws UnknownSchemaException If there is no schema with this name in the specified database.
     */
    public abstract CatalogSchema getSchema( long databaseId, String schemaName ) throws GenericCatalogException, UnknownSchemaException;

    /**
     * Adds a schema in a specified database
     *
     * @param name The name of the schema
     * @param databaseId The id of the associated database
     * @param ownerId The owner of this schema
     * @param schemaType The type of this schema
     * @return The id of the inserted schema
     * @throws GenericCatalogException A generic catalog exception
     */
    public abstract long addSchema( String name, long databaseId, int ownerId, SchemaType schemaType ) throws GenericCatalogException, UnknownSchemaException;

    /**
     * Checks weather a schema with the specified name exists in a database.
     *
     * @param databaseId The if of the database
     * @param schemaName The name of the schema to check
     * @return True if there is a schema with this name. False if not.
     * @throws GenericCatalogException A generic catalog exception
     */
    public abstract boolean checkIfExistsSchema( long databaseId, String schemaName ) throws GenericCatalogException;

    /**
     * Renames a schema
     *
     * @param schemaId The if of the schema to rename
     * @param name New name of the schema
     * @throws GenericCatalogException A generic catalog exception
     */
    public abstract void renameSchema( long schemaId, String name ) throws GenericCatalogException, UnknownSchemaException;

    /**
     * Change owner of a schema
     *
     * @param schemaId The if of the schema
     * @param ownerId Id of the new owner
     * @throws GenericCatalogException A generic catalog exception
     */
    public abstract void setSchemaOwner( long schemaId, long ownerId ) throws GenericCatalogException, UnknownSchemaException;

    /**
     * Delete a schema from the catalog
     *
     * @param schemaId The if of the schema to delete
     * @throws GenericCatalogException A generic catalog exception
     */
    public abstract void deleteSchema( long schemaId ) throws GenericCatalogException, UnknownSchemaException;


    /**
     * Get all tables of the specified schema which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param schemaId The id of the schema
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogTable> getTables( long schemaId, Pattern tableNamePattern ) throws GenericCatalogException;

    /**
     * Get all tables of the specified database which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param databaseId The id of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogTable> getTables( long databaseId, Pattern schemaNamePattern, Pattern tableNamePattern ) throws GenericCatalogException;

    /**
     * Returns the table with the given name in the specified database and schema.
     *
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    public abstract CatalogTable getTable( String databaseName, String schemaName, String tableName ) throws UnknownTableException, GenericCatalogException;

    /**
     * Get all tables of the specified database which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogTable> getTables( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern ) throws GenericCatalogException;

    /**
     * Returns the table with the given id
     *
     * @param tableId The id of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    public abstract CatalogTable getTable( long tableId ) throws UnknownTableException, GenericCatalogException;


    /**
     * Returns the table with the given name in the specified schema.
     *
     * @param schemaId The id of the schema
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    public abstract CatalogTable getTable( long schemaId, String tableName ) throws UnknownTableException, GenericCatalogException;

    /**
     * Returns the table with the given name in the specified database and schema.
     *
     * @param databaseId The id of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    public abstract CatalogTable getTable( long databaseId, String schemaName, String tableName ) throws UnknownTableException, GenericCatalogException;

    /**
     * Adds a table to a specified schema.
     *
     * @param name The name of the table to add
     * @param schemaId The id of the schema
     * @param ownerId The if of the owner
     * @param tableType The table type
     * @param definition The definition of this table (e.g. a SQL string; null if not applicable)
     * @return The id of the inserted table
     * @throws GenericCatalogException A generic catalog exception
     */
    public abstract long addTable( String name, long schemaId, int ownerId, TableType tableType, String definition ) throws GenericCatalogException;

    /**
     * Checks if there is a table with the specified name in the specified schema.
     *
     * @param schemaId The id of the schema
     * @param tableName The name to check for
     * @return true if there is a table with this name, false if not.
     * @throws GenericCatalogException A generic catalog exception
     */
    public abstract boolean checkIfExistsTable( long schemaId, String tableName ) throws GenericCatalogException, UnknownSchemaException;

    /**
     * Renames a table
     *
     * @param tableId The if of the table to rename
     * @param name New name of the table
     * @throws GenericCatalogException A generic catalog exception
     */
    public abstract void renameTable( long tableId, String name ) throws GenericCatalogException, UnknownTableException;

    /**
     * Delete the specified table. Columns need to be deleted before.
     *
     * @param tableId The id of the table to delete
     */
    public abstract void deleteTable( long tableId ) throws GenericCatalogException, UnknownTableException;

    /**
     * Change owner of a table
     *
     * @param tableId The if of the table
     * @param ownerId Id of the new owner
     * @throws GenericCatalogException A generic catalog exception
     */
    public abstract void setTableOwner( long tableId, int ownerId ) throws GenericCatalogException, UnknownTableException;

    /**
     * Set the primary key of a table
     *
     * @param tableId The id of the table
     * @param keyId The id of the key to set as primary key. Set null to set no primary key.
     */
    public abstract void setPrimaryKey( long tableId, Long keyId ) throws GenericCatalogException, UnknownTableException;

    /**
     * Adds a placement for a column.
     *
     * @param storeId The store on which the table should be placed on
     * @param columnId The id of the column to be placed
     * @param placementType The type of placement
     * @param physicalSchemaName The schema name on the data store
     * @param physicalTableName The table name on the data store
     * @param physicalColumnName The column name on the data store
     * @throws GenericCatalogException A generic catalog exception
     */
    public abstract void addColumnPlacement( int storeId, long columnId, PlacementType placementType, String physicalSchemaName, String physicalTableName, String physicalColumnName ) throws GenericCatalogException;

    /**
     * Deletes a column placement
     *
     * @param storeId The id of the store
     * @param columnId The id of the column
     */
    public abstract void deleteColumnPlacement( int storeId, long columnId ) throws GenericCatalogException;


    /**
     * Get a specific column placement.
     *
     * @param storeId The id of the store
     * @param columnId The id of the column
     * @return The specific column placement
     * @throws GenericCatalogException A generic catalog exception
     */
    public abstract CatalogColumnPlacement getColumnPlacement( int storeId, long columnId ) throws GenericCatalogException;


    /**
     * Checks if there is a column with the specified name in the specified table.
     *
     * @param storeId  The id of the store
     * @param columnId The id of the column
     * @return true if there is a column placement, false if not.
     * @throws GenericCatalogException A generic catalog exception
     */
    public abstract boolean checkIfExistsColumnPlacement( int storeId, long columnId );


    /**
     * Get all placements of a column
     *
     * @param columnId The id of the column
     * @return List of placements
     */
    public abstract List<CatalogColumnPlacement> getColumnPlacements( long columnId );

    public abstract List<CatalogColumnPlacement> getColumnPlacementsOnStore( int storeId, long tableId );


    /**
     * Get column placements on a store
     *
     * @param storeId The id of the store
     * @return List of column placements on this store
     */
    public abstract List<CatalogColumnPlacement> getColumnPlacementsOnStore( int storeId );

    public abstract List<CatalogColumnPlacement> getColumnPlacementsByColumn( long columnId );

    public abstract List<CatalogKey> getKeys();

    public abstract List<CatalogKey> getTableKeys( long tableId );

    /**
     * Get column placements in a specific schema on a specific store
     *
     * @param storeId The id of the store
     * @param schemaId The id of the schema
     * @return List of column placements on this store and schema
     */
    public abstract List<CatalogColumnPlacement> getColumnPlacementsOnStoreAndSchema( int storeId, long schemaId ) throws GenericCatalogException;


    /**
     * Update type of a placement.
     *
     * @param storeId       The id of the store
     * @param columnId      The id of the column
     * @param placementType The new type of placement
     */
    public abstract void updateColumnPlacementType( int storeId, long columnId, PlacementType placementType ) throws UnknownColumnPlacementException;


    /**
     * Change physical names of a placement.
     *
     * @param storeId            The id of the store
     * @param columnId           The id of the column
     * @param physicalSchemaName The physical schema name
     * @param physicalTableName  The physical table name
     * @param physicalColumnName The physical column name
     */
    public abstract void updateColumnPlacementPhysicalNames( int storeId, long columnId, String physicalSchemaName, String physicalTableName, String physicalColumnName ) throws GenericCatalogException, UnknownColumnPlacementException;


    /**
     * Get all columns of the specified table.
     *
     * @param tableId The id of the table
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogColumn> getColumns( long tableId );


    /**
     * Get all columns of the specified database which fit to the specified filter patterns.
     * <code>getColumns(xid, databaseName, null, null, null)</code> returns all columns of the database.
     *
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @param columnNamePattern Pattern for the column name. null returns all.
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogColumn> getColumns( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern, Pattern columnNamePattern );

    /**
     * Returns the column with the specified id.
     *
     * @param columnId The id of the column
     * @return A CatalogColumn
     * @throws UnknownColumnException If there is no column with this id
     */
    public abstract CatalogColumn getColumn( long columnId ) throws UnknownColumnException, GenericCatalogException;

    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param tableId The id of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     * @throws UnknownColumnException If there is no column with this name in the specified table of the database and schema.
     */
    public abstract CatalogColumn getColumn( long tableId, String columnName ) throws GenericCatalogException, UnknownColumnException;

    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     * @throws UnknownColumnException If there is no column with this name in the specified table of the database and schema.
     */
    public abstract CatalogColumn getColumn( String databaseName, String schemaName, String tableName, String columnName ) throws GenericCatalogException, UnknownColumnException;

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
    public abstract long addColumn( String name, long tableId, int position, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality, boolean nullable, Collation collation ) throws GenericCatalogException;

    /**
     * Renames a column
     *
     * @param columnId The if of the column to rename
     * @param name New name of the column
     * @throws GenericCatalogException A generic catalog exception
     */
    public abstract void renameColumn( long columnId, String name ) throws GenericCatalogException, UnknownColumnException;

    /**
     * Change the position of the column.
     *
     * @param columnId The id of the column for which to change the position
     * @param position The new position of the column
     */
    public abstract void setColumnPosition( long columnId, int position ) throws GenericCatalogException, UnknownColumnException;

    /**
     * Change the data type of an column.
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
    public abstract void setCollation( long columnId, Collation collation ) throws GenericCatalogException, UnknownColumnException;

    /**
     * Checks if there is a column with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param columnName The name to check for
     * @return true if there is a column with this name, false if not.
     * @throws GenericCatalogException A generic catalog exception
     */
    public abstract boolean checkIfExistsColumn( long tableId, String columnName ) throws GenericCatalogException, UnknownTableException;

    /**
     * Delete the specified column. This also deletes a default value in case there is one defined for this column.
     *
     * @param columnId The id of the column to delete
     */
    public abstract void deleteColumn( long columnId ) throws GenericCatalogException;

    /**
     * Adds a default value for a column. If there already is a default values, it being replaced.
     *
     * @param columnId The id of the column
     * @param type The type of the default value
     * @param defaultValue True if the column should allow null values, false if not.
     */


    public abstract void setDefaultValue( long columnId, PolyType type, String defaultValue ) throws GenericCatalogException, UnknownColumnException;


    /**
     * Deletes an existing default value of a column. NoOp if there is no default value defined.
     *
     * @param columnId The id of the column
     */
    public abstract void deleteDefaultValue( long columnId ) throws GenericCatalogException, UnknownColumnException;

    /**
     * Returns a specified primary key
     *
     * @param key The id of the primary key
     * @return The primary key
     */
    public abstract CatalogPrimaryKey getPrimaryKey( long key ) throws GenericCatalogException, UnknownKeyException;


    public abstract boolean isPrimaryKey( long key );

    public abstract boolean isForeignKey( long keyId );

    public abstract boolean isIndex( long keyId );

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
    public abstract List<CatalogForeignKey> getForeignKeys( long tableId ) throws GenericCatalogException;

    /**
     * Returns all foreign keys that reference the specified table (exported keys).
     *
     * @param tableId The id of the table
     * @return List of foreign keys
     */
    public abstract List<CatalogForeignKey> getExportedKeys( long tableId ) throws GenericCatalogException;

    /**
     * Get all constraints of the specified table
     *
     * @param tableId The id of the table
     * @return List of constraints
     */
    public abstract List<CatalogConstraint> getConstraints( long tableId ) throws GenericCatalogException;


    public abstract List<CatalogIndex> getIndices( CatalogKey key );

    public abstract List<CatalogIndex> getForeignKeys( CatalogKey key );

    public abstract List<CatalogConstraint> getConstraints( CatalogKey key );

    /**
     * Returns the constraint with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param constraintName The name of the constraint
     * @return The constraint
     */
    public abstract CatalogConstraint getConstraint( long tableId, String constraintName ) throws GenericCatalogException, UnknownConstraintException;

    /**
     * Return the foreign key with the specified name from the specified table
     *
     * @param tableId The id of the table
     * @param foreignKeyName The name of the foreign key
     * @return The foreign key
     */
    public abstract CatalogForeignKey getForeignKey( long tableId, String foreignKeyName ) throws GenericCatalogException, UnknownForeignKeyException;

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
     * Returns all indexes of a table
     *
     * @param tableId The id of the table
     * @param onlyUnique true if only indexes for unique values are returned. false if all indexes are returned.
     * @return List of indexes
     */
    public abstract List<CatalogIndex> getIndexes( long tableId, boolean onlyUnique ) throws GenericCatalogException;

    /**
     * Returns the index with the specified name in the specified table
     *
     * @param tableId The id of the table
     * @param indexName The name of the index
     * @return The Index
     */
    public abstract CatalogIndex getIndex( long tableId, String indexName ) throws GenericCatalogException, UnknownIndexException;

    /**
     * Adds an index over the specified columns
     *
     * @param tableId The id of the table
     * @param columnIds A list of column ids
     * @param unique Weather the index should be unique
     * @param type The type of index
     * @param indexName The name of the index
     * @return The id of the created index
     */
    public abstract long addIndex( long tableId, List<Long> columnIds, boolean unique, IndexType type, String indexName ) throws GenericCatalogException;

    /**
     * Delete the specified index
     *
     * @param indexId The id of the index to drop
     */
    public abstract void deleteIndex( long indexId ) throws GenericCatalogException, UnknownIndexException;

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
     * Get the user with the specified name
     *
     * @param userName The name of the user
     * @return The user
     * @throws UnknownUserException If there is no user with the specified name
     */
    public abstract CatalogUser getUser( String userName ) throws UnknownUserException, GenericCatalogException;

    /**
     * Get list of all stores
     *
     * @return List of stores
     */
    public abstract List<CatalogStore> getStores() throws GenericCatalogException;

    /**
     * Get a store by its unique name
     *
     * @return List of stores
     */
    public abstract CatalogStore getStore( String uniqueName ) throws GenericCatalogException, UnknownStoreException;

    /**
     * Get a store by its id
     *
     * @return List of stores
     */
    public abstract CatalogStore getStore( int storeId ) throws GenericCatalogException, UnknownStoreException;

    /**
     * Add a store
     *
     * @param uniqueName The unique name of the store
     * @param adapter    The class name of the adapter
     * @param settings   The configuration of the store
     * @return The id of the newly added store
     */
    public abstract int addStore( String uniqueName, String adapter, Map<String, String> settings ) throws GenericCatalogException;

    /**
     * Delete a store
     *
     * @param storeId The id of the store to delete
     */
    public abstract void deleteStore( int storeId ) throws GenericCatalogException, UnknownStoreException;



    public abstract long addPartition( long tableId, long schemaId, int ownerId, String partitonType ) throws GenericCatalogException;


    public abstract List<CatalogPartition> getPartitions(long tableId );
    /*
     *
     */

    // public abstract CatalogCombinedKey getCombinedKey( long keyId ) throws GenericCatalogException, UnknownKeyException;


    public abstract void close();

    public abstract void clear();


    public enum TableType {
        TABLE( 1 ),
        VIEW( 2 );
        // STREAM, ...

        private final int id;


        TableType( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static TableType getById( final int id ) throws UnknownTableTypeException {
            for ( TableType t : values() ) {
                if ( t.id == id ) {
                    return t;
                }
            }
            throw new UnknownTableTypeException( id );
        }


        public static TableType getByName( final String name ) throws UnknownTableTypeException {
            for ( TableType t : values() ) {
                if ( t.name().equalsIgnoreCase( name ) ) {
                    return t;
                }
            }
            throw new UnknownTableTypeException( name );
        }


        // Used for creating ResultSets
        public Object[] getParameterArray() {
            return new Object[]{ name() };
        }


        // Required for building JDBC result set
        @RequiredArgsConstructor
        public class PrimitiveTableType {

            public final String tableType;
        }
    }


    public enum SchemaType {
        RELATIONAL( 1 );
        // GRAPH, DOCUMENT, ...

        private final int id;


        SchemaType( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static SchemaType getById( final int id ) throws UnknownSchemaTypeException {
            for ( SchemaType t : values() ) {
                if ( t.id == id ) {
                    return t;
                }
            }
            throw new UnknownSchemaTypeException( id );
        }


        public static SchemaType getByName( final String name ) throws UnknownSchemaTypeException {
            for ( SchemaType t : values() ) {
                if ( t.name().equalsIgnoreCase( name ) ) {
                    return t;
                }
            }
            throw new UnknownSchemaTypeException( name );
        }
    }


    public enum Collation {
        CASE_SENSITIVE( 1 ),
        CASE_INSENSITIVE( 2 );

        private final int id;


        Collation( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static Collation getById( int id ) throws UnknownCollationException {
            for ( Collation c : values() ) {
                if ( c.id == id ) {
                    return c;
                }
            }
            throw new UnknownCollationException( id );
        }


        public static Collation parse( @NonNull String str ) throws UnknownCollationException {
            if ( str.equalsIgnoreCase( "CASE SENSITIVE" ) ) {
                return Collation.CASE_SENSITIVE;
            } else if ( str.equalsIgnoreCase( "CASE INSENSITIVE" ) ) {
                return Collation.CASE_INSENSITIVE;
            }
            throw new UnknownCollationException( str );
        }
    }


    public enum IndexType {
        BTREE( 1 ),
        HASH( 2 );

        private final int id;


        IndexType( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static IndexType getById( int id ) throws UnknownIndexTypeException {
            for ( IndexType e : values() ) {
                if ( e.id == id ) {
                    return e;
                }
            }
            throw new UnknownIndexTypeException( id );
        }


        public static IndexType parse( @NonNull String str ) throws UnknownIndexTypeException {
            if ( str.equalsIgnoreCase( "btree" ) ) {
                return IndexType.BTREE;
            } else if ( str.equalsIgnoreCase( "hash" ) ) {
                return IndexType.HASH;
            }
            throw new UnknownIndexTypeException( str );
        }
    }


    public enum ConstraintType {
        UNIQUE( 1 );

        private final int id;


        ConstraintType( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static ConstraintType getById( int id ) throws UnknownConstraintTypeException {
            for ( ConstraintType e : values() ) {
                if ( e.id == id ) {
                    return e;
                }
            }
            throw new UnknownConstraintTypeException( id );
        }


        public static ConstraintType parse( @NonNull String str ) throws UnknownConstraintTypeException {
            if ( str.equalsIgnoreCase( "UNIQUE" ) ) {
                return ConstraintType.UNIQUE;
            }
            throw new UnknownConstraintTypeException( str );
        }
    }


    public enum ForeignKeyOption {
        // IDs according to JDBC standard
        CASCADE( 0 ),
        RESTRICT( 1 ),
        SET_NULL( 2 ),
        SET_DEFAULT( 4 );

        private final int id;


        ForeignKeyOption( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static ForeignKeyOption getById( int id ) throws UnknownForeignKeyOptionException {
            for ( ForeignKeyOption e : values() ) {
                if ( e.id == id ) {
                    return e;
                }
            }
            throw new UnknownForeignKeyOptionException( id );
        }


        public static ForeignKeyOption parse( @NonNull String str ) throws UnknownForeignKeyOptionException {
            if ( str.equalsIgnoreCase( "CASCADE" ) ) {
                return ForeignKeyOption.CASCADE;
            } else if ( str.equalsIgnoreCase( "RESTRICT" ) ) {
                return ForeignKeyOption.RESTRICT;
            } else if ( str.equalsIgnoreCase( "SET NULL" ) ) {
                return ForeignKeyOption.SET_NULL;
            } else if ( str.equalsIgnoreCase( "SET DEFAULT" ) ) {
                return ForeignKeyOption.SET_DEFAULT;
            }
            throw new UnknownForeignKeyOptionException( str );
        }
    }


    public enum PlacementType {
        MANUAL( 1 ),
        AUTOMATIC( 2 );

        private final int id;


        PlacementType( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static PlacementType getById( int id ) throws UnknownPlacementTypeException {
            for ( PlacementType e : values() ) {
                if ( e.id == id ) {
                    return e;
                }
            }
            throw new UnknownPlacementTypeException( id );
        }


        public static PlacementType parse( @NonNull String str ) throws UnknownPlacementTypeException {
            if ( str.equalsIgnoreCase( "MANUAL" ) ) {
                return PlacementType.MANUAL;
            } else if ( str.equalsIgnoreCase( "AUTOMATIC" ) ) {
                return PlacementType.AUTOMATIC;
            }
            throw new UnknownPlacementTypeException( str );
        }


    }


    //HENNLO
    public enum PartitionType {
        NONE( 0 ),
        RANGE( 1 ),
        LIST( 2 ),
        HASH( 3 ),
        ROUNDROBIN( 4 );
        // STREAM, ...

        private final int id;


        PartitionType( int id ) { this.id = id; }

        public int getId() {
            return id;
        }


        //New ExceptionType
        public static PartitionType getById( final int id ) throws UnknownPartitionTypeException {
            for ( PartitionType t : values() ) {
                if ( t.id == id ) {
                    return t;
                }
            }
            throw new UnknownPartitionTypeException( id );
        }


        public static PartitionType getByName( final String name ) throws UnknownPartitionTypeException {
            for ( PartitionType t : values() ) {
                if ( t.name().equalsIgnoreCase( name ) ) {
                    return t;
                }
            }
            throw new UnknownPartitionTypeException( name );
        }

    }


    public static class Pattern {

        public final String pattern;

        public final boolean containsWildcards;


        public Pattern( String pattern ) {
            this.pattern = pattern;
            containsWildcards = pattern.contains( "%" ) || pattern.contains( "_" );
        }


        public String toRegex() {
            return pattern.replace( "_", "(.)" ).replace( "%", "(.*)" );
        }


        @Override
        public String toString() {
            return "Pattern[" + pattern + "]";
        }
    }


    /*
     * Helpers
     */


    public static List<TableType> convertTableTypeList( @NonNull final List<String> stringTypeList ) throws UnknownTableTypeException {
        final List<TableType> typeList = new ArrayList<>( stringTypeList.size() );
        for ( String s : stringTypeList ) {
            typeList.add( TableType.getByName( s ) );
        }
        return typeList;
    }

}

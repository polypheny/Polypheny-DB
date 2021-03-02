/*
 * Copyright 2019-2021 The Polypheny Project
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


import com.google.gson.annotations.SerializedName;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogQueryInterface;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownCollationIdRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintTypeException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintTypeRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyOptionException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyOptionRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownIndexException;
import org.polypheny.db.catalog.exceptions.UnknownIndexTypeException;
import org.polypheny.db.catalog.exceptions.UnknownIndexTypeRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownPlacementTypeException;
import org.polypheny.db.catalog.exceptions.UnknownPlacementTypeRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownQueryInterfaceException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaTypeException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaTypeRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownTableTypeException;
import org.polypheny.db.catalog.exceptions.UnknownTableTypeRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
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
     * Adds a listener which gets notified on updates
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
    public abstract void restoreColumnPlacements( Transaction transaction );


    protected final boolean isValidIdentifier( final String str ) {
        return str.length() <= 100 && str.matches( "^[a-z_][a-z0-9_]*$" ) && !str.isEmpty();
    }


    /**
     * Adds a database
     *
     * @param name The name of the database
     * @param ownerId The owner of this database
     * @param ownerName The name of the owner
     * @param defaultSchemaId The id of the default schema of this database
     * @param defaultSchemaName The name of the default schema of this database
     * @return the id of the newly inserted database
     */
    public abstract long addDatabase( String name, int ownerId, String ownerName, long defaultSchemaId, String defaultSchemaName );

    /**
     * Delete a database from the catalog
     *
     * @param databaseId The id of the database to delete
     */
    public abstract void deleteDatabase( long databaseId );

    /**
     * Get all databases
     *
     * @param pattern A pattern for the database name
     * @return List of databases
     */
    public abstract List<CatalogDatabase> getDatabases( Pattern pattern );

    /**
     * Returns the database with the given name.
     *
     * @param databaseName The name of the database
     * @return The database
     * @throws UnknownDatabaseException If there is no database with this name.
     */
    public abstract CatalogDatabase getDatabase( String databaseName ) throws UnknownDatabaseException;

    /**
     * Returns the database with the given name.
     *
     * @param databaseId The id of the database
     * @return The database
     */
    public abstract CatalogDatabase getDatabase( long databaseId );

    /**
     * Get all schemas which fit to the specified filter pattern.
     * <code>getSchemas(xid, null, null)</code> returns all schemas of all databases.
     *
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogSchema> getSchemas( Pattern databaseNamePattern, Pattern schemaNamePattern );

    /**
     * Get all schemas of the specified database which fit to the specified filter pattern.
     * <code>getSchemas(xid, databaseName, null)</code> returns all schemas of the database.
     *
     * @param databaseId The id of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogSchema> getSchemas( long databaseId, Pattern schemaNamePattern );

    /**
     * Returns the schema with the specified id.
     *
     * @param schemaId The id of the schema
     * @return The schema
     */
    public abstract CatalogSchema getSchema( long schemaId );

    /**
     * Returns the schema with the given name in the specified database.
     *
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @return The schema
     * @throws UnknownSchemaException If there is no schema with this name in the specified database.
     */
    public abstract CatalogSchema getSchema( String databaseName, String schemaName ) throws UnknownSchemaException, UnknownDatabaseException;

    /**
     * Returns the schema with the given name in the specified database.
     *
     * @param databaseId The id of the database
     * @param schemaName The name of the schema
     * @return The schema
     * @throws UnknownSchemaException If there is no schema with this name in the specified database.
     */
    public abstract CatalogSchema getSchema( long databaseId, String schemaName ) throws UnknownSchemaException;

    /**
     * Adds a schema in a specified database
     *
     * @param name The name of the schema
     * @param databaseId The id of the associated database
     * @param ownerId The owner of this schema
     * @param schemaType The type of this schema
     * @return The id of the inserted schema
     */
    public abstract long addSchema( String name, long databaseId, int ownerId, SchemaType schemaType );

    /**
     * Checks weather a schema with the specified name exists in a database.
     *
     * @param databaseId The if of the database
     * @param schemaName The name of the schema to check
     * @return True if there is a schema with this name. False if not.
     */
    public abstract boolean checkIfExistsSchema( long databaseId, String schemaName );

    /**
     * Renames a schema
     *
     * @param schemaId The if of the schema to rename
     * @param name New name of the schema
     */
    public abstract void renameSchema( long schemaId, String name );

    /**
     * Change owner of a schema
     *
     * @param schemaId The if of the schema
     * @param ownerId Id of the new owner
     */
    public abstract void setSchemaOwner( long schemaId, long ownerId );

    /**
     * Delete a schema from the catalog
     *
     * @param schemaId The id of the schema to delete
     */
    public abstract void deleteSchema( long schemaId );

    /**
     * Get all tables of the specified schema which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param schemaId The id of the schema
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogTable> getTables( long schemaId, Pattern tableNamePattern );

    /**
     * Get all tables of the specified database which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param databaseId The id of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogTable> getTables( long databaseId, Pattern schemaNamePattern, Pattern tableNamePattern );

    /**
     * Returns the table with the given name in the specified database and schema.
     *
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @return The table
     */
    public abstract CatalogTable getTable( String databaseName, String schemaName, String tableName ) throws UnknownTableException, UnknownDatabaseException, UnknownSchemaException;

    /**
     * Get all tables of the specified database which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogTable> getTables( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern );

    /**
     * Returns the table with the given id
     *
     * @param tableId The id of the table
     * @return The table
     */
    public abstract CatalogTable getTable( long tableId );

    /**
     * Returns the table with the given name in the specified schema.
     *
     * @param schemaId The id of the schema
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    public abstract CatalogTable getTable( long schemaId, String tableName ) throws UnknownTableException;

    /**
     * Returns the table with the given name in the specified database and schema.
     *
     * @param databaseId The id of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    public abstract CatalogTable getTable( long databaseId, String schemaName, String tableName ) throws UnknownTableException;

    /**
     * Adds a table to a specified schema.
     *
     * @param name The name of the table to add
     * @param schemaId The id of the schema
     * @param ownerId The if of the owner
     * @param tableType The table type
     * @param modifiable Whether the content of the table can be modified
     * @param definition The definition of this table (e.g. a SQL string; null if not applicable)
     * @return The id of the inserted table
     */
    public abstract long addTable( String name, long schemaId, int ownerId, TableType tableType, boolean modifiable, String definition );

    /**
     * Checks if there is a table with the specified name in the specified schema.
     *
     * @param schemaId The id of the schema
     * @param tableName The name to check for
     * @return true if there is a table with this name, false if not.
     */
    public abstract boolean checkIfExistsTable( long schemaId, String tableName );

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
     * @param ownerId Id of the new owner
     */
    public abstract void setTableOwner( long tableId, int ownerId );

    /**
     * Set the primary key of a table
     *
     * @param tableId The id of the table
     * @param keyId The id of the key to set as primary key. Set null to set no primary key.
     */
    public abstract void setPrimaryKey( long tableId, Long keyId );

    /**
     * Adds a placement for a column.
     *
     * @param adapterId The adapter on which the table should be placed on
     * @param columnId The id of the column to be placed
     * @param placementType The type of placement
     * @param physicalSchemaName The schema name on the adapter
     * @param physicalTableName The table name on the adapter
     * @param physicalColumnName The column name on the adapter
     * @param partitionIds List of partitions to place on this column placement (may be null)
     */
    public abstract void addColumnPlacement( int adapterId, long columnId, PlacementType placementType, String physicalSchemaName, String physicalTableName, String physicalColumnName, List<Long> partitionIds );

    /**
     * Deletes a column placement
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     */
    public abstract void deleteColumnPlacement( int adapterId, long columnId );

    /**
     * Get a specific column placement.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @return The specific column placement
     */
    public abstract CatalogColumnPlacement getColumnPlacement( int adapterId, long columnId );

    /**
     * Checks if there is a column with the specified name in the specified table.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @return true if there is a column placement, false if not.
     */
    public abstract boolean checkIfExistsColumnPlacement( int adapterId, long columnId );

    /**
     * Get all column placements of a column
     *
     * @param columnId The id of the specific column
     * @return List of column placements of specific column
     */
    public abstract List<CatalogColumnPlacement> getColumnPlacements( long columnId );

    /**
     * Get column placements of a specific table on a specific adapter
     *
     * @param adapterId The id of the adapter
     * @return List of column placements of the table on the specified adapter
     */
    public abstract List<CatalogColumnPlacement> getColumnPlacementsOnAdapter( int adapterId, long tableId );

    public abstract List<CatalogColumnPlacement> getColumnPlacementsOnAdapterSortedByPhysicalPosition( int storeId, long tableId );

    /**
     * Get column placements on a adapter
     *
     * @param adapterId The id of the adapter
     * @return List of column placements on the specified adapter
     */
    public abstract List<CatalogColumnPlacement> getColumnPlacementsOnAdapter( int adapterId );

    public abstract List<CatalogColumnPlacement> getColumnPlacementsByColumn( long columnId );

    public abstract List<CatalogKey> getKeys();

    public abstract List<CatalogKey> getTableKeys( long tableId );

    /**
     * Get column placements in a specific schema on a specific adapter
     *
     * @param adapterId The id of the adapter
     * @param schemaId The id of the schema
     * @return List of column placements on this adapter and schema
     */
    public abstract List<CatalogColumnPlacement> getColumnPlacementsOnAdapterAndSchema( int adapterId, long schemaId );

    /**
     * Update type of a placement.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @param placementType The new type of placement
     */
    public abstract void updateColumnPlacementType( int adapterId, long columnId, PlacementType placementType );

    /**
     * Update physical position of a column placement on a specified adapter.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @param position The physical position to set
     */
    public abstract void updateColumnPlacementPhysicalPosition( int adapterId, long columnId, long position );

    /**
     * Update physical position of a column placement on a specified adapter. Uses auto-increment to get the globally increasing number.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     */
    public abstract void updateColumnPlacementPhysicalPosition( int adapterId, long columnId );

    /**
     * Change physical names of a placement.
     *
     * @param adapterId The id of the adapter
     * @param columnId The id of the column
     * @param physicalSchemaName The physical schema name
     * @param physicalTableName The physical table name
     * @param physicalColumnName The physical column name
     * @param updatePhysicalColumnPosition Whether to reset the column position (highest number in the table; represents that the column is now at the last position)
     */
    public abstract void updateColumnPlacementPhysicalNames( int adapterId, long columnId, String physicalSchemaName, String physicalTableName, String physicalColumnName, boolean updatePhysicalColumnPosition );

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
     */
    public abstract CatalogColumn getColumn( long columnId );

    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param tableId The id of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     * @throws UnknownColumnException If there is no column with this name in the specified table of the database and schema.
     */
    public abstract CatalogColumn getColumn( long tableId, String columnName ) throws UnknownColumnException;

    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     */
    public abstract CatalogColumn getColumn( String databaseName, String schemaName, String tableName, String columnName ) throws UnknownColumnException, UnknownSchemaException, UnknownDatabaseException, UnknownTableException;

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
     * Check whether a key is a index
     *
     * @param keyId The id of the key
     * @return Whether the key is a index
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


    public abstract List<CatalogIndex> getIndexes( CatalogKey key );

    public abstract List<CatalogIndex> getForeignKeys( CatalogKey key );

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
     * @param location Id of the data store where the index is located (0 for Polypheny-DB itself)
     * @param type The type of index (manual, automatic)
     * @param indexName The name of the index
     * @return The id of the created index
     */
    public abstract long addIndex( long tableId, List<Long> columnIds, boolean unique, String method, String methodDisplayName, int location, IndexType type, String indexName ) throws GenericCatalogException;

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
    public abstract CatalogUser getUser( String userName ) throws UnknownUserException;

    /**
     * Get the user with the specified id.
     *
     * @param userId The id of the user
     * @return The user
     */
    public abstract CatalogUser getUser( int userId );

    /**
     * Get list of all adapters
     *
     * @return List of adapters
     */
    public abstract List<CatalogAdapter> getAdapters();

    /**
     * Get an adapter by its unique name
     *
     * @return The adapter
     */
    public abstract CatalogAdapter getAdapter( String uniqueName ) throws UnknownAdapterException;

    /**
     * Get an adapter by its id
     *
     * @return The adapter
     */
    public abstract CatalogAdapter getAdapter( int adapterId );

    /**
     * Add an adapter
     *
     * @param uniqueName The unique name of the adapter
     * @param clazz The class name of the adapter
     * @param type The type of adapter
     * @param settings The configuration of the adapter
     * @return The id of the newly added adapter
     */
    public abstract int addAdapter( String uniqueName, String clazz, AdapterType type, Map<String, String> settings );

    /**
     * Update settings of an adapter
     *
     * @param adapterId The id of the adapter
     * @param newSettings The new settings for the adapter
     */
    public abstract void updateAdapterSettings( int adapterId, Map<String, String> newSettings );

    /**
     * Delete an adapter
     *
     * @param adapterId The id of the adapter to delete
     */
    public abstract void deleteAdapter( int adapterId );

    /*
     * Get list of all query interfaces
     *
     * @return List of query interfaces
     */
    public abstract List<CatalogQueryInterface> getQueryInterfaces();

    /**
     * Get a query interface by its unique name
     *
     * @param uniqueName The unique name of the query interface
     * @return The CatalogQueryInterface
     */
    public abstract CatalogQueryInterface getQueryInterface( String uniqueName ) throws UnknownQueryInterfaceException;

    /**
     * Get a query interface by its id
     *
     * @param ifaceId The id of the query interface
     * @return The CatalogQueryInterface
     */
    public abstract CatalogQueryInterface getQueryInterface( int ifaceId );

    /**
     * Add a query interface
     *
     * @param uniqueName The unique name of the query interface
     * @param clazz The class name of the query interface
     * @param settings The configuration of the query interface
     * @return The id of the newly added query interface
     */
    public abstract int addQueryInterface( String uniqueName, String clazz, Map<String, String> settings );

    /**
     * Delete a query interface
     *
     * @param ifaceId The id of the query interface to delete
     */
    public abstract void deleteQueryInterface( int ifaceId );

    /**
     * Adds a partition to the catalog
     *
     * @param tableId The unique id of the table
     * @param schemaId The unique id of the table
     * @param ownerId the partitionId to be deleted
     * @param partitionType partition Type of the added partition
     * @return The id of the created partition
     */
    public abstract long addPartition( long tableId, String partitionName, long schemaId, int ownerId, PartitionType partitionType, List<String> effectivePartitionQualifier, boolean isUnbound ) throws GenericCatalogException;

    /**
     * Deletes a single partition and all references.
     *
     * @param tableId The unique id of the table
     * @param schemaId The unique id of the table
     * @param partitionId The partitionId to be deleted
     */
    public abstract void deletePartition( long tableId, long schemaId, long partitionId );

    /**
     * Get a partition object by its unique id
     *
     * @param partitionId The unique id of the partition
     * @return A catalog partition
     */
    public abstract CatalogPartition getPartition( long partitionId );

    /**
     * Effectively partitions a table with the specified partitionType
     *
     * @param tableId Table to be partitioned
     * @param partitionType Partition function to apply on the table
     * @param partitionColumnId Column used to apply the partition function on
     * @param numPartitions Explicit number of partitions
     * @param partitionIds List of ids of the catalog partitions
     */
    public abstract void partitionTable( long tableId, PartitionType partitionType, long partitionColumnId, int numPartitions, List<Long> partitionIds );

    /**
     * Merges a  partitioned table.
     * Resets all objects and structures which were introduced by partitionTable.
     *
     * @param tableId Table to be merged
     */
    public abstract void mergeTable( long tableId );

    /**
     * Get a List of all partitions belonging to a specific table
     *
     * @param tableId Table to be queried
     * @return list of all partitions on this table
     */
    public abstract List<CatalogPartition> getPartitions( long tableId );

    /**
     * Get all partitions of the specified database which fit to the specified filter patterns.
     * <code>getColumns(xid, databaseName, null, null, null)</code> returns all partitions of the database.
     *
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns catalog/src/test/java/org/polypheny/db/test/CatalogTest.javaall.
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    public abstract List<CatalogPartition> getPartitions( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern );

    /**
     * Get a List of all partition name belonging to a specific table
     *
     * @param tableId Table to be queried
     * @return list of all partition names on this table
     */
    public abstract List<String> getPartitionNames( long tableId );

    /**
     * Get placements by partition. Identify the location of partitions.
     * Essentially returns all ColumnPlacements which hold the specified partitionID.
     *
     * @param tableId The id of the table
     * @param partitionId The id of the partition
     * @param columnId The id of tje column
     * @return List of CatalogColumnPlacements
     */
    public abstract List<CatalogColumnPlacement> getColumnPlacementsByPartition( long tableId, long partitionId, long columnId );

    /**
     * Get adapters by partition. Identify the location of partitions/replicas
     * Essentially returns all adapters which hold the specified partitionID
     *
     * @param tableId The unique id of the table
     * @param partitionId The unique id of the partition
     * @return List of CatalogAdapters
     */
    public abstract List<CatalogAdapter> getAdaptersByPartition( long tableId, long partitionId );

    /**
     * Updates the reference which partitions reside on which DataPlacement (identified by adapterId and tableId)
     *
     * @param adapterId The unique id of the adapter
     * @param tableId The unique id of the table
     * @param partitionIds List of partitionsIds to be updated
     */
    public abstract void updatePartitionsOnDataPlacement( int adapterId, long tableId, List<Long> partitionIds );

    /**
     * Get all partitions of a DataPlacement (identified by adapterId and tableId)
     *
     * @param adapterId The unique id of the adapter
     * @param tableId The unique id of the table
     * @return List of partitionIds
     */
    public abstract List<Long> getPartitionsOnDataPlacement( int adapterId, long tableId );

    /**
     * Returns list with the index of the partitions on this store from  0..numPartitions
     *
     * @param adapterId The unique id of the adapter
     * @param tableId The unique id of the table
     * @return List of partitionId Indices
     */
    public abstract List<Long> getPartitionsIndexOnDataPlacement( int adapterId, long tableId );

    /**
     * Mostly needed if a placement is dropped from a store.
     *
     * @param storeId Placement to be updated with new partitions
     * @param tableId List of partitions which the placement should hold
     */
    public abstract void deletePartitionsOnDataPlacement( int storeId, long tableId );

    /**
     * Checks depending on the current partition distribution and partitionType
     * if the distribution would be sufficient. Basically a passthrough method to simplify the code
     *
     * @param adapterId The id of the adapter to be checked
     * @param tableId The id of the table to be checked
     * @param columnId The id of the column to be checked
     * @return If its correctly distributed or not
     */
    public abstract boolean validatePartitionDistribution( int adapterId, long tableId, long columnId );

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


    /*
     *
     */


    public abstract void close();

    public abstract void clear();


    public enum TableType {
        TABLE( 1 ),
        SOURCE( 2 ),
        VIEW( 3 );
        // STREAM, ...

        private final int id;


        TableType( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static TableType getById( final int id ) {
            for ( TableType t : values() ) {
                if ( t.id == id ) {
                    return t;
                }
            }
            throw new UnknownTableTypeRuntimeException( id );
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
        public static class PrimitiveTableType {

            public final String tableType;

        }
    }


    public enum SchemaType {
        @SerializedName("relational")
        RELATIONAL( 1 ),
        @SerializedName("document")
        DOCUMENT( 2 );

        // GRAPH, DOCUMENT, ...

        private final int id;


        SchemaType( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static SchemaType getById( final int id ) {
            for ( SchemaType t : values() ) {
                if ( t.id == id ) {
                    return t;
                }
            }
            throw new UnknownSchemaTypeRuntimeException( id );
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


        public static Collation getById( int id ) {
            for ( Collation c : values() ) {
                if ( c.id == id ) {
                    return c;
                }
            }
            throw new UnknownCollationIdRuntimeException( id );
        }


        public static Collation parse( @NonNull String str ) throws UnknownCollationException {
            if ( str.equalsIgnoreCase( "CASE SENSITIVE" ) ) {
                return Collation.CASE_SENSITIVE;
            } else if ( str.equalsIgnoreCase( "CASE INSENSITIVE" ) ) {
                return Collation.CASE_INSENSITIVE;
            }
            throw new UnknownCollationException( str );
        }


        public static Collation getDefaultCollation() {
            return getById( RuntimeConfig.DEFAULT_COLLATION.getInteger() );
        }
    }


    public enum IndexType {
        MANUAL( 1 ),
        AUTOMATIC( 2 );

        private final int id;


        IndexType( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static Catalog.IndexType getById( int id ) {
            for ( Catalog.IndexType e : values() ) {
                if ( e.id == id ) {
                    return e;
                }
            }
            throw new UnknownIndexTypeRuntimeException( id );
        }


        public static Catalog.IndexType parse( @NonNull String str ) throws UnknownIndexTypeException {
            if ( str.equalsIgnoreCase( "MANUAL" ) ) {
                return Catalog.IndexType.MANUAL;
            } else if ( str.equalsIgnoreCase( "AUTOMATIC" ) ) {
                return Catalog.IndexType.AUTOMATIC;
            }
            throw new UnknownIndexTypeException( str );
        }
    }


    public enum ConstraintType {
        UNIQUE( 1 ),
        PRIMARY( 2 );

        private final int id;


        ConstraintType( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static ConstraintType getById( int id ) {
            for ( ConstraintType e : values() ) {
                if ( e.id == id ) {
                    return e;
                }
            }
            throw new UnknownConstraintTypeRuntimeException( id );
        }


        public static ConstraintType parse( @NonNull String str ) throws UnknownConstraintTypeException {
            if ( str.equalsIgnoreCase( "UNIQUE" ) ) {
                return ConstraintType.UNIQUE;
            }
            throw new UnknownConstraintTypeException( str );
        }
    }


    public enum ForeignKeyOption {
        NONE( -1 ),
        // IDs according to JDBC standard
        //CASCADE( 0 ),
        RESTRICT( 1 );
        //SET_NULL( 2 ),
        //SET_DEFAULT( 4 );

        private final int id;


        ForeignKeyOption( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static ForeignKeyOption getById( int id ) {
            for ( ForeignKeyOption e : values() ) {
                if ( e.id == id ) {
                    return e;
                }
            }
            throw new UnknownForeignKeyOptionRuntimeException( id );
        }


        public static ForeignKeyOption parse( @NonNull String str ) throws UnknownForeignKeyOptionException {
            if ( str.equalsIgnoreCase( "NONE" ) ) {
                return ForeignKeyOption.NONE;
            } else if ( str.equalsIgnoreCase( "RESTRICT" ) ) {
                return ForeignKeyOption.RESTRICT;
            } /*else if ( str.equalsIgnoreCase( "CASCADE" ) ) {
                return ForeignKeyOption.CASCADE;
            } else if ( str.equalsIgnoreCase( "SET NULL" ) ) {
                return ForeignKeyOption.SET_NULL;
            } else if ( str.equalsIgnoreCase( "SET DEFAULT" ) ) {
                return ForeignKeyOption.SET_DEFAULT;
            }*/
            throw new UnknownForeignKeyOptionException( str );
        }
    }


    public enum PlacementType {
        MANUAL( 1 ),
        AUTOMATIC( 2 ),
        STATIC( 3 );

        private final int id;


        PlacementType( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static PlacementType getById( int id ) {
            for ( PlacementType e : values() ) {
                if ( e.id == id ) {
                    return e;
                }
            }
            throw new UnknownPlacementTypeRuntimeException( id );
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


    public enum PartitionType {
        NONE( 0 ),
        RANGE( 1 ),
        LIST( 2 ),
        HASH( 3 );

        private final int id;


        PartitionType( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static PartitionType getById( final int id ) {
            for ( PartitionType t : values() ) {
                if ( t.id == id ) {
                    return t;
                }
            }
            throw new UnknownPartitionTypeRuntimeException( id );
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

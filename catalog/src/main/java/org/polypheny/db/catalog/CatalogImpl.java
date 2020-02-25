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


import com.google.common.collect.ImmutableList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.polypheny.db.PolySqlType;
import org.polypheny.db.UnknownTypeException;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogStore;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedDatabase;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedKey;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedSchema;
import org.polypheny.db.catalog.entity.combined.CatalogCombinedTable;
import org.polypheny.db.catalog.exceptions.CatalogConnectionException;
import org.polypheny.db.catalog.exceptions.CatalogTransactionException;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyException;
import org.polypheny.db.catalog.exceptions.UnknownIndexException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaTypeException;
import org.polypheny.db.catalog.exceptions.UnknownStoreException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownTableTypeException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.transaction.PolyXid;


@Slf4j
public class CatalogImpl extends Catalog {


    private static final String FILE_PATH = "mapDB";
    private static DB db;

    private static HTreeMap<Long, CatalogUser> users;
    private static HTreeMap<Long, CatalogDatabase> databases;
    private static HTreeMap<Long, CatalogSchema> schemas;
    private static HTreeMap<Long, CatalogTable> tables;
    private static HTreeMap<Long, CatalogColumn> columns;

    private static HTreeMap<Long, ImmutableList<Integer>> databaseChildren;
    private static HTreeMap<Long, ImmutableList<Integer>> schemaChildren;
    //private static NavigableSet<Object[]> schemaChildren;
    private static HTreeMap<Long, ImmutableList<Integer>> tableChildren;

    private static final AtomicLong schemaIdBuilder = new AtomicLong();
    private static final AtomicLong databaseIdBuilder = new AtomicLong();
    private static final AtomicLong tableIdBuilder = new AtomicLong();
    private static final AtomicLong columnIdBuilder = new AtomicLong();


    CatalogImpl( PolyXid xid ) {
        super( xid );
        System.out.println( "open catalog" );

        if ( db != null && !db.isClosed() ) {
            return;
        } else if ( db != null ) {
            db.close();
        }

        db = DBMaker
                .fileDB( FILE_PATH )
                .fileMmapEnable()
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .closeOnJvmShutdown()
                .make();

        initDBLayout( db );
    }


    private void initDBLayout( DB db ) {
        users = db.hashMap( "users", Serializer.LONG, new GenericSerializer<CatalogUser>() ).createOrOpen();

        databases = db.hashMap( "databases", Serializer.LONG, new GenericSerializer<CatalogDatabase>() ).createOrOpen();

        schemas = db.hashMap( "schemas", Serializer.LONG, new GenericSerializer<CatalogSchema>() ).createOrOpen();

        tables = db.hashMap( "tables", Serializer.LONG, new GenericSerializer<CatalogTable>() ).createOrOpen();

        columns = db.hashMap( "columns", Serializer.LONG, new GenericSerializer<CatalogColumn>() ).createOrOpen();

        databaseChildren = db.hashMap( "databaseChildren", Serializer.LONG, new GenericSerializer<ImmutableList<Integer>>() ).createOrOpen();

        schemaChildren = db.hashMap( "schemaChildren", Serializer.LONG, new GenericSerializer<ImmutableList<Integer>>() ).createOrOpen();

        tableChildren = db.hashMap( "tableChildren", Serializer.LONG, new GenericSerializer<ImmutableList<Integer>>() ).createOrOpen();

    }

    public void close() {
        db.close();
    }


    /**
     * Get all databases
     *
     * @param pattern A pattern for the database name
     * @return List of databases
     */
    @Override
    public List<CatalogDatabase> getDatabases( Pattern pattern ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getDatabases( transactionHandler, pattern );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the database with the given name.
     *
     * @param databaseName The name of the database
     * @return The database
     * @throws UnknownDatabaseException If there is no database with this name.
     */
    @Override
    public CatalogDatabase getDatabase( String databaseName ) throws GenericCatalogException, UnknownDatabaseException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getDatabase( transactionHandler, databaseName );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the database with the given name.
     *
     * @param databaseId The id of the database
     * @return The database
     * @throws UnknownDatabaseException If there is no database with this name.
     */
    @Override
    public CatalogDatabase getDatabase( long databaseId ) throws GenericCatalogException, UnknownDatabaseException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getDatabase( transactionHandler, databaseId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all schemas which fit to the specified filter pattern.
     * <code>getSchemas(xid, null, null)</code> returns all schemas of all databases.
     *
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogSchema> getSchemas( Pattern databaseNamePattern, Pattern schemaNamePattern ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getSchemas( transactionHandler, databaseNamePattern, schemaNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownSchemaTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all schemas of the specified database which fit to the specified filter pattern.
     * <code>getSchemas(xid, databaseName, null)</code> returns all schemas of the database.
     *
     * @param databaseId The id of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogSchema> getSchemas( long databaseId, Pattern schemaNamePattern ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getSchemas( transactionHandler, databaseId, schemaNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownSchemaTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the schema with the given name in the specified database.
     *
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @return The schema
     * @throws UnknownSchemaException If there is no schema with this name in the specified database.
     */
    @Override
    public CatalogSchema getSchema( String databaseName, String schemaName ) throws GenericCatalogException, UnknownSchemaException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getSchema( transactionHandler, databaseName, schemaName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownSchemaTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the schema with the given name in the specified database.
     *
     * @param databaseId The id of the database
     * @param schemaName The name of the schema
     * @return The schema
     * @throws UnknownSchemaException If there is no schema with this name in the specified database.
     */
    @Override
    public CatalogSchema getSchema( long databaseId, String schemaName ) throws GenericCatalogException, UnknownSchemaException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getSchema( transactionHandler, databaseId, schemaName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownSchemaTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


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
    @Override
    public long addSchema( String name, long databaseId, int ownerId, SchemaType schemaType ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogDatabase database = Statements.getDatabase( transactionHandler, databaseId );
            CatalogUser owner = Statements.getUser( transactionHandler, ownerId );

            long id = schemaIdBuilder.incrementAndGet();
            schemas.put( id, new CatalogSchema( id, name, databaseId, database.name, ownerId, owner.name, schemaType ) );

            return Statements.addSchema( transactionHandler, name, database.id, owner.id, schemaType );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownDatabaseException | UnknownUserException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Checks weather a schema with the specified name exists in a database.
     *
     * @param databaseId The if of the database
     * @param schemaName The name of the schema to check
     * @return True if there is a schema with this name. False if not.
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public boolean checkIfExistsSchema( long databaseId, String schemaName ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogDatabase database = Statements.getDatabase( transactionHandler, databaseId );
            Statements.getSchema( transactionHandler, database.id, schemaName );
            return true;
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownSchemaTypeException | UnknownDatabaseException e ) {
            throw new GenericCatalogException( e );
        } catch ( UnknownSchemaException e ) {
            return false;
        }
    }


    /**
     * Rename a schema
     *
     * @param schemaId The if of the schema to rename
     * @param name New name of the schema
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public void renameSchema( long schemaId, String name ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.renameSchema( transactionHandler, schemaId, name );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Change owner of a schema
     *
     * @param schemaId The if of the schema to rename
     * @param ownerId Id of the new owner
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public void setSchemaOwner( long schemaId, long ownerId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.setSchemaOwner( transactionHandler, schemaId, ownerId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Delete a schema from the catalog
     *
     * @param schemaId The if of the schema to delete
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public void deleteSchema( long schemaId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteSchema( transactionHandler, schemaId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all tables of the specified schema which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param schemaId The id of the schema
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogTable> getTables( long schemaId, Pattern tableNamePattern ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTables( transactionHandler, schemaId, tableNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all tables of the specified database which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param databaseId The id of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogTable> getTables( long databaseId, Pattern schemaNamePattern, Pattern tableNamePattern ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTables( transactionHandler, databaseId, schemaNamePattern, tableNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all tables of the specified database which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogTable> getTables( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTables( transactionHandler, databaseNamePattern, schemaNamePattern, tableNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the table with the given name in the specified schema.
     *
     * @param schemaId The id of the schema
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    @Override
    public CatalogTable getTable( long schemaId, String tableName ) throws UnknownTableException, GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTable( transactionHandler, schemaId, tableName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the table with the given name in the specified database and schema.
     *
     * @param databaseId The id of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    @Override
    public CatalogTable getTable( long databaseId, String schemaName, String tableName ) throws UnknownTableException, GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTable( transactionHandler, databaseId, schemaName, tableName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the table with the given name in the specified database and schema.
     *
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    @Override
    public CatalogTable getTable( String databaseName, String schemaName, String tableName ) throws UnknownTableException, GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTable( transactionHandler, databaseName, schemaName, tableName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


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
    @Override
    public long addTable( String name, long schemaId, int ownerId, TableType tableType, String definition ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogSchema schema = Statements.getSchema( transactionHandler, schemaId );
            CatalogUser owner = Statements.getUser( transactionHandler, ownerId );

            long id = tableIdBuilder.getAndIncrement();
            tables.put( id, new CatalogTable( id, name, schemaId, schema.name, schema.databaseId, schema.databaseName, ownerId, owner.name, tableType, definition, null) );

            return Statements.addTable( transactionHandler, name, schema.id, owner.id, tableType, definition );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownUserException | UnknownSchemaTypeException | UnknownSchemaException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Checks if there is a table with the specified name in the specified schema.
     *
     * @param schemaId The id of the schema
     * @param tableName The name to check for
     * @return true if there is a table with this name, false if not.
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public boolean checkIfExistsTable( long schemaId, String tableName ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogSchema schema = Statements.getSchema( transactionHandler, schemaId );
            Statements.getTable( transactionHandler, schema.id, tableName );
            return true;
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownSchemaTypeException | UnknownTableTypeException | UnknownSchemaException e ) {
            throw new GenericCatalogException( e );
        } catch ( UnknownTableException e ) {
            return false;
        }
    }


    /**
     * Renames a table
     *
     * @param tableId The if of the table to rename
     * @param name New name of the table
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public void renameTable( long tableId, String name ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.renameTable( transactionHandler, tableId, name );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Delete the specified table. Columns, Keys and ColumnPlacements need to be deleted before.
     *
     * @param tableId The id of the table to delete
     */
    @Override
    public void deleteTable( long tableId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteTable( transactionHandler, tableId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Change owner of a table
     *
     * @param tableId The if of the table
     * @param ownerId Id of the new owner
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public void setTableOwner( long tableId, int ownerId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.setTableOwner( transactionHandler, tableId, ownerId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Set the primary key of a table
     *
     * @param tableId The id of the table
     * @param keyId The id of the key to set as primary key. Set null to set no primary key.
     */
    @Override
    public void setPrimaryKey( long tableId, Long keyId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.setPrimaryKey( transactionHandler, tableId, keyId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


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
    @Override
    public void addColumnPlacement( int storeId, long columnId, PlacementType placementType, String physicalSchemaName, String physicalTableName, String physicalColumnName ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogStore store = Statements.getStore( transactionHandler, storeId );
            CatalogColumn column = Statements.getColumn( transactionHandler, columnId );
            Statements.addColumnPlacement( transactionHandler, store.id, column.id, column.tableId, placementType, physicalSchemaName, physicalTableName, physicalColumnName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownStoreException | UnknownCollationException | UnknownTypeException | UnknownColumnException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Deletes a column placement
     *
     * @param storeId The id of the store
     * @param columnId The id of the column
     */
    @Override
    public void deleteColumnPlacement( int storeId, long columnId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteColumnPlacement( transactionHandler, storeId, columnId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get column placements on a store
     *
     * @param storeId The id of the store
     * @return List of column placements on this store
     */
    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnStore( int storeId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumnPlacementsOnStore( transactionHandler, storeId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Change physical names of a placement.
     *
     * @param storeId The id of the store
     * @param columnId The id of the column
     * @param physicalSchemaName The physical schema name
     * @param physicalTableName The physical table name
     * @param physicalColumnName The physical column name
     */
    @Override
    public void updateColumnPlacementPhysicalNames( int storeId, long columnId, String physicalSchemaName, String physicalTableName, String physicalColumnName ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.updateColumnPlacementPhysicalNames( transactionHandler, storeId, columnId, physicalSchemaName, physicalTableName, physicalColumnName );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all columns of the specified table.
     *
     * @param tableId The id of the table
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogColumn> getColumns( long tableId ) throws GenericCatalogException, UnknownCollationException, UnknownTypeException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumns( transactionHandler, tableId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


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
    @Override
    public List<CatalogColumn> getColumns( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern, Pattern columnNamePattern ) throws GenericCatalogException, UnknownCollationException, UnknownTypeException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumns( transactionHandler, databaseNamePattern, schemaNamePattern, tableNamePattern, columnNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the column with the specified id.
     *
     * @param columnId The id of the column
     * @return A CatalogColumn
     * @throws UnknownColumnException If there is no column with this id
     */
    @Override
    public CatalogColumn getColumn( long columnId ) throws UnknownColumnException, GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumn( transactionHandler, columnId );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownCollationException | UnknownTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param tableId The id of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     * @throws UnknownColumnException If there is no column with this name in the specified table of the database and schema.
     */
    @Override
    public CatalogColumn getColumn( long tableId, String columnName ) throws GenericCatalogException, UnknownColumnException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumn( transactionHandler, tableId, columnName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownCollationException | UnknownTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


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
    @Override
    public CatalogColumn getColumn( String databaseName, String schemaName, String tableName, String columnName ) throws GenericCatalogException, UnknownColumnException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumn( transactionHandler, databaseName, schemaName, tableName, columnName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownCollationException | UnknownTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Adds a column.
     *
     * @param name The name of the column
     * @param tableId The id of the corresponding table
     * @param position The ordinal position of the column (starting with 1)
     * @param type The type of the column
     * @param length The length of the field (if applicable, else null)
     * @param scale The number of digits after the decimal point (if applicable)
     * @param nullable Weather the column can contain null values
     * @param collation The collation of the field (if applicable, else null)
     * @return The id of the inserted column
     */
    @Override
    public long addColumn( String name, long tableId, int position, PolySqlType type, Integer length, Integer scale, boolean nullable, Collation collation ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogTable table = Statements.getTable( transactionHandler, tableId );
            if ( type.isCharType() && collation == null ) {
                throw new RuntimeException( "Collation is not allowed to be null for char types." );
            }
            if ( scale != null && scale > length ) {
                throw new RuntimeException( "Invalid scale! Scale can not be larger than length." );
            }
            return Statements.addColumn( transactionHandler, name, table.id, position, type, length, scale, nullable, collation );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException | UnknownTableException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Renames a column
     *
     * @param columnId The if of the column to rename
     * @param name New name of the column
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public void renameColumn( long columnId, String name ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.renameColumn( transactionHandler, columnId, name );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Change move the column to the specified position. Make sure, that there is no other column with this position in the table.
     *
     * @param columnId The id of the column for which to change the position
     * @param position The new position of the column
     */
    @Override
    public void setColumnPosition( long columnId, int position ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.setColumnPosition( transactionHandler, columnId, position );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Change the data type of an column.
     *
     * @param columnId The id of the column
     * @param type The new type of the column
     */
    @Override
    public void setColumnType( long columnId, PolySqlType type, Integer length, Integer scale ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            if ( scale != null && scale > length ) {
                throw new RuntimeException( "Invalid scale! Scale can not be larger than length." );
            }
            Collation collation = type.isCharType() ? Collation.getById( RuntimeConfig.DEFAULT_COLLATION.getInteger() ) : null;
            Statements.setColumnType( transactionHandler, columnId, type, length, scale, collation );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownCollationException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Change nullability of the column (weather the column allows null values).
     *
     * @param columnId The id of the column
     * @param nullable True if the column should allow null values, false if not.
     */
    @Override
    public void setNullable( long columnId, boolean nullable ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );

            if ( nullable ) {
                // Check if the column is part of a primary key (pk's are not allowed to contain null values)
                CatalogColumn catalogColumn = Statements.getColumn( transactionHandler, columnId );
                CatalogTable catalogTable = Statements.getTable( transactionHandler, catalogColumn.tableId );
                if ( catalogTable.primaryKey != null ) {
                    CatalogKey catalogKey = Statements.getPrimaryKey( transactionHandler, catalogTable.primaryKey );
                    if ( catalogKey.columnIds.contains( columnId ) ) {
                        throw new GenericCatalogException( "Unable to allow null values in a column that is part of the primary key." );
                    }
                }
            } else {
                // TODO: Check that the column does not contain any null values
            }

            Statements.setNullable( transactionHandler, columnId, nullable );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownCollationException | UnknownTypeException | UnknownColumnException | UnknownTableTypeException | UnknownTableException | UnknownKeyException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Set the collation of a column.
     * If the column already has the specified collation set, this method is a NoOp.
     *
     * @param columnId The id of the column
     * @param collation The collation to set
     */
    @Override
    public void setCollation( long columnId, Collation collation ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogColumn catalogColumn = Statements.getColumn( transactionHandler, columnId );
            if ( !catalogColumn.type.isCharType() ) {
                throw new RuntimeException( "Illegal attempt to set collation for a non-char column!" );
            }
            Statements.setCollation( transactionHandler, columnId, collation );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownCollationException | UnknownColumnException | UnknownTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Checks if there is a column with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param columnName The name to check for
     * @return true if there is a column with this name, false if not.
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public boolean checkIfExistsColumn( long tableId, String columnName ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogTable table = Statements.getTable( transactionHandler, tableId );
            Statements.getColumn( transactionHandler, table.id, columnName );
            return true;
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownCollationException | UnknownTableTypeException | UnknownTableException | UnknownTypeException e ) {
            throw new GenericCatalogException( e );
        } catch ( UnknownColumnException e ) {
            return false;
        }
    }


    /**
     * Delete the specified column. This also deletes a default value in case there is one defined for this column.
     *
     * @param columnId The id of the column to delete
     */
    @Override
    public void deleteColumn( long columnId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            deleteDefaultValue( columnId );
            Statements.deleteColumn( transactionHandler, columnId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }

    // TODO: String is only a temporary solution


    /**
     * Adds a default value for a column. If there already is a default values, it being replaced.
     *
     * @param columnId The id of the column
     * @param type The type of the default value
     * @param defaultValue The default value
     */
    @Override
    public void setDefaultValue( long columnId, PolySqlType type, String defaultValue ) throws GenericCatalogException {
        try {
            deleteDefaultValue( columnId );
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.setDefaultValue( transactionHandler, columnId, type, defaultValue );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Deletes an existing default value of a column. NoOp if there is no default value defined.
     *
     * @param columnId The id of the column
     */
    @Override
    public void deleteDefaultValue( long columnId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteDefaultValue( transactionHandler, columnId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns a specified primary key
     *
     * @param key The id of the primary key
     * @return The primary key
     */
    @Override
    public CatalogPrimaryKey getPrimaryKey( long key ) throws GenericCatalogException, UnknownKeyException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return new CatalogPrimaryKey( Statements.getPrimaryKey( transactionHandler, key ) );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Adds a primary key to a specified table. If there is already a primary key defined for this table it is replaced.
     *
     * @param tableId The id of the table
     * @param columnIds The id of key which will be part of the primary keys
     */
    @Override
    public void addPrimaryKey( long tableId, List<Long> columnIds ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogTable catalogTable = Statements.getTable( transactionHandler, tableId );

            // Check if the columns are set 'not null'
            for ( Long columnId : columnIds ) {
                CatalogColumn catalogColumn = Statements.getColumn( transactionHandler, columnId );
                if ( catalogColumn.nullable ) {
                    throw new GenericCatalogException( "Primary key is not allowed to contain null values but the column '" + catalogColumn.name + "' is declared nullable." );
                }
            }

            // TODO: Check if the current values are unique

            // Check if there is already a primary key defined for this table and if so, delete it.
            Long oldPrimaryKey = catalogTable.primaryKey;
            if ( oldPrimaryKey != null ) {
                CatalogCombinedKey combinedKey = getCombinedKey( oldPrimaryKey );
                if ( combinedKey.getUniqueCount() == 1 && combinedKey.getReferencedBy().size() > 0 ) {
                    // This primary key is the only constraint for the uniqueness of this key.
                    throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. To drop this primary key, first drop the foreign keys or create a unique constraint." );
                }
                Statements.setPrimaryKey( transactionHandler, tableId, null );
                deleteKeyIfNoLongerUsed( transactionHandler, oldPrimaryKey );
            }
            long keyId = getOrAddKey( transactionHandler, tableId, columnIds );
            Statements.setPrimaryKey( transactionHandler, tableId, keyId );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException | UnknownTableException | UnknownKeyException | UnknownCollationException | UnknownTypeException | UnknownColumnException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns all (imported) foreign keys of a specified table
     *
     * @param tableId The id of the table
     * @return List of foreign keys
     */
    @Override
    public List<CatalogForeignKey> getForeignKeys( long tableId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getForeignKeys( transactionHandler, tableId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns all foreign keys that reference the specified table (exported keys).
     *
     * @param tableId The id of the table
     * @return List of foreign keys
     */
    @Override
    public List<CatalogForeignKey> getExportedKeys( long tableId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getExportedKeys( transactionHandler, tableId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all constraints of the specified table
     *
     * @param tableId The id of the table
     * @return List of constraints
     */
    @Override
    public List<CatalogConstraint> getConstraints( long tableId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getConstraints( transactionHandler, tableId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the constraint with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param constraintName The name of the constraint
     * @return The constraint
     */
    @Override
    public CatalogConstraint getConstraint( long tableId, String constraintName ) throws GenericCatalogException, UnknownConstraintException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getConstraint( transactionHandler, tableId, constraintName );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Return the foreign key with the specified name from the specified table
     *
     * @param tableId The id of the table
     * @param foreignKeyName The name of the foreign key
     * @return The foreign key
     */
    @Override
    public CatalogForeignKey getForeignKey( long tableId, String foreignKeyName ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getForeignKey( transactionHandler, tableId, foreignKeyName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownForeignKeyException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Adds a unique foreign key constraint.
     *
     * @param tableId The id of the table
     * @param columnIds The id of the columns which are part of the foreign key
     * @param referencesIds The id of columns forming the key referenced by this key
     * @param constraintName The name of the constraint
     * @param onUpdate The option for updates
     * @param onDelete The option for deletes
     */
    @Override
    public void addForeignKey( long tableId, List<Long> columnIds, long referencesTableId, List<Long> referencesIds, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            List<CatalogKey> keys = Statements.getKeys( transactionHandler, referencesTableId );
            for ( CatalogKey refKey : keys ) {
                if ( refKey.columnIds.size() == referencesIds.size() && refKey.columnIds.containsAll( referencesIds ) && referencesIds.containsAll( refKey.columnIds ) ) {
                    CatalogCombinedKey combinedKey = getCombinedKey( transactionHandler, refKey.id );
                    // Check if the data type of the referenced column matches the data type of the referencing column
                    int i = 0;
                    for ( CatalogColumn referencedColumn : combinedKey.getColumns() ) {
                        CatalogColumn referencingColumn = Statements.getColumn( transactionHandler, columnIds.get( i++ ) );
                        if ( referencedColumn.type != referencingColumn.type ) {
                            throw new GenericCatalogException( "The data type of the referenced columns does not match the data type of the referencing column: " + referencingColumn.type.name() + " != " + referencedColumn.type );
                        }
                    }
                    if ( combinedKey.getUniqueCount() > 0 ) {
                        long keyId = getOrAddKey( transactionHandler, tableId, columnIds );
                        Statements.addForeignKey( transactionHandler, keyId, refKey.id, constraintName, onUpdate, onDelete );
                        return;
                    }
                }
            }
            throw new GenericCatalogException( "The referenced columns do not define a primary key, unique index or unique constraint." );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownKeyException | UnknownCollationException | UnknownTypeException | UnknownColumnException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Adds a unique constraint.
     *
     * @param tableId The id of the table
     * @param constraintName The name of the constraint
     * @param columnIds A list of column ids
     */
    @Override
    public void addUniqueConstraint( long tableId, String constraintName, List<Long> columnIds ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            long keyId = getOrAddKey( transactionHandler, tableId, columnIds );
            // Check if there is already a unique constraint
            List<CatalogConstraint> constraints = Statements.getConstraintsByKey( transactionHandler, keyId );
            for ( CatalogConstraint constraint : constraints ) {
                if ( constraint.type == ConstraintType.UNIQUE ) {
                    throw new GenericCatalogException( "There is already a unique constraint!" );
                }
            }
            // TODO: Check if the current values are unique
            Statements.addConstraint( transactionHandler, keyId, ConstraintType.UNIQUE, constraintName );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns all indexes of a table
     *
     * @param tableId The id of the table
     * @param onlyUnique true if only indexes for unique values are returned. false if all indexes are returned.
     * @return List of indexes
     */
    @Override
    public List<CatalogIndex> getIndexes( long tableId, boolean onlyUnique ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getIndexes( transactionHandler, tableId, onlyUnique );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the index with the specified name in the specified table
     *
     * @param tableId The id of the table
     * @param indexName The name of the index
     * @return The Index
     */
    @Override
    public CatalogIndex getIndex( long tableId, String indexName ) throws GenericCatalogException, UnknownIndexException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getIndex( transactionHandler, tableId, indexName );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


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
    @Override
    public long addIndex( long tableId, List<Long> columnIds, boolean unique, IndexType type, String indexName ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            long keyId = getOrAddKey( transactionHandler, tableId, columnIds );
            if ( unique ) {
                // TODO: Check if the current values are unique
            }
            return Statements.addIndex( transactionHandler, keyId, type, unique, null, indexName );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Delete the specified index
     *
     * @param indexId The id of the index to drop
     */
    @Override
    public void deleteIndex( long indexId ) throws GenericCatalogException, UnknownIndexException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogIndex index = Statements.getIndex( transactionHandler, indexId );
            if ( index.unique ) {
                CatalogCombinedKey combinedKey = getCombinedKey( index.keyId );
                if ( combinedKey.getUniqueCount() == 1 && combinedKey.getReferencedBy().size() > 0 ) {
                    // This unique index is the only constraint for the uniqueness of this key.
                    throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. To delete this index, first add a unique constraint." );
                }
            }
            Statements.deleteIndex( transactionHandler, index.id );
            deleteKeyIfNoLongerUsed( transactionHandler, index.keyId );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownKeyException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Deletes the specified primary key (including the entry in the key table). If there is an index on this key, make sure to delete it first.
     * If there is no primary key, this operation is a NoOp.
     *
     * @param tableId The id of the key to drop
     */
    @Override
    public void deletePrimaryKey( long tableId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogTable catalogTable = Statements.getTable( transactionHandler, tableId );
            // TODO: Check if the currently stored values are unique
            if ( catalogTable.primaryKey != null ) {
                // Check if this primary key is required to maintain to uniqueness
                CatalogCombinedKey key = getCombinedKey( transactionHandler, catalogTable.primaryKey );
                if ( key.getReferencedBy().size() > 0 ) {
                    if ( key.getUniqueCount() < 2 ) {
                        throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. To drop this primary key either drop the foreign key or create an unique constraint." );
                    }
                }

                Statements.setPrimaryKey( transactionHandler, tableId, null );
                deleteKeyIfNoLongerUsed( transactionHandler, catalogTable.primaryKey );
            }
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException | UnknownTableException | UnknownKeyException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Delete the specified foreign key (does not delete the referenced key).
     *
     * @param foreignKeyId The id of the foreign key to delete
     */
    @Override
    public void deleteForeignKey( long foreignKeyId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogForeignKey catalogForeignKey = Statements.getForeignKey( transactionHandler, foreignKeyId );
            CatalogCombinedKey key = getCombinedKey( transactionHandler, catalogForeignKey.id );
            Statements.deleteForeignKey( transactionHandler, key.getKey().id );
            deleteKeyIfNoLongerUsed( transactionHandler, key.getKey().id );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownKeyException | UnknownForeignKeyException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Delete the specified constraint.
     * For deleting foreign keys, use {@link #deleteForeignKey(long)}.
     *
     * @param constraintId The id of the constraint to delete
     */
    @Override
    public void deleteConstraint( long constraintId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogConstraint catalogConstraint = Statements.getConstraint( transactionHandler, constraintId );
            CatalogCombinedKey key = getCombinedKey( transactionHandler, catalogConstraint.keyId );
            if ( catalogConstraint.type == ConstraintType.UNIQUE && key.getReferencedBy().size() > 0 ) {
                if ( key.getUniqueCount() < 2 ) {
                    throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. Unable to drop unique constraint." );
                }
            }
            Statements.deleteConstraint( transactionHandler, catalogConstraint.id );
            deleteKeyIfNoLongerUsed( transactionHandler, key.getKey().id );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownKeyException | UnknownConstraintException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get the user with the specified name
     *
     * @param userName The name of the user
     * @return The user
     * @throws UnknownUserException If there is no user with the specified name
     */
    @Override
    public CatalogUser getUser( String userName ) throws UnknownUserException, GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getTransactionHandler( xid );
            return Statements.getUser( transactionHandler, userName );
        } catch ( GenericCatalogException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get list of all stores
     *
     * @return List of stores
     */
    @Override
    public List<CatalogStore> getStores() throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getStores( transactionHandler );
        } catch ( GenericCatalogException | CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get a store by its unique name
     *
     * @return List of stores
     */
    @Override
    public CatalogStore getStore( String uniqueName ) throws GenericCatalogException {
        uniqueName = uniqueName.toLowerCase();
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getStore( transactionHandler, uniqueName );
        } catch ( GenericCatalogException | UnknownStoreException | CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Add a store
     *
     * @param uniqueName The unique name of the store
     * @param adapter The class name of the adapter
     * @param settings The configuration of the store
     * @return The id of the newly added store
     */
    @Override
    public int addStore( String uniqueName, String adapter, Map<String, String> settings ) throws GenericCatalogException {
        uniqueName = uniqueName.toLowerCase();
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.addStore( transactionHandler, uniqueName, adapter, settings );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Delete a store
     *
     * @param storeId The id of the store to delete
     */
    @Override
    public void deleteStore( int storeId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteStore( transactionHandler, storeId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public CatalogCombinedDatabase getCombinedDatabase( long databaseId ) throws GenericCatalogException, UnknownSchemaException, UnknownTableException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return getCombinedDatabase( transactionHandler, databaseId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }

    }


    private CatalogCombinedDatabase getCombinedDatabase( XATransactionHandler transactionHandler, long databaseId ) throws GenericCatalogException, UnknownSchemaException, UnknownTableException {
        try {
            CatalogDatabase database = Statements.getDatabase( transactionHandler, databaseId );
            List<CatalogSchema> schemas = Statements.getSchemas( transactionHandler, databaseId, null );
            List<CatalogCombinedSchema> combinedSchemas = new LinkedList<>();
            for ( CatalogSchema schema : schemas ) {
                combinedSchemas.add( getCombinedSchema( transactionHandler, schema.id ) );
            }
            CatalogSchema defaultSchema = null;
            if ( database.defaultSchemaId != null ) {
                defaultSchema = Statements.getSchema( transactionHandler, database.defaultSchemaId );
            }
            CatalogUser owner = Statements.getUser( transactionHandler, database.ownerId );
            return new CatalogCombinedDatabase( database, combinedSchemas, defaultSchema, owner );
        } catch ( UnknownSchemaTypeException | UnknownDatabaseException | UnknownUserException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public CatalogCombinedSchema getCombinedSchema( long schemaId ) throws GenericCatalogException, UnknownSchemaException, UnknownTableException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return getCombinedSchema( transactionHandler, schemaId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }

    }


    private CatalogCombinedSchema getCombinedSchema( XATransactionHandler transactionHandler, long schemaId ) throws GenericCatalogException, UnknownSchemaException, UnknownTableException {
        try {
            CatalogSchema schema = Statements.getSchema( transactionHandler, schemaId );
            List<CatalogTable> tables = Statements.getTables( transactionHandler, schemaId, null );
            List<CatalogCombinedTable> combinedTables = new LinkedList<>();
            for ( CatalogTable table : tables ) {
                combinedTables.add( getCombinedTable( transactionHandler, table.id ) );
            }
            CatalogDatabase database = Statements.getDatabase( transactionHandler, schema.databaseId );
            CatalogUser owner = Statements.getUser( transactionHandler, schema.ownerId );
            return new CatalogCombinedSchema( schema, combinedTables, database, owner );
        } catch ( UnknownTableTypeException | UnknownSchemaTypeException | UnknownDatabaseException | UnknownUserException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public CatalogCombinedTable getCombinedTable( long tableId ) throws GenericCatalogException, UnknownTableException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return getCombinedTable( transactionHandler, tableId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    private CatalogCombinedTable getCombinedTable( XATransactionHandler transactionHandler, long tableId ) throws GenericCatalogException, UnknownTableException {
        try {
            CatalogTable table = Statements.getTable( transactionHandler, tableId );
            List<CatalogColumn> columns = Statements.getColumns( transactionHandler, tableId );
            CatalogSchema schema = Statements.getSchema( transactionHandler, table.schemaId );
            CatalogDatabase database = Statements.getDatabase( transactionHandler, schema.databaseId );
            CatalogUser owner = Statements.getUser( transactionHandler, table.ownerId );

            Map<Integer, List<CatalogColumnPlacement>> columnPlacementsByStore = new LinkedHashMap<>();
            Map<Long, List<CatalogColumnPlacement>> columnPlacementsByColumn = new LinkedHashMap<>();
            // We know the columns
            for ( CatalogColumn catalogColumn : columns ) {
                columnPlacementsByColumn.put( catalogColumn.id, new LinkedList<>() );
            }
            for ( CatalogColumnPlacement p : Statements.getColumnPlacementsOfTable( transactionHandler, tableId ) ) {
                columnPlacementsByColumn.get( p.columnId ).add( p );
                if ( !columnPlacementsByStore.containsKey( p.storeId ) ) {
                    columnPlacementsByStore.put( p.storeId, new LinkedList<>() );
                }
                columnPlacementsByStore.get( p.storeId ).add( p );
            }

            List<CatalogKey> keys = Statements.getKeys( transactionHandler, tableId );
            return new CatalogCombinedTable( table, columns, schema, database, owner, columnPlacementsByStore, columnPlacementsByColumn, keys );
        } catch ( UnknownCollationException | UnknownTypeException | UnknownTableTypeException | UnknownSchemaTypeException | UnknownSchemaException | UnknownDatabaseException | UnknownUserException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public CatalogCombinedKey getCombinedKey( long keyId ) throws GenericCatalogException, UnknownKeyException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return getCombinedKey( transactionHandler, keyId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    private CatalogCombinedKey getCombinedKey( XATransactionHandler transactionHandler, long keyId ) throws GenericCatalogException, UnknownKeyException {
        try {
            CatalogKey key = Statements.getKey( transactionHandler, keyId );

            List<CatalogColumn> columns = new LinkedList<>();
            for ( long columnId : key.columnIds ) {
                columns.add( Statements.getColumn( transactionHandler, columnId ) );
            }

            CatalogTable table = Statements.getTable( transactionHandler, key.tableId );
            CatalogSchema schema = Statements.getSchema( transactionHandler, table.schemaId );
            CatalogDatabase database = Statements.getDatabase( transactionHandler, schema.databaseId );

            List<CatalogForeignKey> foreignKeys = Statements.getForeignKeysByReference( transactionHandler, keyId );
            List<CatalogIndex> indexes = Statements.getIndexesByKey( transactionHandler, keyId );
            List<CatalogConstraint> constraints = Statements.getConstraintsByKey( transactionHandler, keyId );

            List<CatalogForeignKey> referencedBy = Statements.getForeignKeysByReference( transactionHandler, keyId );

            return new CatalogCombinedKey( key, columns, table, schema, database, foreignKeys, indexes, constraints, referencedBy );
        } catch ( UnknownCollationException | UnknownTypeException | UnknownTableTypeException | UnknownSchemaTypeException | UnknownSchemaException | UnknownDatabaseException | UnknownColumnException | UnknownTableException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public boolean prepare() throws CatalogTransactionException {
        val transactionHandler = XATransactionHandler.getTransactionHandler( xid );
        if ( XATransactionHandler.hasTransactionHandler( xid ) ) {
            return transactionHandler.prepare();
        } else {
            // e.g. SELECT 1; commit;
            log.debug( "Unknown transaction handler. This is not necessarily a problem as long as the query has not initiated any catalog lookups." );
            return true;
        }
    }


    @Override
    public void commit() throws CatalogTransactionException {
        val transactionHandler = XATransactionHandler.getTransactionHandler( xid );
        if ( XATransactionHandler.hasTransactionHandler( xid ) ) {
            transactionHandler.commit();
        } else {
            // e.g. SELECT 1; commit;
            log.debug( "Unknown transaction handler. This is not necessarily a problem as long as the query has not initiated any catalog lookups." );
        }
        CatalogManagerImpl.getInstance().removeCatalog( xid );
    }


    @Override
    public void rollback() throws CatalogTransactionException {
        val transactionHandler = XATransactionHandler.getTransactionHandler( xid );
        if ( XATransactionHandler.hasTransactionHandler( xid ) ) {
            transactionHandler.rollback();
        } else {
            // e.g. SELECT 1; commit;
            log.debug( "Unknown transaction handler. This is not necessarily a problem as long as the query has not initiated any catalog lookups." );
        }
        CatalogManagerImpl.getInstance().removeCatalog( xid );
    }


    // Check if the specified key is used as primary key, index or constraint. If so, this is a NoOp. If it is not used, the key is deleted.
    private void deleteKeyIfNoLongerUsed( XATransactionHandler transactionHandler, Long keyId ) throws GenericCatalogException, UnknownKeyException {
        CatalogCombinedKey combinedKey = getCombinedKey( transactionHandler, keyId );
        if ( combinedKey.isPrimaryKey() ) {
            return;
        }
        if ( combinedKey.getConstraints().size() > 0 ) {
            return;
        }
        if ( combinedKey.getForeignKeys().size() > 0 ) {
            return;
        }
        if ( combinedKey.getIndexes().size() > 0 ) {
            return;
        }
        // This key is not used anymore. Delete it.
        Statements.deleteKey( transactionHandler, keyId );
    }


    // Returns the id of they defined by the specified column ids. If this key does not yet exist, create it.
    private long getOrAddKey( XATransactionHandler transactionHandler, long tableId, List<Long> columnIds ) throws GenericCatalogException {
        long keyId = -1;
        // Check if there is already a key
        List<CatalogKey> keys = Statements.getKeys( transactionHandler, tableId );
        for ( CatalogKey key : keys ) {
            if ( key.columnIds.size() == columnIds.size() && key.columnIds.containsAll( columnIds ) && columnIds.containsAll( key.columnIds ) ) {
                keyId = key.id;
            }
        }
        if ( keyId == -1 ) {
            // Key does not exist, create it
            keyId = Statements.addKey( transactionHandler, tableId, columnIds );
        }
        return keyId;
    }


}

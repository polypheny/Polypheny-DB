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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;
import org.mapdb.serializer.SerializerLongArray;
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
import org.polypheny.db.catalog.exceptions.UnknownIndexException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownTableTypeException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;

// TODO remove from all substructures


@Slf4j
public class CatalogImpl extends Catalog {


    private static final String FILE_PATH = "mapDB";
    private static DB db;

    private static HTreeMap<Integer, CatalogUser> users;
    private static HTreeMap<Long, CatalogDatabase> databases;
    private static HTreeMap<String, CatalogDatabase> databaseNames;
    private static HTreeMap<Long, CatalogSchema> schemas;
    private static HTreeMap<Long, CatalogTable> tables;
    private static HTreeMap<Long, CatalogColumn> columns;
    private static HTreeMap<Integer, CatalogStore> stores;

    private static HTreeMap<Long, ImmutableList<Long>> databaseChildren;
    // identifier is long array with databaseId, schemaId etc. -> faster lookup
    private static HTreeMap<Long, ImmutableList<Long>> schemaChildren;
    private static HTreeMap<Long, ImmutableList<Long>> tableChildren;


    private static final AtomicLong schemaIdBuilder = new AtomicLong();
    private static final AtomicLong databaseIdBuilder = new AtomicLong();
    private static final AtomicLong tableIdBuilder = new AtomicLong();
    private static final AtomicLong columnIdBuilder = new AtomicLong();
    private static final AtomicInteger userIdBuilder = new AtomicInteger();
    private static final AtomicLong keyIdBuilder = new AtomicLong();
    private static final AtomicLong constraintIdBuilder = new AtomicLong();
    private static final AtomicLong indexIdBuilder = new AtomicLong();
    private static final AtomicInteger storeIdBuilder = new AtomicInteger();

    // qualified name with database prefixed e.g. [database].[schema].[table].[column]
    private static BTreeMap<Object[], CatalogSchema> schemaNames;
    private static BTreeMap<Object[], Long> tableNames;
    private static BTreeMap<Object[], Long> columnNames;
    private static HTreeMap<String, CatalogUser> userNames;
    private static HTreeMap<String, Integer> storeNames;
    private static BTreeMap<long[], CatalogColumnPlacement> columnPlacement;
    private static HTreeMap<Long, CatalogPrimaryKey> primaryKeys;
    private static HTreeMap<Long, CatalogKey> keys;
    private static HTreeMap<Long, CatalogForeignKey> foreignKeys;
    private static HTreeMap<Long, CatalogConstraint> constraints;
    private static HTreeMap<Long, CatalogIndex> indices;


    public CatalogImpl() {
        this( FILE_PATH );
    }


    /**
     * MapDB Catalog; idea is to only need a minimal amount( max 2-3 ) map lookups for each get
     * most maps should work with ids to prevent overhead when renaming
     */
    public CatalogImpl( String path ) {
        super();

        if ( db != null && !db.isClosed() ) {
            return;
        } else if ( db != null ) {
            db.close();
        }
        synchronized ( this ) {

            db = DBMaker
                    .fileDB( path )
                    .closeOnJvmShutdown()
                    .checksumHeaderBypass() // TODO clean shutdown needed
                    .fileMmapEnable()
                    .fileMmapEnableIfSupported()
                    .fileMmapPreclearDisable()
                    .make();

            initDBLayout( db );

            // mirrors default data from old sql file
            try {
                insertDefaultData();
                restoreAllIdBuilders();
            } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException e ) {
                e.printStackTrace();
            }

        }
    }


    /**
     * Initializes the default catalog layout
     *
     * @param db the databases object on which the layout is created
     */
    private void initDBLayout( DB db ) {
        initUserInfo( db );
        initDatabaseInfo( db );
        initSchemaInfo( db );
        initTableInfo( db );
        initColumnInfo( db );
        initKeysAndConstraintsInfo( db );
        initStoreInfo( db );

    }


    private void restoreIdBuilder( Map<Integer, ?> map, AtomicInteger idBuilder ) {
        if ( !map.isEmpty() ) {
            idBuilder.set( Collections.max( map.keySet() ) + 1 );
        }
    }


    private void restoreIdBuilder( Map<Long, ?> map, AtomicLong idBuilder ) {
        if ( !map.isEmpty() ) {
            idBuilder.set( Collections.max( map.keySet() ) + 1 );
        }
    }


    private void restoreAllIdBuilders() {
        restoreIdBuilder( schemas, schemaIdBuilder );
        restoreIdBuilder( databases, databaseIdBuilder );
        restoreIdBuilder( tables, tableIdBuilder );
        restoreIdBuilder( columns, columnIdBuilder );
        restoreIdBuilder( users, userIdBuilder );
        restoreIdBuilder( keys, keyIdBuilder );
        restoreIdBuilder( constraints, columnIdBuilder );
        restoreIdBuilder( indices, indexIdBuilder );
        restoreIdBuilder( stores, storeIdBuilder );

    }


    private void initStoreInfo( DB db ) {
        stores = db.hashMap( "stores", Serializer.INTEGER, new GenericSerializer<CatalogStore>() ).createOrOpen();
        storeNames = db.hashMap( "storeNames", Serializer.STRING, Serializer.INTEGER ).createOrOpen();
    }


    private void initKeysAndConstraintsInfo( DB db ) {
        keys = db.hashMap( "keys", Serializer.LONG, new GenericSerializer<CatalogKey>() ).createOrOpen();
        primaryKeys = db.hashMap( "primaryKeys", Serializer.LONG, new GenericSerializer<CatalogPrimaryKey>() ).createOrOpen();
        foreignKeys = db.hashMap( "foreignKeys", Serializer.LONG, new GenericSerializer<CatalogForeignKey>() ).createOrOpen();
        constraints = db.hashMap( "constraints", Serializer.LONG, new GenericSerializer<CatalogConstraint>() ).createOrOpen();
        indices = db.hashMap( "indices", Serializer.LONG, new GenericSerializer<CatalogIndex>() ).createOrOpen();
    }


    /**
     * Fills the catalog database with default data, skips if data is already inserted
     */
    private void insertDefaultData() throws GenericCatalogException, UnknownUserException, UnknownDatabaseException {
        //////////////
        // init users
        Integer systemId = null;
        if ( !userNames.containsKey( "system" ) ) {
            systemId = addUser( "system", "" );
        }

        if ( !userNames.containsKey( "pa" ) ) {
            int ownerId = addUser( "pa", "" );
        }

        //////////////
        // init database
        Long databaseId = null;
        if ( !databaseNames.containsKey( "APP" ) ) {
            if ( systemId == null ) {
                systemId = getUser( "system" ).id;
            }

            databaseId = addDatabase( "APP", systemId, "system", 0L, "public" );
        }

        if ( databaseId == null ) {
            databaseId = getDatabase( "APP" ).id;
        }

        //////////////
        // init schema

        Long schemaId = null;
        if ( !schemaNames.containsKey( new Object[]{ databaseId, "public" } ) ) {

            schemaId = addSchema( "public", databaseId, 0, SchemaType.RELATIONAL );
        }

        if ( schemaId == null ) {
            schemaId = getDatabase( "APP" ).id;
        }

        //////////////
        // init schema
        if ( !tableNames.containsKey( new Object[]{ databaseId, schemaId, "depts" } ) ) {
            addTable( "depts", 0, 0, TableType.TABLE, null );
        }
        if ( !tableNames.containsKey( new Object[]{ databaseId, schemaId, "emps" } ) ) {
            addTable( "emps", 0, 0, TableType.TABLE, null );
        }

        //////////////
        // init store
        // TODO refactor
        if ( !storeNames.containsKey( "hsqldb" ) ) {
            Map<String, String> hsqldbSettings = new HashMap<>();
            hsqldbSettings.put( "type", "Memory" );
            hsqldbSettings.put( "path", "maxConnections" );
            hsqldbSettings.put( "maxConnections", "25" );
            hsqldbSettings.put( "trxControlMode", "mvcc" );
            hsqldbSettings.put( "trxIsolationLevel", "read_committed" );

            addStore( "hsqldb", "org.polypheny.db.adapter.jdbc.stores.HsqldbStore", hsqldbSettings );
        }

        if ( !storeNames.containsKey( "csv" ) ) {
            Map<String, String> csvSetttings = new HashMap<>();
            csvSetttings.put( "directory", "classpath://hr" );

            addStore( "csv", "org.polypheny.db.adapter.csv.CsvStore", csvSetttings );
        }
    }


    private void initUserInfo( DB db ) {
        users = db.hashMap( "users", Serializer.INTEGER, new GenericSerializer<CatalogUser>() ).createOrOpen();
        userNames = db.hashMap( "usersNames", Serializer.STRING, new GenericSerializer<CatalogUser>() ).createOrOpen();
    }


    /**
     * initialize the column maps
     * "columns" holds all CatalogColumn objects, access by id
     * "columnNames" holds the id, which can be access by String[], which consist of databaseName, schemaName, tableName, columnName
     * "columnPlacements" holds the columnPlacement accessed by long[], which consist of storeId and columnPlacementId
     *
     * @param db the mapdb database object on which the maps are generated from
     */
    private void initColumnInfo( DB db ) {
        columns = db.hashMap( "columns", Serializer.LONG, new GenericSerializer<CatalogColumn>() ).createOrOpen();
        columnNames = db.treeMap( "columnNames", new SerializerArrayTuple( Serializer.LONG, Serializer.LONG, Serializer.LONG, Serializer.STRING ), Serializer.LONG ).createOrOpen();

        columnPlacement = db.treeMap( "columnPlacement", new SerializerLongArray(), Serializer.JAVA ).createOrOpen();
    }


    private void initTableInfo( DB db ) {
        tables = db.hashMap( "tables", Serializer.LONG, new GenericSerializer<CatalogTable>() ).createOrOpen();
        tableChildren = db.hashMap( "tableChildren", Serializer.LONG, new GenericSerializer<ImmutableList<Long>>() ).createOrOpen();
        tableNames = db.treeMap( "tableNames", new SerializerArrayTuple( Serializer.LONG, Serializer.LONG, Serializer.STRING ), Serializer.LONG ).createOrOpen();
    }


    private void initSchemaInfo( DB db ) {
        schemas = db.hashMap( "schemas", Serializer.LONG, new GenericSerializer<CatalogSchema>() ).createOrOpen();
        schemaChildren = db.hashMap( "schemaChildren", Serializer.LONG, new GenericSerializer<ImmutableList<Long>>() ).createOrOpen();
        schemaNames = db.treeMap( "schemaNames", new SerializerArrayTuple( Serializer.LONG, Serializer.STRING ), Serializer.JAVA ).createOrOpen();
    }


    private void initDatabaseInfo( DB db ) {
        databases = db.hashMap( "databases", Serializer.LONG, new GenericSerializer<CatalogDatabase>() ).createOrOpen();
        databaseNames = db.hashMap( "databaseNames", Serializer.STRING, new GenericSerializer<CatalogDatabase>() ).createOrOpen();
        databaseChildren = db.hashMap( "databaseChildren", Serializer.LONG, new GenericSerializer<ImmutableList<Long>>() ).createOrOpen();
    }


    @Override
    public void close() {
        db.close();
    }

    @Override
    public void clear(){
        db.getAll().clear();
    }


    /**
     * Inserts a new database,
     * if a database with the same name already exists, it throws an error // TODO should it?
     *
     * @return the id of the newly inserted database
     */
    public long addDatabase( String name, int ownerId, String ownerName, long defaultSchemaId, String defaultSchemaName ) {
        long id = databaseIdBuilder.getAndIncrement();
        CatalogDatabase database = new CatalogDatabase( id, name, ownerId, ownerName, defaultSchemaId, defaultSchemaName );
        databases.put( id, database );
        databaseNames.put( name, database );
        databaseChildren.put( id, ImmutableList.<Long>builder().build() );
        return id;
    }


    /**
     * Inserts a new user,
     * if a user with the same name already exists, it throws an error // TODO should it?
     *
     * @param name of the user
     * @param password of the user
     * @return the id of the created user
     */
    public int addUser( String name, String password ) {
        CatalogUser user = new CatalogUser( userIdBuilder.getAndIncrement(), name, password );
        users.put( user.id, user );
        userNames.put( user.name, user );
        return user.id;
    }


    /**
     * Get all databases
     * if pattern is specified, only the ones which confirm to it
     *
     * @param pattern A pattern for the database name
     * @return List of databases
     */
    @Override
    public List<CatalogDatabase> getDatabases( Pattern pattern ) {

        if ( pattern == null ) {
            return new ArrayList<>( databases.values() );
        } else {
            if ( databaseNames.containsKey( pattern.pattern ) ) {
                return Collections.singletonList( databases.get( databaseNames.get( pattern.pattern ) ) );
            }
            return new ArrayList<>();
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
    public CatalogDatabase getDatabase( String databaseName ) throws UnknownDatabaseException {
        if ( databaseNames.containsKey( databaseName ) ) {
            return databases.get( databaseNames.get( databaseName ) );
        }
        throw new UnknownDatabaseException( databaseName );
    }


    /**
     * Returns the database with the given name.
     *
     * @param databaseId The id of the database
     * @return The database
     * @throws UnknownDatabaseException If there is no database with this name.
     */
    @Override
    public CatalogDatabase getDatabase( long databaseId ) throws UnknownDatabaseException {
        if ( databases.containsKey( databaseId ) ) {
            return databases.get( databaseId );
        }
        throw new UnknownDatabaseException( databaseId );

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
    public List<CatalogSchema> getSchemas( Pattern databaseNamePattern, Pattern schemaNamePattern ) {
        if ( databaseNamePattern != null && schemaNamePattern != null ) {
            try {
                CatalogSchema schema = getSchema( databaseNamePattern.pattern, schemaNamePattern.pattern );
                return Collections.singletonList( schema );

            } catch ( UnknownSchemaException e ) {
                log.error( Arrays.toString( e.getStackTrace() ) );
            }

            return new ArrayList<>();

        } else if ( databaseNamePattern != null ) {
            try {
                if ( databaseNames.containsKey( databaseNamePattern.pattern ) ) {
                    long id = databaseNames.get( databaseNamePattern.pattern );
                    return getSchemas( id );
                }


            } catch ( UnknownSchemaException e ) {
                e.printStackTrace();
            }
            return new ArrayList<>();

        } else if ( schemaNamePattern != null ) {
            return schemas.values().stream().filter( e -> e.name.equals( schemaNamePattern.pattern ) ).collect( Collectors.toList() );
        } else {
            return new ArrayList<>( schemas.values() );
        }
    }


    public List<CatalogSchema> getSchemas( long databaseId ) throws UnknownSchemaException {
        if ( schemaChildren.containsKey( databaseId ) ) {
            List<Long> children = databaseChildren.get( databaseId );
            if ( children != null ) {

                List<CatalogSchema> list = new ArrayList<>();
                for ( Long child : children ) {
                    CatalogSchema schema = getSchema( child );
                    if ( schema != null ) {
                        list.add( schema );
                    }
                }
                return list;
            }
        }
        throw new UnknownSchemaException( databaseId );

    }

    // TODO remove? not used


    /**
     * Get all schemas of the specified database which fit to the specified filter pattern.
     * <code>getSchemas(xid, databaseName, null)</code> returns all schemas of the database.
     *
     * @param databaseId The id of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogSchema> getSchemas( long databaseId, Pattern schemaNamePattern ) throws UnknownSchemaException {
        if ( schemaNamePattern != null ) {

            Long id = schemaNames.get( new Object[]{ databaseId, schemaNamePattern.pattern } );
            if ( id != null && schemas.containsKey( id ) ) {
                return Collections.singletonList( schemas.get( id ) );
            }
            throw new UnknownSchemaException( databaseId );

        }
        return schemaNames.prefixSubMap( new Object[]{ databaseId } ).values().stream().map( schemas::get ).collect( Collectors.toList() );

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
    public CatalogSchema getSchema( String databaseName, String schemaName ) throws UnknownSchemaException {
        if ( databaseNames.containsKey( databaseName ) ) {
            Long databaseId = databaseNames.get( databaseName );
            if ( databaseId != null ) {

                Long schemaId = schemaNames.get( new Object[]{ databaseId, schemaName } );
                if ( schemaId != null && schemas.containsKey( schemaId ) ) {
                    return schemas.get( schemaId );
                }
            }
        }
        throw new UnknownSchemaException( databaseName, schemaName );
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
    public CatalogSchema getSchema( long databaseId, String schemaName ) throws UnknownSchemaException {
        try {
            return schemas.get( schemaNames.get( new Object[]{ databaseId, schemaName } ) );
        } catch ( NullPointerException e ) {
            throw new UnknownSchemaException( databaseId, schemaName );
        }
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getSchema( transactionHandler, databaseId, schemaName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownSchemaTypeException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    public CatalogSchema getSchema( long schemaId ) throws UnknownSchemaException {
        if ( schemas.containsKey( schemaId ) ) {
            return schemas.get( schemaId );
        }
        throw new UnknownSchemaException( schemaId );
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
        CatalogDatabase database = databases.get( databaseId );
        // TODO long or int for user
        CatalogUser owner = users.get( ownerId );
        long id = schemaIdBuilder.getAndIncrement();
        schemas.put( id, new CatalogSchema( id, name, databaseId, database.name, ownerId, owner.name, schemaType ) );
        schemaNames.put( new Object[]{ databaseId, name }, id );
        schemaChildren.put( id, ImmutableList.<Long>builder().build() );
        List<Long> children = new ArrayList<>( databaseChildren.get( databaseId ) );
        children.add( id );
        databaseChildren.replace( databaseId, ImmutableList.copyOf( children ) );
        return id;

        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogDatabase database = Statements.getDatabase( transactionHandler, databaseId );
            CatalogUser owner = Statements.getUser( transactionHandler, ownerId );

            return Statements.addSchema( transactionHandler, name, database.id, owner.id, schemaType );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownDatabaseException | UnknownUserException e ) {
            throw new GenericCatalogException( e );
        }*/
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
        return schemaNames.get( new Object[]{ databaseId, schemaName } ) != null;
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogDatabase database = Statements.getDatabase( transactionHandler, databaseId );
            Statements.getSchema( transactionHandler, database.id, schemaName );
            return true;
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownSchemaTypeException | UnknownDatabaseException e ) {
            throw new GenericCatalogException( e );
        } catch ( UnknownSchemaException e ) {
            return false;
        }*/
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

        CatalogSchema old = schemas.get( schemaId );
        CatalogSchema schema = CatalogSchema.rename( old, name );

        schemas.replace( schemaId, schema );
        schemaNames.remove( new Object[]{ old.databaseId, old.name } );
        schemaNames.put( new Object[]{ old.databaseId, name }, schemaId );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.renameSchema( transactionHandler, schemaId, name );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Change owner of a schema
     *
     * @param schemaId The id of the schema which gets its ownerId changed
     * @param ownerId Id of the new owner
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public void setSchemaOwner( long schemaId, long ownerId ) throws GenericCatalogException {
        CatalogSchema old = schemas.get( schemaId );
        CatalogSchema schema = CatalogSchema.changeOwner( old, (int) ownerId );
        schemas.replace( schemaId, schema );

        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.setSchemaOwner( transactionHandler, schemaId, ownerId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
         */
    }


    /**
     * Delete a schema from the catalog
     *
     * @param schemaId The if of the schema to delete
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public void deleteSchema( long schemaId ) throws GenericCatalogException {

        CatalogSchema schema = schemas.get( schemaId );
        schemaNames.remove( new Object[]{ schema.databaseId, schema.name } );
        List<Long> oldChildren = new ArrayList<>( databaseChildren.get( schema.id ) );
        oldChildren.remove( schemaId );
        databaseChildren.replace( schema.databaseId, ImmutableList.copyOf( oldChildren ) );
        schemaChildren.remove( schemaId );
        schemas.remove( schemaId );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteSchema( transactionHandler, schemaId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
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
        //TODO refactor call
        CatalogSchema schema = schemas.get( schemaId );
        if ( tableNamePattern != null ) {
            long id = tableNames.get( new Object[]{ schema.databaseId, schemaId, tableNamePattern.pattern } );
            return Collections.singletonList( tables.get( id ) );
        } else {
            List<Long> children = new ArrayList<>( tableNames.prefixSubMap( new Object[]{ schema.databaseId, schemaId } ).values() );
            return children.stream().map( tables::get ).filter( Objects::nonNull ).collect( Collectors.toList() );
        }


        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTables( transactionHandler, schemaId, tableNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }*/
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

        // TODO refactor call

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

        // TODO refactor call

        if ( databaseNamePattern != null && schemaNamePattern != null && tableNamePattern != null ) {
            long databaseId = databaseNames.get( databaseNamePattern.pattern );
            long schemaId = schemaNames.get( new Object[]{ databaseId, schemaNamePattern.pattern } );
            long tableId = tableNames.get( new Object[]{ databaseId, schemaId, tableNamePattern.pattern } );
            return Collections.singletonList( tables.get( tableId ) );
        }
        if ( databaseNamePattern != null && schemaNamePattern != null ) {
            long databaseId = databaseNames.get( databaseNamePattern.pattern );
            long schemaId = schemaNames.get( new Object[]{ databaseId, schemaNamePattern.pattern } );
            return tableNames.prefixSubMap( new Object[]{ databaseId, schemaId } ).values().stream().map( tables::get ).filter( Objects::nonNull ).collect( Collectors.toList() );
        }
        if ( databaseNamePattern != null ) {
            long databaseId = databaseNames.get( databaseNamePattern.pattern );
            return tableNames.prefixSubMap( new Object[]{ databaseId } ).values().stream().map( tables::get ).filter( Objects::nonNull ).collect( Collectors.toList() );
        }

        return tables.values().stream().collect( Collectors.toList() );

        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTables( transactionHandler, databaseNamePattern, schemaNamePattern, tableNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }*/
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
        CatalogSchema schema = schemas.get( schemaId );
        return tables.get( tableNames.get( new Object[]{ schema.databaseId, schemaId, tableName } ) );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTable( transactionHandler, schemaId, tableName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }*/
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

        long schemaId = schemaNames.get( new Object[]{ databaseId, schemaName } );
        return tables.get( tableNames.get( new Object[]{ databaseId, schemaId, tableName } ) );
        /*

        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTable( transactionHandler, databaseId, schemaName, tableName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }*/
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

        long databaseId = databaseNames.get( databaseName );
        long schemaId = schemaNames.get( new Object[]{ databaseId, schemaName } );
        return tables.get( tableNames.get( new Object[]{ databaseId, schemaId, tableName } ) );

        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTable( transactionHandler, databaseName, schemaName, tableName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }*/
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

        long id = tableIdBuilder.getAndIncrement();
        CatalogSchema schema = schemas.get( schemaId );
        CatalogUser owner = users.get( ownerId );
        tables.put( id, new CatalogTable( id, name, schemaId, schema.name, schema.databaseId, schema.databaseName, ownerId, owner.name, tableType, definition, null, null ) );
        // add null instead of empty list? needs check anyway
        tableChildren.put( id, ImmutableList.<Long>builder().build() );
        tableNames.put( new Object[]{ schema.databaseId, schemaId, name }, id );
        List<Long> children = new ArrayList<>( schemaChildren.get( schemaId ) );
        children.add( id );
        schemaChildren.replace( schemaId, ImmutableList.copyOf( children ) );
        return id;
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogSchema schema = Statements.getSchema( transactionHandler, schemaId );
            CatalogUser owner = Statements.getUser( transactionHandler, ownerId );

            return Statements.addTable( transactionHandler, name, schema.id, owner.id, tableType, definition );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownUserException | UnknownSchemaTypeException | UnknownSchemaException e ) {
            throw new GenericCatalogException( e );
        }*/
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

        return tableNames.get( new Object[]{ schemaId, tableName } ) != null;
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogSchema schema = Statements.getSchema( transactionHandler, schemaId );
            Statements.getTable( transactionHandler, schema.id, tableName );
            return true;
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownSchemaTypeException | UnknownTableTypeException | UnknownSchemaException e ) {
            throw new GenericCatalogException( e );
        } catch ( UnknownTableException e ) {
            return false;
        }*/
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

        CatalogTable table = tables.get( tableId );
        tables.replace( tableId, CatalogTable.rename( table, name ) );
        tableNames.remove( new String[]{ table.databaseName, table.schemaName, table.name } );
        tableNames.put( new String[]{ table.databaseName, table.schemaName, name }, tableId );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.renameTable( transactionHandler, tableId, name );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Delete the specified table. Columns, Keys and ColumnPlacements need to be deleted before.
     *
     * @param tableId The id of the table to delete
     */
    @Override
    public void deleteTable( long tableId ) throws GenericCatalogException {
        CatalogTable table = tables.get( tableId );
        List<Long> children = new ArrayList<>( schemaChildren.get( table.schemaId ) );
        children.remove( tableId );
        schemaChildren.replace( table.schemaId, ImmutableList.copyOf( children ) );
        schemas.remove( tableId );
        schemaNames.remove( new String[]{ table.databaseName, table.schemaName, table.name } );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteTable( transactionHandler, tableId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
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

        tables.replace( tableId, CatalogTable.replaceOwner( tables.get( tableId ), ownerId ) );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.setTableOwner( transactionHandler, tableId, ownerId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Set the primary key of a table
     *
     * @param tableId The id of the table
     * @param keyId The id of the key to set as primary key. Set null to set no primary key.
     */
    @Override
    public void setPrimaryKey( long tableId, Long keyId ) throws GenericCatalogException {

        tables.replace( tableId, CatalogTable.replacePrimary( tables.get( tableId ), keyId ) );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.setPrimaryKey( transactionHandler, tableId, keyId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
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

        CatalogColumn column = columns.get( columnId );
        CatalogStore store = stores.get( storeId );

        columnPlacement.put( new long[]{ storeId, columnId }, new CatalogColumnPlacement( column.tableId, column.tableName, columnId, column.name, storeId, store.uniqueName, placementType, physicalSchemaName, physicalTableName, physicalColumnName ) );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogStore store = Statements.getStore( transactionHandler, storeId );
            CatalogColumn column = Statements.getColumn( transactionHandler, columnId );
            Statements.addColumnPlacement( transactionHandler, store.id, column.id, column.tableId, placementType, physicalSchemaName, physicalTableName, physicalColumnName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownStoreException | UnknownCollationException | UnknownTypeException | UnknownColumnException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Deletes a column placement
     *
     * @param storeId The id of the store
     * @param columnId The id of the column
     */
    @Override
    public void deleteColumnPlacement( int storeId, long columnId ) throws GenericCatalogException {
        columnPlacement.remove( new long[]{ storeId, columnId } );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteColumnPlacement( transactionHandler, storeId, columnId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Get column placements on a store
     *
     * @param storeId The id of the store
     * @return List of column placements on this store
     */
    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnStore( int storeId ) {

        return new ArrayList<>( columnPlacement.prefixSubMap( new long[]{ storeId } ).values() );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumnPlacementsOnStore( transactionHandler, storeId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementByColumn( long columnId ) {
        // todo
        return null;
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
        columnPlacement.put( new long[]{ storeId, columnId }, CatalogColumnPlacement.replacePhysicalNames( columnPlacement.get( new long[]{ storeId, columnId } ), physicalSchemaName, physicalTableName, physicalColumnName ) );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.updateColumnPlacementPhysicalNames( transactionHandler, storeId, columnId, physicalSchemaName, physicalTableName, physicalColumnName );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Get all columns of the specified table.
     *
     * @param tableId The id of the table
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogColumn> getColumns( long tableId ) throws GenericCatalogException, UnknownCollationException, UnknownTypeException {
        return tableChildren.get( tableId ).stream().map( columns::get ).filter( Objects::nonNull ).collect( Collectors.toList() );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumns( transactionHandler, tableId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
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

        if ( databaseNamePattern != null && schemaNamePattern != null && tableNamePattern != null && columnNamePattern != null ) {
            long databaseId = databaseNames.get( databaseNamePattern.pattern );
            long schemaId = schemaNames.get( new Object[]{ databaseId, schemaNamePattern.pattern } );
            long tableId = tableNames.get( new Object[]{ databaseId, schemaId, tableNamePattern.pattern } );
            long columnId = columnNames.get( new Object[]{ databaseId, schemaId, tableId, columnNamePattern } );
            return Collections.singletonList( columns.get( columnId ) );
        }
        if ( databaseNamePattern != null && schemaNamePattern != null && tableNamePattern != null ) {
            long databaseId = databaseNames.get( databaseNamePattern.pattern );
            long schemaId = schemaNames.get( new Object[]{ databaseId, schemaNamePattern.pattern } );
            long tableId = tableNames.get( new Object[]{ databaseId, schemaId, tableNamePattern.pattern } );
            return tableChildren.get( tableId ).stream().map( columns::get ).filter( Objects::nonNull ).collect( Collectors.toList() );
        }
        if ( databaseNamePattern != null && schemaNamePattern != null ) {
            long databaseId = databaseNames.get( databaseNamePattern.pattern );
            long schemaId = schemaNames.get( new Object[]{ databaseId, schemaNamePattern.pattern } );
            return tableNames.prefixSubMap( new Object[]{ databaseId, schemaId } ).values().stream().map( columns::get ).filter( Objects::nonNull ).collect( Collectors.toList() );
        }
        if ( databaseNamePattern != null ) {
            long databaseId = databaseNames.get( databaseNamePattern.pattern );
            return tableNames.prefixSubMap( new Object[]{ databaseId } ).values().stream().map( columns::get ).filter( Objects::nonNull ).collect( Collectors.toList() );
        }

        return columns.values().stream().collect( Collectors.toList() );
        /*

        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumns( transactionHandler, databaseNamePattern, schemaNamePattern, tableNamePattern, columnNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
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
        return columns.get( columnId );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumn( transactionHandler, columnId );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownCollationException | UnknownTypeException e ) {
            throw new GenericCatalogException( e );
        }*/
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

        CatalogTable table = tables.get( tableId );
        return columns.get( columnNames.get( new Object[]{ table.databaseId, table.schemaId, table.name } ) );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumn( transactionHandler, tableId, columnName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownCollationException | UnknownTypeException e ) {
            throw new GenericCatalogException( e );
        }*/
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

        long databaseId = databaseNames.get( databaseName );
        long schemaId = schemaNames.get( new Object[]{ databaseId, schemaName } );
        long tableId = tableNames.get( new Object[]{ databaseId, schemaId, tableName } );
        long columnId = columnNames.get( new Object[]{ databaseId, schemaId, tableId, } );
        return columns.get( columnId );
        /*

        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumn( transactionHandler, databaseName, schemaName, tableName, columnName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownCollationException | UnknownTypeException e ) {
            throw new GenericCatalogException( e );
        }*/
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

        CatalogTable table = tables.get( tableId );
        long id = columnIdBuilder.getAndIncrement();
        columns.put( id, new CatalogColumn( id, name, tableId, table.name, table.schemaId, table.schemaName, table.databaseId, table.databaseName, position, type, length, scale, nullable, collation, null ) );
        columnNames.put( new Object[]{ table.databaseId, table.schemaId, name }, id );
        List<Long> children = new ArrayList<>( tableChildren.get( tableId ) );
        children.add( id );
        tableChildren.replace( id, ImmutableList.copyOf( children ) );
        return id;
        /*

        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogTable table = Statements.getTable( transactionHandler, tableId );
            if ( type.isCharType() && collation == null ) {
                throw new RuntimeException( "Collation is not allowed to be null for char types." );
            }
            if ( scale != null && scale > length ) {
                throw new RuntimeException( "Invalid scale! Scale can not be larger than length." );
            }

            long id = columnIdBuilder.getAndIncrement();
            columns.put( id, new CatalogColumn( id, name, tableId, name, table.schemaId, table.schemaName, table.databaseId, table.databaseName, position, type, length, scale, nullable, collation, null ) );

            return Statements.addColumn( transactionHandler, name, table.id, position, type, length, scale, nullable, collation );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException | UnknownTableException e ) {
            throw new GenericCatalogException( e );
        }
         */
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
        CatalogColumn column = columns.get( columnId );
        columns.replace( columnId, CatalogColumn.replaceName( column, name ) );
        columnNames.remove( new Object[]{ column.databaseId, column.schemaId, column.tableId, column.name } );
        columnNames.put( new Object[]{ column.databaseId, column.schemaId, column.tableId, name }, columnId );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.renameColumn( transactionHandler, columnId, name );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Change move the column to the specified position. Make sure, that there is no other column with this position in the table.
     *
     * @param columnId The id of the column for which to change the position
     * @param position The new position of the column
     */
    @Override
    public void setColumnPosition( long columnId, int position ) throws GenericCatalogException {
        CatalogColumn column = columns.get( columnId );
        columns.replace( columnId, CatalogColumn.replacePosition( column, position ) );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.setColumnPosition( transactionHandler, columnId, position );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Change the data type of an column.
     *
     * @param columnId The id of the column
     * @param type The new type of the column
     */
    @Override
    public void setColumnType( long columnId, PolySqlType type, Integer length, Integer scale ) throws GenericCatalogException {
        CatalogColumn column = columns.get( columnId );
        columns.replace( columnId, CatalogColumn.replaceColumnType( column, type, length, scale ) );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            if ( scale != null && scale > length ) {
                throw new RuntimeException( "Invalid scale! Scale can not be larger than length." );
            }
            Collation collation = type.isCharType() ? Collation.getById( RuntimeConfig.DEFAULT_COLLATION.getInteger() ) : null;
            Statements.setColumnType( transactionHandler, columnId, type, length, scale, collation );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownCollationException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Change nullability of the column (weather the column allows null values).
     *
     * @param columnId The id of the column
     * @param nullable True if the column should allow null values, false if not.
     */
    @Override
    public void setNullable( long columnId, boolean nullable ) throws GenericCatalogException {
        CatalogColumn column = columns.get( columnId );
        columns.replace( columnId, CatalogColumn.replaceNullable( column, nullable ) );
        /*
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
         */
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
        CatalogColumn column = columns.get( columnId );
        columns.replace( columnId, CatalogColumn.replaceCollation( column, collation ) );

        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogColumn catalogColumn = Statements.getColumn( transactionHandler, columnId );
            if ( !catalogColumn.type.isCharType() ) {
                throw new RuntimeException( "Illegal attempt to set collation for a non-char column!" );
            }
            Statements.setCollation( transactionHandler, columnId, collation );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownCollationException | UnknownColumnException | UnknownTypeException e ) {
            throw new GenericCatalogException( e );
        }*/
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
        CatalogTable table = tables.get( tableId );
        return columnNames.get( new Object[]{ table.databaseId, table.schemaId, tableId, columnName } ) != null;

        /*

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
         */
    }


    /**
     * Delete the specified column. This also deletes a default value in case there is one defined for this column.
     *
     * @param columnId The id of the column to delete
     */
    @Override
    public void deleteColumn( long columnId ) throws GenericCatalogException {
        CatalogColumn column = columns.get( columnId );
        columnNames.remove( new Object[]{ column.databaseId, column.schemaId, column.tableId, column.name } );
        List<Long> children = new ArrayList<>( tableChildren.get( column.tableId ) );
        children.remove( columnId );
        tableChildren.replace( column.tableId, ImmutableList.copyOf( children ) );
        columns.remove( columnId );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            deleteDefaultValue( columnId );
            Statements.deleteColumn( transactionHandler, columnId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
         */
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
        CatalogColumn column = columns.get( columnId );
        columns.replace( columnId, CatalogColumn.replaceDefaultValue( column, type, defaultValue ) );
        /*
        try {
            deleteDefaultValue( columnId );
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.setDefaultValue( transactionHandler, columnId, type, defaultValue );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Deletes an existing default value of a column. NoOp if there is no default value defined.
     *
     * @param columnId The id of the column
     */
    @Override
    public void deleteDefaultValue( long columnId ) throws GenericCatalogException {
        CatalogColumn column = columns.get( columnId );
        columns.replace( columnId, CatalogColumn.replaceDefaultValue( column, column.type, null ) );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteDefaultValue( transactionHandler, columnId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
         */
    }


    /**
     * Returns a specified primary key
     *
     * @param key The id of the primary key
     * @return The primary key
     */
    @Override
    public CatalogPrimaryKey getPrimaryKey( long key ) throws GenericCatalogException, UnknownKeyException {
        return primaryKeys.get( key );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return new CatalogPrimaryKey( Statements.getPrimaryKey( transactionHandler, key ) );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Adds a primary key to a specified table. If there is already a primary key defined for this table it is replaced.
     *
     * @param tableId The id of the table
     * @param columnIds The id of key which will be part of the primary keys
     */
    @Override
    public void addPrimaryKey( long tableId, List<Long> columnIds ) throws GenericCatalogException {

        CatalogTable table = tables.get( tableId );
        List<CatalogColumn> nullables = columnIds.stream().map( columns::get ).filter( c -> c.nullable ).collect( Collectors.toList() );
        for ( CatalogColumn col : nullables ) {
            throw new GenericCatalogException( "Primary key is not allowed to contain null values but the column '" + col.name + "' is declared nullable." );
        }

        // TODO refactor
        /*
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
        */
    }


    /**
     * Returns all (imported) foreign keys of a specified table
     *
     * @param tableId The id of the table
     * @return List of foreign keys
     */
    @Override
    public List<CatalogForeignKey> getForeignKeys( long tableId ) throws GenericCatalogException {
        // TODO check list
        return tables.get( tableId ).foreignKeys.stream().map( foreignKeys::get ).filter( Objects::nonNull ).collect( Collectors.toList() );
        /*

        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getForeignKeys( transactionHandler, tableId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Returns all foreign keys that reference the specified table (exported keys).
     *
     * @param tableId The id of the table
     * @return List of foreign keys
     */
    @Override
    public List<CatalogForeignKey> getExportedKeys( long tableId ) throws GenericCatalogException {

        return foreignKeys.values().stream().filter( k -> k.referencedKeyTableId == tableId ).collect( Collectors.toList() );
        /*

        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getExportedKeys( transactionHandler, tableId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Get all constraints of the specified table
     *
     * @param tableId The id of the table
     * @return List of constraints
     */
    @Override
    public List<CatalogConstraint> getConstraints( long tableId ) throws GenericCatalogException {
        return constraints.values().stream().filter( c -> keys.get( c.key ).tableId == tableId ).collect( Collectors.toList() );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getConstraints( transactionHandler, tableId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
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

        return keys.values().stream()
                .filter( k -> k.tableId == tableId )
                .map( k -> constraints.get( k.id ) )
                .filter( c -> c.name.equals( constraintName ) ).findFirst().get();

        /*

        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getConstraint( transactionHandler, tableId, constraintName );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
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
        return foreignKeys.values().stream().filter( f -> f.tableId == tableId && f.name.equals( foreignKeyName ) ).findFirst().get();
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getForeignKey( transactionHandler, tableId, foreignKeyName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownForeignKeyException e ) {
            throw new GenericCatalogException( e );
        }*/
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
        CatalogTable table = tables.get( tableId );
        List<CatalogKey> childKeys = table.foreignKeys.stream().map( keys::get ).collect( Collectors.toList() );

        for ( CatalogKey refKey : childKeys ) {
            if ( refKey.columnIds.size() == referencesIds.size() && refKey.columnIds.containsAll( referencesIds ) && referencesIds.containsAll( refKey.columnIds ) ) {
                // TODO refactor
                List<CatalogColumn> cols = refKey.columnIds.stream().map( columns::get ).filter( Objects::nonNull ).collect( Collectors.toList() );
                int i = 0;
                for ( CatalogColumn referencedColumn : cols ) {
                    CatalogColumn referencingColumn = columns.get( referencedColumn.id );
                    if ( referencedColumn.type != referencingColumn.type ) {
                        throw new GenericCatalogException( "The data type of the referenced columns does not match the data type of the referencing column: " + referencingColumn.type.name() + " != " + referencedColumn.type );
                    }
                }
                // TODO simplify constraintType tested twice atm
                List<CatalogConstraint> consts = constraints.values().stream().filter( c -> c.keyId == refKey.id && c.type == ConstraintType.UNIQUE ).collect( Collectors.toList() );
                List<CatalogIndex> inds = indices.values().stream().filter( in -> in.keyId == refKey.id ).collect( Collectors.toList() );
                final int unique = computeUniqueCount( tables.get( refKey.tableId ).primaryKey == refKey.id, consts, inds );
                if ( unique > 0 ) {
                    long keyId = getOrAddKey( tableId, columnIds );
                    foreignKeys.put( keyId, new CatalogForeignKey( keyId, constraintName, refKey.tableId, refKey.tableName, refKey.schemaId, refKey.schemaName, refKey.databaseId, refKey.databaseName, keyId, tableId, table.name, table.schemaId, table.schemaName, table.databaseId, table.databaseName, onUpdate, onDelete ) );
                    return;
                }

            }
        }
        /*

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
         */
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
        // TODO
        long keyId = getOrAddKey( tableId, columnIds );
        List<CatalogConstraint> catalogConstraints = constraints.values().stream().filter( c -> c.keyId == keyId && c.type == ConstraintType.UNIQUE ).collect( Collectors.toList() );
        if ( catalogConstraints.size() > 0 ) {
            throw new GenericCatalogException( "There is already a unique constraint!" );
        }
        long id = constraintIdBuilder.getAndIncrement();
        constraints.put( id, new CatalogConstraint( id, keyId, ConstraintType.UNIQUE, constraintName ) );
        /*
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
        }*/
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
        if ( !onlyUnique ) {
            return indices.values().stream().filter( i -> i.key.tableId == tableId ).collect( Collectors.toList() );
        } else {
            return indices.values().stream().filter( i -> i.key.tableId == tableId && i.unique ).collect( Collectors.toList() );
        }
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getIndexes( transactionHandler, tableId, onlyUnique );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
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
        return indices.values().stream().filter( i -> i.key.tableId == tableId && i.name.equals( indexName ) ).findFirst().get();
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getIndex( transactionHandler, tableId, indexName );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
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
        long keyId = getOrAddKey( tableId, columnIds );
        if ( unique ) {
            // TODO: Check if the current values are unique
        }
        long id = indexIdBuilder.getAndIncrement();
        indices.put( id, new CatalogIndex( id, indexName, unique, type, null, keyId ) );
        return id;
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            long keyId = getOrAddKey( transactionHandler, tableId, columnIds );
            if ( unique ) {
                // TODO: Check if the current values are unique
            }
            return Statements.addIndex( transactionHandler, keyId, type, unique, null, indexName );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Delete the specified index
     *
     * @param indexId The id of the index to drop
     */
    @Override
    public void deleteIndex( long indexId ) throws GenericCatalogException, UnknownIndexException {
        CatalogIndex index = indices.get( indexId );
        if ( index.unique ) {
            List<CatalogConstraint> consts = constraints.values().stream().filter( c -> c.keyId == index.key.id && c.type == ConstraintType.UNIQUE ).collect( Collectors.toList() );
            List<CatalogIndex> inds = indices.values().stream().filter( in -> in.keyId == index.key.id ).collect( Collectors.toList() );
            final int uniqueCount = computeUniqueCount( tables.get( index.key.tableId ).primaryKey == index.key.id, consts, inds );
            final int referencedByCount = foreignKeys.values().stream().filter( f -> f.referencedKeyId == indexId ).collect( Collectors.toList() ).size();
            if ( uniqueCount == 1 && referencedByCount > 0 ) {
                // This unique index is the only constraint for the uniqueness of this key.
                throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. To delete this index, first add a unique constraint." );
            }
            indices.remove( indexId );
            deleteKeyIfNoLongerUsed( index.key.id );
        }
        /*
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
        }*/
    }


    /**
     * Deletes the specified primary key (including the entry in the key table). If there is an index on this key, make sure to delete it first.
     * If there is no primary key, this operation is a NoOp.
     *
     * @param tableId The id of the key to drop
     */
    @Override
    public void deletePrimaryKey( long tableId ) throws GenericCatalogException {
        CatalogTable table = tables.get( tableId );

        // TODO: Check if the currently stored values are unique
        if ( table.primaryKey != null ) {
            CatalogKey key = keys.get( table.primaryKey );
            //TODO move to method?
            final long referencedCount = foreignKeys.values().stream().filter( f -> f.referencedKeyId == key.id ).count();
            if ( referencedCount > 0 ) {
                // TODO move to method
                List<CatalogConstraint> consts = constraints.values().stream().filter( c -> c.keyId == key.id && c.type == ConstraintType.UNIQUE ).collect( Collectors.toList() );
                List<CatalogIndex> inds = indices.values().stream().filter( in -> in.keyId == key.id ).collect( Collectors.toList() );
                final int uniqueCount = computeUniqueCount( true, consts, inds );
                if ( uniqueCount < 2 ) {
                    throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. To drop this primary key either drop the foreign key or create an unique constraint." );
                }
                setPrimaryKey( tableId, key.id );
                deleteKeyIfNoLongerUsed( key.id );
            }
        }

        /*
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
                deleteKeyIfNoLongerUsed( catalogTable.primaryKey );
            }
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownTableTypeException | UnknownTableException | UnknownKeyException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Delete the specified foreign key (does not delete the referenced key).
     *
     * @param foreignKeyId The id of the foreign key to delete
     */
    @Override
    public void deleteForeignKey( long foreignKeyId ) throws GenericCatalogException {
        CatalogForeignKey catalogForeignKey = foreignKeys.get( foreignKeyId );
        foreignKeys.remove( catalogForeignKey.referencedKeyId );
        deleteKeyIfNoLongerUsed( catalogForeignKey.referencedKeyId );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogForeignKey catalogForeignKey = Statements.getForeignKey( transactionHandler, foreignKeyId );
            CatalogCombinedKey key = getCombinedKey( transactionHandler, catalogForeignKey.id );
            Statements.deleteForeignKey( transactionHandler, key.getKey().id );
            deleteKeyIfNoLongerUsed( key.getKey().id );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownKeyException | UnknownForeignKeyException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Delete the specified constraint.
     * For deleting foreign keys, use {@link #deleteForeignKey(long)}.
     *
     * @param constraintId The id of the constraint to delete
     */
    @Override
    public void deleteConstraint( long constraintId ) throws GenericCatalogException {
        CatalogConstraint constraint = constraints.get( constraintId );

        List<CatalogConstraint> consts = constraints.values().stream().filter( c -> c.keyId == constraint.keyId && c.type == ConstraintType.UNIQUE ).collect( Collectors.toList() );
        List<CatalogIndex> inds = indices.values().stream().filter( in -> in.keyId == constraint.keyId ).collect( Collectors.toList() );

        final long referencedCount = foreignKeys.values().stream().filter( f -> f.referencedKeyId == constraint.keyId ).count();
        if ( constraint.type == ConstraintType.UNIQUE && referencedCount > 0 ) {
            final int uniqueCount = computeUniqueCount( true, consts, inds );
            if ( uniqueCount < 2 ) {
                throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. Unable to drop unique constraint." );
            }
        }
        /*
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
            deleteKeyIfNoLongerUsed( key.getKey().id );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownKeyException | UnknownConstraintException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Get the user with the specified name
     *
     * @param userName The name of the user
     * @return The user
     * @throws UnknownUserException If there is no user with the specified name
     */
    @Override
    public CatalogUser getUser( String userName ) throws UnknownUserException {
        try {
            return users.get( userNames.get( userName ) );
        } catch ( NullPointerException e ) {
            throw new UnknownUserException( userName );
        }

        /*
        try {
            val transactionHandler = XATransactionHandler.getTransactionHandler( xid );
            return Statements.getUser( transactionHandler, userName );
        } catch ( GenericCatalogException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    public CatalogUser getUser( int userId ) throws UnknownUserException {
        try {
            return users.get( userId );
        } catch ( NullPointerException e ) {
            throw new UnknownUserException( userId );
        }
    }


    /**
     * Get list of all stores
     *
     * @return List of stores
     */
    @Override
    public List<CatalogStore> getStores() throws GenericCatalogException {
        return new ArrayList<>( stores.values() );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getStores( transactionHandler );
        } catch ( GenericCatalogException | CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Get a store by its unique name
     *
     * @return List of stores
     */
    @Override
    public CatalogStore getStore( String uniqueName ) throws GenericCatalogException {
        uniqueName = uniqueName.toLowerCase();

        return stores.get( uniqueName );
        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getStore( transactionHandler, uniqueName );
        } catch ( GenericCatalogException | UnknownStoreException | CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
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

        int id = storeIdBuilder.getAndIncrement();
        stores.put( id, new CatalogStore( id, uniqueName, adapter, settings ) );
        storeNames.put( uniqueName, id );
        return id;
        /*

        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.addStore( transactionHandler, uniqueName, adapter, settings );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/
    }


    /**
     * Delete a store
     *
     * @param storeId The id of the store to delete
     */
    @Override
    public void deleteStore( int storeId ) throws GenericCatalogException {
        // TODO remove database/schemas/... as well?
        stores.remove( storeId );

        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteStore( transactionHandler, storeId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Builds a complex/expensive combined database object
     *
     * @param databaseId the id of the database
     * @return the combined database object
     * @throws UnknownDatabaseException if databaseId does not exist in catalog
     */
    @Override
    public CatalogCombinedDatabase getCombinedDatabase( long databaseId ) throws UnknownDatabaseException, UnknownSchemaException, UnknownUserException, UnknownTableException {
        try {
            CatalogDatabase database = databases.get( databaseId );
            List<CatalogCombinedSchema> childSchemas = new ArrayList<>();
            for ( Long aLong : databaseChildren.get( databaseId ) ) {
                CatalogCombinedSchema combinedSchema = getCombinedSchema( aLong );
                if ( combinedSchema != null ) {
                    childSchemas.add( combinedSchema );
                }
            }

            CatalogSchema defaultSchema = null;
            if ( database.defaultSchemaId != null ) {
                defaultSchema = schemas.get( database.defaultSchemaId );
            }

            CatalogUser user = users.get( database.ownerId );
            return new CatalogCombinedDatabase( database, childSchemas, defaultSchema, user );
        } catch ( NullPointerException e ) {
            throw new UnknownDatabaseException( databaseId );
        }

        /*
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return getCombinedDatabase( transactionHandler, databaseId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }*/

    }


    // TODO move
    public List<CatalogKey> getKeys() {
        return new ArrayList<>( keys.values() );
    }


    @Override
    public CatalogCombinedSchema getCombinedSchema( long schemaId ) throws UnknownSchemaException, UnknownDatabaseException, UnknownTableException, UnknownUserException {
        try {
            CatalogSchema schema = schemas.get( schemaId );
            List<CatalogCombinedTable> childTables = new ArrayList<>();
            for ( Long aLong : schemaChildren.get( schemaId ) ) {
                CatalogCombinedTable combinedTable = getCombinedTable( aLong );
                if ( combinedTable != null ) {
                    childTables.add( combinedTable );
                }
            }
            CatalogDatabase database = getDatabase( schema.databaseId );
            CatalogUser owner = getUser( schema.ownerId );

            return new CatalogCombinedSchema( schema, childTables, database, owner );
        } catch ( NullPointerException e ) {
            throw new UnknownSchemaException( schemaId );
        }

    }


    @Override
    public CatalogCombinedTable getCombinedTable( long tableId ) throws UnknownTableException {
        try {
            CatalogTable table = tables.get( tableId );
            List<CatalogColumn> childColumns = tableChildren.get( tableId ).stream().map( columns::get ).collect( Collectors.toList() );
            CatalogSchema schema = schemas.get( table.schemaId );
            CatalogDatabase database = databases.get( table.databaseId );
            CatalogUser user = users.get( table.ownerId );

            Map<Integer, List<CatalogColumnPlacement>> columnPlacementByStore = new HashMap<>();
            stores.keySet().forEach( id -> {
                // TODO empty list, no entry when empty?
                columnPlacementByStore.put( id, getColumnPlacementsOnStore( id ) );
            } );

            Map<Long, List<CatalogColumnPlacement>> columnPlacementByColumn = new HashMap<>();

            tableChildren.get( tableId ).forEach( id -> {
                columnPlacementByColumn.put( id, getColumnPlacementByColumn( id ) );
            } );

            List<CatalogKey> tableKeys = getKeys().stream().filter( k -> k.tableId == tableId ).collect( Collectors.toList() );

            return new CatalogCombinedTable( table, childColumns, schema, database, user, columnPlacementByStore, columnPlacementByColumn, tableKeys );
        } catch ( NullPointerException e ) {
            throw new UnknownTableException( tableId );
        }

    }


    @Override
    public CatalogCombinedKey getCombinedKey( long keyId ) throws UnknownKeyException {
        try {

            CatalogKey key = keys.get( keyId );
            List<CatalogColumn> childColumns = key.columnIds.stream().map( columns::get ).filter( Objects::nonNull ).collect( Collectors.toList() );
            CatalogTable table = tables.get( key.tableId );
            CatalogSchema schema = schemas.get( key.tableId );
            CatalogDatabase database = databases.get( key.databaseId );
            List<CatalogForeignKey> childForeignKeys = foreignKeys.values().stream().filter( f -> f.referencedKeyId == keyId ).collect( Collectors.toList() );
            List<CatalogIndex> childIndices = indices.values().stream().filter( i -> i.keyId == keyId ).collect( Collectors.toList() );
            List<CatalogConstraint> childConstraints = constraints.values().stream().filter( c -> c.keyId == keyId ).collect( Collectors.toList() );

            return new CatalogCombinedKey( key, childColumns, table, schema, database, childForeignKeys, childIndices, childConstraints, childForeignKeys );
        } catch ( NullPointerException e ) {
            throw new UnknownKeyException( keyId );
        }
    }

    // TODO move to right location, here for now


    private int computeUniqueCount( boolean isPrimaryKey, List<CatalogConstraint> constraints, List<CatalogIndex> indexes ) {
        int count = 0;
        if ( isPrimaryKey ) {
            count++;
        }

        for ( CatalogConstraint constraint : constraints ) {
            if ( constraint.type == ConstraintType.UNIQUE ) {
                count++;
            }
        }

        for ( CatalogIndex index : indexes ) {
            if ( index.unique ) {
                count++;
            }
        }

        return count;
    }


    // Check if the specified key is used as primary key, index or constraint. If so, this is a NoOp. If it is not used, the key is deleted.
    private void deleteKeyIfNoLongerUsed( Long keyId ) throws GenericCatalogException {
        CatalogKey key = keys.get( keyId );
        CatalogTable table = tables.get( key.tableId );
        if ( table.primaryKey == keyId ) {
            return;
        }
        if ( constraints.values().stream().filter( c -> c.keyId == keyId ).collect( Collectors.toList() ).size() > 0 ) {
            return;
        }
        if ( foreignKeys.values().stream().filter( f -> f.referencedKeyId == keyId ).collect( Collectors.toList() ).size() > 0 ) {
            return;
        }
        if ( indices.values().stream().filter( i -> i.keyId == keyId ).collect( Collectors.toList() ).size() > 0 ) {
            return;
        }
        keys.remove( keyId );

        /*
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
         */
    }


    // Returns the id of they defined by the specified column ids. If this key does not yet exist, create it.
    private long getOrAddKey( long tableId, List<Long> columnIds ) throws GenericCatalogException {
        // TODO change to 0 when done, -1 cause 0 was unrecognizable
        long keyId = -1;
        List<CatalogKey> catalogKeys = keys.values().stream().filter( k -> k.tableId == tableId ).collect( Collectors.toList() );
        for ( CatalogKey key : catalogKeys ) {
            if ( key.columnIds.size() == columnIds.size() && key.columnIds.containsAll( columnIds ) && columnIds.containsAll( key.columnIds ) ) {
                keyId = key.id;
            }
        }
        if ( keyId == -1 ) {
            keyId = addKey( tableId, columnIds );
        }
        return keyId;


        /*
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
        return keyId;*/
    }


    private long addKey( long tableId, List<Long> columnIds ) {
        CatalogTable table = tables.get( tableId );
        long id = keyIdBuilder.getAndIncrement();
        keys.put( id, new CatalogKey( id, table.id, table.name, table.schemaId, table.schemaName, table.databaseId, table.databaseName, columnIds, columnIds.stream().map( i -> columns.get( i ).name ).collect( Collectors.toList() ) ) );
        return id;
    }


}

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
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;
import org.polypheny.db.PolySqlType;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.StoreManager;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogDefaultValue;
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
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownColumnPlacementException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyException;
import org.polypheny.db.catalog.exceptions.UnknownIndexException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownStoreException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.transaction.Transaction;


@Slf4j
public class CatalogImpl extends Catalog {


    private static final String FILE_PATH = "mapDB";
    private static DB db;

    private static HTreeMap<Integer, CatalogUser> users;
    private static HTreeMap<String, CatalogUser> userNames;

    private static HTreeMap<Long, CatalogDatabase> databases;
    private static HTreeMap<String, CatalogDatabase> databaseNames;
    private static HTreeMap<Long, ImmutableList<Long>> databaseChildren;
    private static HTreeMap<Long, CatalogCombinedDatabase> combinedDatabases;


    private static HTreeMap<Long, CatalogSchema> schemas;
    private static BTreeMap<Object[], CatalogSchema> schemaNames;
    private static HTreeMap<Long, ImmutableList<Long>> schemaChildren;
    private static HTreeMap<Long, CatalogCombinedSchema> combinedSchemas;


    private static HTreeMap<Long, CatalogTable> tables;
    private static BTreeMap<Object[], CatalogTable> tableNames;
    private static HTreeMap<Long, ImmutableList<Long>> tableChildren;
    private static HTreeMap<Long, CatalogCombinedTable> combinedTables;

    private static HTreeMap<Long, CatalogColumn> columns;
    private static BTreeMap<Object[], CatalogColumn> columnNames;
    private static BTreeMap<Object[], CatalogColumnPlacement> columnPlacements;

    private static HTreeMap<Integer, CatalogStore> stores;
    private static HTreeMap<String, CatalogStore> storeNames;

    private static HTreeMap<Long, CatalogKey> keys;
    private static HTreeMap<Long, CatalogCombinedKey> combinedKeys;

    private static HTreeMap<Long, CatalogPrimaryKey> primaryKeys;
    private static HTreeMap<Long, CatalogForeignKey> foreignKeys;
    private static HTreeMap<Long, CatalogConstraint> constraints;
    private static HTreeMap<Long, CatalogIndex> indices;


    private static final AtomicInteger storeIdBuilder = new AtomicInteger();
    private static final AtomicInteger userIdBuilder = new AtomicInteger();

    private static final AtomicLong databaseIdBuilder = new AtomicLong();
    private static final AtomicLong schemaIdBuilder = new AtomicLong();
    private static final AtomicLong tableIdBuilder = new AtomicLong();
    private static final AtomicLong columnIdBuilder = new AtomicLong();

    private static final AtomicLong keyIdBuilder = new AtomicLong();
    private static final AtomicLong constraintIdBuilder = new AtomicLong();
    private static final AtomicLong indexIdBuilder = new AtomicLong();
    private static final AtomicLong foreignKeyIdBuilder = new AtomicLong();


    public CatalogImpl() {
        this( FILE_PATH, true, true );
    }


    /**
     * MapDB Catalog; idea is to only need a minimal amount( max 2-3 ) map lookups for each get
     * most maps should work with ids to prevent overhead when renaming
     */
    public CatalogImpl( String path, boolean doInitSchema, boolean doInitInformationPage ) {
        super();

        if ( db != null && !db.isClosed() ) {
            return;
        } else if ( db != null ) {
            db.close();
        }
        synchronized ( this ) {

            db = DBMaker
                    .fileDB( new File( path ) )
                    .closeOnJvmShutdown()
                    .checksumHeaderBypass() // TODO clean shutdown needed
                    .fileMmapEnable()
                    .fileMmapEnableIfSupported()
                    .fileMmapPreclearDisable()
                    .make();

            initDBLayout( db );

            // mirrors default data from old sql file
            try {
                restoreAllIdBuilders();
                if ( doInitSchema ) {
                    insertDefaultData();
                }

            } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownTableException | UnknownSchemaException | UnknownStoreException e ) {
                e.printStackTrace();
            }
            if ( doInitInformationPage ) {
                new CatalogInfoPage( this );
            }

        }
    }


    /**
     * Restores all columnPlacements in the dedicated store
     */
    public void restoreColumnPlacements( Transaction trx ) {
        StoreManager manager = StoreManager.getInstance();

        List<Long> restoredTables = new ArrayList<>();

        columns.values().forEach( c -> {
            List<CatalogColumnPlacement> placements = getColumnPlacements( c.id );
            if ( placements.size() == 0 ) {
                // no placements shouldn't happen
            } else if ( placements.size() == 1 ) {
                Store store = manager.getStore( placements.get( 0 ).storeId );
                if ( !store.isPersistent() ) {

                    CatalogCombinedTable combinedTable = getCombinedTable( c.tableId );
                    // TODO only full placements atm here

                    if ( !restoredTables.contains( c.tableId ) ) {
                        store.createTable( trx.getPrepareContext(), combinedTable );
                        restoredTables.add( c.tableId );
                    }

                }
            } else {
                Map<Integer, Boolean> persistent = placements.stream().collect( Collectors.toMap( p -> p.storeId, p -> manager.getStore( p.storeId ).isPersistent() ) );
                if ( !persistent.containsValue( true ) ) {
                    // no persistent placement for this column
                    try {
                        CatalogCombinedTable table = getCombinedTable( c.tableId );
                        for ( CatalogColumnPlacement p : placements ) {
                            manager.getStore( p.storeId ).addColumn( null, table, getColumn( p.columnId ) );
                        }
                    } catch ( UnknownColumnException e ) {
                        e.printStackTrace();
                    }

                } else if ( persistent.containsValue( true ) && persistent.containsValue( false ) ) {
                    // TODO DL change so column gets copied
                    persistent.entrySet().stream().filter( p -> !p.getValue() ).forEach( p -> deleteColumnPlacement( p.getKey(), c.id ) );
                }
            }
        } );
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
        restoreIdBuilder( foreignKeys, foreignKeyIdBuilder );

    }


    private void initStoreInfo( DB db ) {
        stores = db.hashMap( "stores", Serializer.INTEGER, new GenericSerializer<CatalogStore>() ).createOrOpen();
        storeNames = db.hashMap( "storeNames", Serializer.STRING, new GenericSerializer<CatalogStore>() ).createOrOpen();
    }


    private void initKeysAndConstraintsInfo( DB db ) {
        combinedKeys = db.hashMap( "combinedKeys", Serializer.LONG, new GenericSerializer<CatalogCombinedKey>() ).createOrOpen();
        keys = db.hashMap( "keys", Serializer.LONG, new GenericSerializer<CatalogKey>() ).createOrOpen();
        primaryKeys = db.hashMap( "primaryKeys", Serializer.LONG, new GenericSerializer<CatalogPrimaryKey>() ).createOrOpen();
        foreignKeys = db.hashMap( "foreignKeys", Serializer.LONG, new GenericSerializer<CatalogForeignKey>() ).createOrOpen();
        constraints = db.hashMap( "constraints", Serializer.LONG, new GenericSerializer<CatalogConstraint>() ).createOrOpen();
        indices = db.hashMap( "indices", Serializer.LONG, new GenericSerializer<CatalogIndex>() ).createOrOpen();
    }


    /**
     * Fills the catalog database with default data, skips if data is already inserted
     */
    private void insertDefaultData() throws GenericCatalogException, UnknownUserException, UnknownDatabaseException, UnknownTableException, UnknownSchemaException, UnknownStoreException {
        //////////////
        // init users
        int systemId;
        if ( !userNames.containsKey( "system" ) ) {
            systemId = addUser( "system", "" );
        } else {
            systemId = getUser( "system" ).id;
        }

        if ( !userNames.containsKey( "pa" ) ) {
            addUser( "pa", "" );
        }

        //////////////
        // init database
        long databaseId;
        if ( !databaseNames.containsKey( "APP" ) ) {
            databaseId = addDatabase( "APP", systemId, "system", 0L, "public" );
        } else {
            databaseId = getDatabase( "APP" ).id;
        }

        //////////////
        // init schema

        long schemaId;
        if ( !schemaNames.containsKey( new Object[]{ databaseId, "public" } ) ) {
            schemaId = addSchema( "public", databaseId, 0, SchemaType.RELATIONAL );
        } else {
            schemaId = getSchema( "APP", "public" ).id;
        }

        //////////////
        // init schema
        if ( !tableNames.containsKey( new Object[]{ databaseId, schemaId, "depts" } ) ) {
            addTable( "depts", schemaId, systemId, TableType.TABLE, null );
        }
        if ( !tableNames.containsKey( new Object[]{ databaseId, schemaId, "emps" } ) ) {
            addTable( "emps", schemaId, systemId, TableType.TABLE, null );
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
            csvSetttings.put( "persistent", "true" );

            addStore( "csv", "org.polypheny.db.adapter.csv.CsvStore", csvSetttings );
        }

        CatalogStore csv = getStore( "csv" );
        // TODO temporary change

        addDefaultCsvColumns( csv );

    }


    private void addDefaultCsvColumns( CatalogStore csv ) throws UnknownSchemaException, UnknownTableException, GenericCatalogException {
        CatalogSchema schema = getSchema( "APP", "public" );
        CatalogTable depts = getTable( schema.id, "depts" );

        addDefaultColumn( csv, depts, "deptno", PolySqlType.INTEGER, null, 1, null );
        addDefaultColumn( csv, depts, "name", PolySqlType.VARCHAR, Collation.CASE_INSENSITIVE, 2, 20 );

        CatalogTable emps = getTable( schema.id, "emps" );
        addDefaultColumn( csv, emps, "empid", PolySqlType.INTEGER, null, 1, null );
        addDefaultColumn( csv, emps, "deptno", PolySqlType.INTEGER, null, 2, null );
        addDefaultColumn( csv, emps, "name", PolySqlType.VARCHAR, Collation.CASE_INSENSITIVE, 3, 20 );
        addDefaultColumn( csv, emps, "salary", PolySqlType.INTEGER, null, 4, null );
        addDefaultColumn( csv, emps, "commission", PolySqlType.INTEGER, null, 5, null );
    }


    private void addDefaultColumn( CatalogStore csv, CatalogTable table, String name, PolySqlType type, Collation collation, int position, Integer length ) throws GenericCatalogException, UnknownTableException {
        if ( !checkIfExistsColumn( table.id, name ) ) {
            long colId = addColumn( name, table.id, position, type, length, null, false, collation );
            addColumnPlacement( csv.id, colId, PlacementType.AUTOMATIC, null, table.name, name );
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
     * @param db the MapDB database object on which the maps are generated from
     */
    private void initColumnInfo( DB db ) {
        columns = db.hashMap( "columns", Serializer.LONG, new GenericSerializer<CatalogColumn>() ).createOrOpen();
        //noinspection unchecked
        columnNames = db.treeMap( "columnNames", new SerializerArrayTuple( Serializer.LONG, Serializer.LONG, Serializer.LONG, Serializer.STRING ), Serializer.JAVA ).createOrOpen();
        //noinspection unchecked
        columnPlacements = db.treeMap( "columnPlacement", new SerializerArrayTuple( Serializer.INTEGER, Serializer.LONG ), Serializer.JAVA ).createOrOpen();
    }


    private void initTableInfo( DB db ) {
        tables = db.hashMap( "tables", Serializer.LONG, new GenericSerializer<CatalogTable>() ).createOrOpen();
        combinedTables = db.hashMap( "combinedTables", Serializer.LONG, new GenericSerializer<CatalogCombinedTable>() ).createOrOpen();
        tableChildren = db.hashMap( "tableChildren", Serializer.LONG, new GenericSerializer<ImmutableList<Long>>() ).createOrOpen();
        //noinspection unchecked
        tableNames = db.treeMap( "tableNames" )
                .keySerializer( new SerializerArrayTuple( Serializer.LONG, Serializer.LONG, Serializer.STRING ) )
                .valueSerializer( Serializer.JAVA )
                .createOrOpen();
    }


    private void initSchemaInfo( DB db ) {
        schemas = db.hashMap( "schemas", Serializer.LONG, new GenericSerializer<CatalogSchema>() ).createOrOpen();
        combinedSchemas = db.hashMap( "combinedSchemas", Serializer.LONG, new GenericSerializer<CatalogCombinedSchema>() ).createOrOpen();
        schemaChildren = db.hashMap( "schemaChildren", Serializer.LONG, new GenericSerializer<ImmutableList<Long>>() ).createOrOpen();
        //noinspection unchecked
        schemaNames = db.treeMap( "schemaNames", new SerializerArrayTuple( Serializer.LONG, Serializer.STRING ), Serializer.JAVA ).createOrOpen();
    }


    private void initDatabaseInfo( DB db ) {
        databases = db.hashMap( "databases", Serializer.LONG, new GenericSerializer<CatalogDatabase>() ).createOrOpen();
        combinedDatabases = db.hashMap( "combined", Serializer.LONG, new GenericSerializer<CatalogCombinedDatabase>() ).createOrOpen();
        databaseNames = db.hashMap( "databaseNames", Serializer.STRING, new GenericSerializer<CatalogDatabase>() ).createOrOpen();
        databaseChildren = db.hashMap( "databaseChildren", Serializer.LONG, new GenericSerializer<ImmutableList<Long>>() ).createOrOpen();
    }


    @Override
    public void close() {
        db.close();
    }


    @Override
    public void clear() {
        db.getAll().clear();
    }


    /**
     * Inserts a new database,
     * if a database with the same name already exists, it throws an error // TODO should it?
     *
     * @return the id of the newly inserted database
     */
    public long addDatabase( String name, int ownerId, String ownerName, long defaultSchemaId, String defaultSchemaName ) throws GenericCatalogException {
        long id = databaseIdBuilder.getAndIncrement();
        CatalogDatabase database = new CatalogDatabase( id, name, ownerId, ownerName, defaultSchemaId, defaultSchemaName );
        databases.put( id, database );
        databaseNames.put( name, database );
        databaseChildren.put( id, ImmutableList.<Long>builder().build() );

        combinedDatabases.put( id, buildCombinedDatabase( database ) );

        observers.firePropertyChange( "database", null, database );
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
        try {
            if ( pattern != null ) {
                if ( pattern.containsWildcards ) {
                    return databaseNames.entrySet().stream().filter( e -> e.getKey().matches( pattern.toRegex() ) ).map( Entry::getValue ).collect( Collectors.toList() );
                } else {
                    return Collections.singletonList( Objects.requireNonNull( databaseNames.get( pattern.pattern ) ) );
                }
            } else {
                return new ArrayList<>( databases.values() );
            }
        } catch ( NullPointerException e ) {
            e.printStackTrace();
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
        try {
            return databaseNames.get( databaseName );
        } catch ( NullPointerException e ) {
            throw new UnknownDatabaseException( databaseName );
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
    public CatalogDatabase getDatabase( long databaseId ) throws UnknownDatabaseException {
        try {
            return databases.get( databaseId );
        } catch ( NullPointerException e ) {
            throw new UnknownDatabaseException( databaseId );
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
    public List<CatalogSchema> getSchemas( Pattern databaseNamePattern, Pattern schemaNamePattern ) {

        List<CatalogDatabase> catalogDatabases = getDatabases( databaseNamePattern );
        try {
            if ( catalogDatabases.size() > 0 ) {
                Stream<CatalogSchema> catalogSchemas = catalogDatabases.stream().flatMap( d -> Objects.requireNonNull( databaseChildren.get( d.id ) ).stream() ).map( schemas::get );

                if ( schemaNamePattern != null ) {
                    catalogSchemas = catalogSchemas.filter( s -> s.name.matches( schemaNamePattern.toRegex() ) );
                }
                return catalogSchemas.collect( Collectors.toList() );
            }
        } catch ( NullPointerException e ) {
            e.printStackTrace();

        }
        return new ArrayList<>();

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
            try {
                return Collections.singletonList( schemaNames.get( new Object[]{ databaseId, schemaNamePattern.pattern } ) );
            } catch ( NullPointerException e ) {
                throw new UnknownSchemaException( databaseId );
            }
        }

        return new ArrayList<>( schemaNames.prefixSubMap( new Object[]{ databaseId } ).values() );
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
        try {
            return schemaNames.get( new Object[]{ Objects.requireNonNull( databaseNames.get( databaseName ) ).id, schemaName } );
        } catch ( NullPointerException e ) {
            throw new UnknownSchemaException( databaseName, schemaName );
        }

    }


    /**
     * Returns the schema with the given id
     *
     * @param schemaId the schema id
     * @return the catalogSchema
     * @throws UnknownSchemaException if no schema with the given id exists
     */
    public CatalogSchema getSchema( long schemaId ) throws UnknownSchemaException {
        try {
            return schemas.get( schemaId );
        } catch ( NullPointerException e ) {
            throw new UnknownSchemaException( schemaId );
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
    public CatalogSchema getSchema( long databaseId, String schemaName ) throws UnknownSchemaException {
        try {
            return schemaNames.get( new Object[]{ databaseId, schemaName } );
        } catch ( NullPointerException e ) {
            throw new UnknownSchemaException( databaseId, schemaName );
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
     */
    @Override
    public long addSchema( String name, long databaseId, int ownerId, SchemaType schemaType ) throws GenericCatalogException {
        try {

            CatalogDatabase database = databases.get( databaseId );
            CatalogUser owner = users.get( ownerId );
            long id = schemaIdBuilder.getAndIncrement();

            CatalogSchema schema = new CatalogSchema( id, name, databaseId, Objects.requireNonNull( database ).name, ownerId, Objects.requireNonNull( owner ).name, schemaType );
            schemas.put( id, schema );
            schemaNames.put( new Object[]{ databaseId, name }, schema );
            schemaChildren.put( id, ImmutableList.<Long>builder().build() );
            List<Long> children = new ArrayList<>( Objects.requireNonNull( databaseChildren.get( databaseId ) ) );
            children.add( id );
            databaseChildren.replace( databaseId, ImmutableList.copyOf( children ) );

            combinedSchemas.put( id, buildCombinedSchema( schema, database ) );
            combinedDatabases.replace( database.id, buildCombinedDatabase( database ) );
            observers.firePropertyChange( "schema", null, schema );
            return id;
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Checks weather a schema with the specified name exists in a database.
     *
     * @param databaseId The if of the database
     * @param schemaName The name of the schema to check
     * @return True if there is a schema with this name. False if not.
     */
    @Override
    public boolean checkIfExistsSchema( long databaseId, String schemaName ) {
        return schemaNames.containsKey( new Object[]{ databaseId, schemaName } );
    }


    /**
     * Rename a schema
     *
     * @param schemaId The if of the schema to rename
     * @param name New name of the schema
     */
    @Override
    public void renameSchema( long schemaId, String name ) throws GenericCatalogException {
        try {
            CatalogSchema old = Objects.requireNonNull( schemas.get( schemaId ) );
            CatalogSchema schema = CatalogSchema.rename( old, name );

            schemas.replace( schemaId, schema );
            schemaNames.remove( new Object[]{ old.databaseId, old.name } );
            schemaNames.put( new Object[]{ old.databaseId, name }, schema );

            replaceCombinedSchema( schema );
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    private void replaceCombinedSchema( CatalogSchema schema ) throws GenericCatalogException {
        try {
            CatalogDatabase database = getDatabase( schema.databaseId );
            combinedSchemas.replace( schema.id, buildCombinedSchema( schema, database ) );
            combinedDatabases.replace( schema.databaseId, buildCombinedDatabase( database ) );
        } catch ( UnknownDatabaseException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Change owner of a schema
     *
     * @param schemaId The id of the schema which gets its ownerId changed
     * @param ownerId Id of the new owner
     */
    @Override
    public void setSchemaOwner( long schemaId, long ownerId ) throws GenericCatalogException {
        try {
            CatalogSchema old = Objects.requireNonNull( schemas.get( schemaId ) );
            CatalogSchema schema = CatalogSchema.changeOwner( old, (int) ownerId );
            schemas.replace( schemaId, schema );
            schemaNames.replace( new Object[]{ schema.databaseId, schema.name }, schema );

            replaceCombinedSchema( schema );
        } catch ( NullPointerException | GenericCatalogException e ) {
            throw new GenericCatalogException( e );
        }

    }


    /**
     * Delete a schema from the catalog
     *
     * @param schemaId The if of the schema to delete
     */
    @Override
    public void deleteSchema( long schemaId ) throws GenericCatalogException {
        try {
            CatalogSchema schema = Objects.requireNonNull( schemas.get( schemaId ) );
            schemaNames.remove( new Object[]{ schema.databaseId, schema.name } );
            List<Long> oldChildren = new ArrayList<>( Objects.requireNonNull( databaseChildren.get( schema.databaseId ) ) );
            oldChildren.remove( schemaId );
            databaseChildren.replace( schema.databaseId, ImmutableList.copyOf( oldChildren ) );

            for ( Long id : Objects.requireNonNull( schemaChildren.get( schemaId ) ) ) {
                deleteTable( id );
            }

            schemaChildren.remove( schemaId );
            schemas.remove( schemaId );

            combinedSchemas.remove( schemaId );
            combinedDatabases.replace( schema.databaseId, buildCombinedDatabase( getDatabase( schema.databaseId ) ) );
        } catch ( NullPointerException | UnknownDatabaseException e ) {
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
    public List<CatalogTable> getTables( long schemaId, Pattern tableNamePattern ) {
        try {
            CatalogSchema schema = Objects.requireNonNull( schemas.get( schemaId ) );
            if ( tableNamePattern != null ) {
                return Collections.singletonList( tableNames.get( new Object[]{ schema.databaseId, schemaId, tableNamePattern.pattern } ) );
            } else {

                return new ArrayList<>( tableNames.prefixSubMap( new Object[]{ schema.databaseId, schemaId } ).values() );
            }
        } catch ( NullPointerException e ) {
            e.printStackTrace();
        }
        return new ArrayList<>();
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
    public List<CatalogTable> getTables( long databaseId, Pattern schemaNamePattern, Pattern tableNamePattern ) {

        try {
            if ( schemaNamePattern != null && tableNamePattern != null ) {
                CatalogSchema schema = Objects.requireNonNull( schemaNames.get( new Object[]{ databaseId, schemaNamePattern.pattern } ) );
                return Collections.singletonList( Objects.requireNonNull( tableNames.get( new Object[]{ databaseId, schema.id, tableNamePattern.pattern } ) ) );

            } else if ( schemaNamePattern != null ) {
                CatalogSchema schema = Objects.requireNonNull( schemaNames.get( new Object[]{ databaseId, schemaNamePattern.pattern } ) );
                return new ArrayList<>( tableNames.prefixSubMap( new Object[]{ databaseId, schema.id } ).values() );
            } else {
                return new ArrayList<>( tableNames.prefixSubMap( new Object[]{ databaseId } ).values() );
            }
        } catch ( NullPointerException e ) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }


    /**
     * Get all tables of the specified database which fit to the specified filters.
     * <code>
     * ables(xid, databaseName, null, null, null)
     * </code> returns all tables of the database.
     *
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogTable> getTables( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern ) {
        List<CatalogSchema> catalogSchemas = getSchemas( databaseNamePattern, schemaNamePattern );
        try {
            if ( catalogSchemas.size() > 0 ) {
                Stream<CatalogTable> catalogTables = catalogSchemas.stream().flatMap( t -> Objects.requireNonNull( schemaChildren.get( t.id ) ).stream() ).map( tables::get );

                if ( tableNamePattern != null ) {
                    catalogTables = catalogTables.filter( t -> t.name.matches( tableNamePattern.toRegex() ) );
                }
                return catalogTables.collect( Collectors.toList() );
            }
        } catch ( NullPointerException e ) {
            e.printStackTrace();
        }
        return new ArrayList<>();

    }


    @Override
    public CatalogTable getTable( long tableId ) throws UnknownTableException {
        try {
            return Objects.requireNonNull( tables.get( tableId ) );
        } catch ( NullPointerException e ) {
            throw new UnknownTableException( tableId );
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
    public CatalogTable getTable( long schemaId, String tableName ) throws UnknownTableException {
        try {
            CatalogSchema schema = Objects.requireNonNull( schemas.get( schemaId ) );
            return tableNames.get( new Object[]{ schema.databaseId, schemaId, tableName } );
        } catch ( NullPointerException e ) {
            throw new UnknownTableException( schemaId, tableName );
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
    public CatalogTable getTable( long databaseId, String schemaName, String tableName ) throws UnknownTableException {
        try {
            long schemaId = Objects.requireNonNull( schemaNames.get( new Object[]{ databaseId, schemaName } ) ).id;
            return tableNames.get( new Object[]{ databaseId, schemaId, tableName } );
        } catch ( NullPointerException e ) {
            throw new UnknownTableException( databaseId, schemaName, tableName );
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
    public CatalogTable getTable( String databaseName, String schemaName, String tableName ) throws UnknownTableException {
        try {
            long databaseId = Objects.requireNonNull( databaseNames.get( databaseName ) ).id;
            long schemaId = Objects.requireNonNull( schemaNames.get( new Object[]{ databaseId, schemaName } ) ).id;
            return tableNames.get( new Object[]{ databaseId, schemaId, tableName } );
        } catch ( NullPointerException e ) {
            throw new UnknownTableException( databaseName, schemaName, tableName );
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
     * @throws GenericCatalogException when some needed information is not accessible
     */
    @Override
    public long addTable( String name, long schemaId, int ownerId, TableType tableType, String definition ) throws GenericCatalogException {
        try {
            long id = tableIdBuilder.getAndIncrement();
            CatalogSchema schema = Objects.requireNonNull( schemas.get( schemaId ) );
            CatalogDatabase database = Objects.requireNonNull( databases.get( schema.databaseId ) );
            CatalogUser owner = Objects.requireNonNull( users.get( ownerId ) );
            CatalogTable table = new CatalogTable( id, name, schemaId, schema.name, schema.databaseId, schema.databaseName, ownerId, owner.name, tableType, definition, null );

            tables.put( id, table );

            tableChildren.put( id, ImmutableList.<Long>builder().build() );
            tableNames.put( new Object[]{ schema.databaseId, schemaId, name }, table );
            List<Long> children = new ArrayList<>( Objects.requireNonNull( schemaChildren.get( schemaId ) ) );
            children.add( id );
            schemaChildren.replace( schemaId, ImmutableList.copyOf( children ) );

            combinedTables.put( id, buildCombinedTable( table, schema, database ) );
            combinedSchemas.replace( schema.id, buildCombinedSchema( schema, database ) );
            combinedDatabases.replace( database.id, buildCombinedDatabase( database ) );

            observers.firePropertyChange( "table", null, table );
            return id;
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Checks if there is a table with the specified name in the specified schema.
     *
     * @param schemaId The id of the schema
     * @param tableName The name to check for
     * @return true if there is a table with this name, false if not.
     */
    @Override
    public boolean checkIfExistsTable( long schemaId, String tableName ) throws UnknownSchemaException {
        try {
            CatalogSchema schema = Objects.requireNonNull( schemas.get( schemaId ) );
            return tableNames.containsKey( new Object[]{ schema.databaseId, schemaId, tableName } );
        } catch ( NullPointerException e ) {
            throw new UnknownSchemaException( schemaId );
        }
    }


    /**
     * Renames a table
     *
     * @param tableId The if of the table to rename
     * @param name New name of the table
     */
    @Override
    public void renameTable( long tableId, String name ) throws GenericCatalogException {
        try {
            CatalogTable table = Objects.requireNonNull( tables.get( tableId ) );
            tables.replace( tableId, CatalogTable.rename( table, name ) );
            tableNames.remove( new Object[]{ table.databaseId, table.schemaId, table.name } );
            tableNames.put( new Object[]{ table.databaseId, table.schemaId, name }, table );

            replaceCombinedTable( table );

        } catch ( NullPointerException | UnknownDatabaseException | GenericCatalogException | UnknownSchemaException e ) {
            throw new GenericCatalogException( e );
        }
    }


    private void replaceCombinedTable( CatalogTable table ) throws UnknownDatabaseException, UnknownSchemaException, GenericCatalogException {
        CatalogDatabase database = getDatabase( table.databaseId );
        CatalogSchema schema = getSchema( table.schemaId );

        combinedTables.replace( table.id, buildCombinedTable( table, schema, database ) );
        combinedSchemas.replace( schema.id, buildCombinedSchema( schema, database ) );
        combinedDatabases.replace( database.id, buildCombinedDatabase( database ) );
    }


    /**
     * Delete the specified table. Columns, Keys and ColumnPlacements need to be deleted before.
     *
     * @param tableId The id of the table to delete
     */
    @Override
    public void deleteTable( long tableId ) throws GenericCatalogException {
        try {
            CatalogTable table = Objects.requireNonNull( tables.get( tableId ) );
            List<Long> children = new ArrayList<>( Objects.requireNonNull( schemaChildren.get( table.schemaId ) ) );
            children.remove( tableId );
            schemaChildren.replace( table.schemaId, ImmutableList.copyOf( children ) );

            for ( Long columId : Objects.requireNonNull( tableChildren.get( tableId ) ) ) {
                try {
                    deleteColumn( columId );
                } catch ( GenericCatalogException e ) {
                    e.printStackTrace();
                }
            }

            tableChildren.remove( tableId );
            tables.remove( tableId );
            tableNames.remove( new Object[]{ table.databaseId, table.schemaId, table.name } );

            CatalogDatabase database = getDatabase( table.databaseId );
            CatalogSchema schema = getSchema( table.schemaId );

            combinedTables.remove( tableId );
            combinedSchemas.replace( schema.id, buildCombinedSchema( schema, database ) );
            combinedDatabases.replace( database.id, buildCombinedDatabase( database ) );

        } catch ( NullPointerException | UnknownSchemaException | UnknownDatabaseException | GenericCatalogException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Change owner of a table
     *
     * @param tableId The if of the table
     * @param ownerId Id of the new owner
     */
    @Override
    public void setTableOwner( long tableId, int ownerId ) throws GenericCatalogException {
        try {
            CatalogTable table = CatalogTable.replaceOwner( Objects.requireNonNull( tables.get( tableId ) ), ownerId );
            tables.replace( tableId, table );
            tableNames.replace( new Object[]{ table.databaseId, table.schemaId, table.name }, table );

            replaceCombinedTable( table );
        } catch ( NullPointerException | UnknownDatabaseException | UnknownSchemaException | GenericCatalogException e ) {
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
            CatalogTable table = CatalogTable.replacePrimary( Objects.requireNonNull( tables.get( tableId ) ), keyId );
            tables.replace( tableId, table );
            tableNames.replace( new Object[]{ table.databaseId, table.schemaId, table.name }, table );
            if ( keyId != null ) {
                primaryKeys.put( keyId, new CatalogPrimaryKey( Objects.requireNonNull( keys.get( keyId ) ) ) );
            }

            replaceCombinedTable( table );
            updateCombinedKeys( keyId );

        } catch ( NullPointerException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new GenericCatalogException( e );
        }
    }


    private void updateCombinedMaps( CatalogColumn column ) throws GenericCatalogException {
        try {
            CatalogDatabase database = getDatabase( column.databaseId );
            CatalogSchema schema = getSchema( column.schemaId );
            CatalogTable table = getTable( column.tableId );

            combinedTables.replace( table.id, buildCombinedTable( table, schema, database ) );
            combinedSchemas.replace( schema.id, buildCombinedSchema( schema, database ) );
            combinedDatabases.replace( database.id, buildCombinedDatabase( database ) );
        } catch ( UnknownTableException | GenericCatalogException | UnknownSchemaException | UnknownDatabaseException e ) {
            throw new GenericCatalogException( e );
        }
    }


    private void updateCombinedMaps( List<Long> columnIds ) {
        try {
            for ( long id : columnIds ) {
                updateCombinedMaps( getColumn( id ) );
            }
        } catch ( GenericCatalogException | UnknownColumnException e ) {
            e.printStackTrace();
        }
    }


    private void updateCombinedKeys( Long keyId ) {
        try {
            CatalogKey key = keys.get( keyId );
            combinedKeys.replace( keyId, buildCombinedKey( key ) );
        } catch ( GenericCatalogException e ) {
            e.printStackTrace();
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
            CatalogColumn column = Objects.requireNonNull( columns.get( columnId ) );
            CatalogStore store = Objects.requireNonNull( stores.get( storeId ) );

            columnPlacements.put( new Object[]{ storeId, columnId }, new CatalogColumnPlacement( column.tableId, column.tableName, columnId, column.name, storeId, store.uniqueName, placementType, physicalSchemaName, physicalTableName, physicalColumnName ) );

            updateCombinedMaps( column );
        } catch ( NullPointerException e ) {
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
    public void deleteColumnPlacement( int storeId, long columnId ) {
        columnPlacements.remove( new Object[]{ storeId, columnId } );

        try {
            updateCombinedMaps( getColumn( columnId ) );
        } catch ( GenericCatalogException | UnknownColumnException e ) {
            e.printStackTrace();
        }
    }


    /**
     * Get column placements on a store
     *
     * @param storeId The id of the store
     * @return List of column placements on this store
     */
    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnStore( int storeId ) {

        return new ArrayList<>( columnPlacements.prefixSubMap( new Object[]{ storeId } ).values() );
    }


    public List<CatalogColumnPlacement> getColumnPlacementsOnStore( int storeId, long tableId ) {
        return getColumnPlacementsOnStore( storeId ).stream().filter( p -> p.tableId == tableId ).collect( Collectors.toList() );
    }


    /**
     * Get all column placements of a column
     *
     * @param columnId the id of the specific column
     * @return List of column placements of specific column
     */
    @Override
    public List<CatalogColumnPlacement> getColumnPlacements( long columnId ) {
        return columnPlacements.values().stream().filter( p -> p.columnId == columnId ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnStoreAndSchema( int storeId, long schemaId ) throws GenericCatalogException {
        try {
            return getColumnPlacementsOnStore( storeId ).stream().filter( p -> Objects.requireNonNull( columns.get( p.columnId ) ).schemaId == schemaId ).collect( Collectors.toList() );
        } catch ( NullPointerException e ) {
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
    public void updateColumnPlacementPhysicalNames( int storeId, long columnId, String physicalSchemaName, String physicalTableName, String physicalColumnName ) throws UnknownColumnPlacementException {
        try {
            CatalogColumnPlacement old = Objects.requireNonNull( columnPlacements.get( new Object[]{ storeId, columnId } ) );
            CatalogColumnPlacement placement = CatalogColumnPlacement.replacePhysicalNames( old, physicalSchemaName, physicalTableName, physicalColumnName );
            columnPlacements.put( new Object[]{ storeId, columnId }, placement );

            updateCombinedMaps( getColumn( placement.columnId ) );
        } catch ( NullPointerException | UnknownColumnException | GenericCatalogException e ) {
            throw new UnknownColumnPlacementException( storeId, columnId );
        }
    }


    /**
     * Get all columns of the specified table.
     *
     * @param tableId The id of the table
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogColumn> getColumns( long tableId ) throws UnknownTableException {
        try {
            CatalogTable table = Objects.requireNonNull( tables.get( tableId ) );
            return new ArrayList<>( columnNames.prefixSubMap( new Object[]{ table.databaseId, table.schemaId, table.id } ).values() );
        } catch ( NullPointerException e ) {
            throw new UnknownTableException( tableId );
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
    public List<CatalogColumn> getColumns( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern, Pattern columnNamePattern ) {

        List<CatalogTable> catalogTables = getTables( databaseNamePattern, schemaNamePattern, tableNamePattern );
        try {
            if ( catalogTables.size() > 0 ) {
                Stream<CatalogColumn> catalogColumns = catalogTables.stream().flatMap( t -> Objects.requireNonNull( tableChildren.get( t.id ) ).stream() ).map( columns::get );

                if ( columnNamePattern != null ) {
                    catalogColumns = catalogColumns.filter( c -> c.name.matches( columnNamePattern.toRegex() ) );
                }
                return catalogColumns.collect( Collectors.toList() );
            }
        } catch ( NullPointerException e ) {
            e.printStackTrace();

        }
        return new ArrayList<>();
    }


    /**
     * Returns the column with the specified id.
     *
     * @param columnId The id of the column
     * @return A CatalogColumn
     * @throws UnknownColumnException If there is no column with this id
     */
    @Override
    public CatalogColumn getColumn( long columnId ) throws UnknownColumnException {
        try {
            return Objects.requireNonNull( columns.get( columnId ) );
        } catch ( NullPointerException e ) {
            throw new UnknownColumnException( columnId );
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
    public CatalogColumn getColumn( long tableId, String columnName ) throws UnknownColumnException {
        try {
            CatalogTable table = Objects.requireNonNull( tables.get( tableId ) );
            return columnNames.get( new Object[]{ table.databaseId, table.schemaId, table.id, columnName } );
        } catch ( NullPointerException e ) {
            throw new UnknownColumnException( tableId, columnName );
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
    public CatalogColumn getColumn( String databaseName, String schemaName, String tableName, String columnName ) throws UnknownColumnException {

        try {
            CatalogTable table = getTable( databaseName, schemaName, tableName );
            return columnNames.get( new Object[]{ table.databaseId, table.schemaId, table.id, columnName } );
        } catch ( UnknownTableException | NullPointerException e ) {
            throw new UnknownColumnException( databaseName, schemaName, tableName, columnName );
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
            CatalogTable table = Objects.requireNonNull( tables.get( tableId ) );

            if ( type.isCharType() && collation == null ) {
                throw new RuntimeException( "Collation is not allowed to be null for char types." );
            }
            if ( scale != null && scale > length ) {
                throw new RuntimeException( "Invalid scale! Scale can not be larger than length." );
            }

            long id = columnIdBuilder.getAndIncrement();
            CatalogColumn column = new CatalogColumn( id, name, tableId, table.name, table.schemaId, table.schemaName, table.databaseId, table.databaseName, position, type, length, scale, nullable, collation, null );
            columns.put( id, column );
            columnNames.put( new Object[]{ table.databaseId, table.schemaId, table.id, name }, column );
            List<Long> children = new ArrayList<>( Objects.requireNonNull( tableChildren.get( tableId ) ) );
            children.add( id );
            tableChildren.replace( tableId, ImmutableList.copyOf( children ) );

            updateCombinedMaps( column );

            observers.firePropertyChange( "column", null, column );
            return id;
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Renames a column
     *
     * @param columnId The if of the column to rename
     * @param name New name of the column
     */
    @Override
    public void renameColumn( long columnId, String name ) throws GenericCatalogException {
        try {
            CatalogColumn old = Objects.requireNonNull( columns.get( columnId ) );
            CatalogColumn column = CatalogColumn.replaceName( old, name );
            columns.replace( columnId, column );
            columnNames.remove( new Object[]{ column.databaseId, column.schemaId, column.tableId, old.name } );
            columnNames.put( new Object[]{ column.databaseId, column.schemaId, column.tableId, name }, column );

            updateCombinedMaps( column );
        } catch ( NullPointerException e ) {
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
    public void setColumnPosition( long columnId, int position ) throws UnknownColumnException {
        try {
            CatalogColumn old = Objects.requireNonNull( columns.get( columnId ) );
            CatalogColumn column = CatalogColumn.replacePosition( old, position );
            columns.replace( columnId, column );
            columnNames.replace( new Object[]{ column.databaseId, column.schemaId, column.tableId, column.name }, column );

            updateCombinedMaps( column );
        } catch ( NullPointerException | GenericCatalogException e ) {
            throw new UnknownColumnException( columnId );
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

            CatalogColumn column = Objects.requireNonNull( columns.get( columnId ) );

            if ( scale != null && scale > length ) {
                throw new RuntimeException( "Invalid scale! Scale can not be larger than length." );
            }
            Collation collation = type.isCharType() ? Collation.getById( RuntimeConfig.DEFAULT_COLLATION.getInteger() ) : null;

            columns.replace( columnId, CatalogColumn.replaceColumnType( column, type, length, scale, collation ) );

            updateCombinedMaps( column );

        } catch ( NullPointerException | UnknownCollationException e ) {
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
            CatalogColumn old = Objects.requireNonNull( columns.get( columnId ) );
            if ( nullable ) {
                // Check if the column is part of a primary key (pk's are not allowed to contain null values)
                CatalogTable table = Objects.requireNonNull( tables.get( old.tableId ) );
                if ( table.primaryKey != null ) {
                    CatalogKey catalogKey = getPrimaryKey( table.primaryKey );
                    if ( catalogKey.columnIds.contains( columnId ) ) {
                        throw new GenericCatalogException( "Unable to allow null values in a column that is part of the primary key." );
                    }
                }
            } else {
                // TODO: Check that the column does not contain any null values
                getColumnPlacements( columnId );
            }
            CatalogColumn column = CatalogColumn.replaceNullable( old, nullable );
            columns.replace( columnId, column );
            columnNames.replace( new Object[]{ old.databaseId, old.schemaId, old.tableId, old.name }, column );

            updateCombinedMaps( column );
        } catch ( NullPointerException | UnknownKeyException e ) {
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
    public void setCollation( long columnId, Collation collation ) throws UnknownColumnException {
        try {
            CatalogColumn column = Objects.requireNonNull( columns.get( columnId ) );

            if ( !column.type.isCharType() ) {
                throw new RuntimeException( "Illegal attempt to set collation for a non-char column!" );
            }

            columns.replace( columnId, CatalogColumn.replaceCollation( column, collation ) );

            updateCombinedMaps( column );
        } catch ( NullPointerException | GenericCatalogException e ) {
            throw new UnknownColumnException( columnId );
        }
    }


    /**
     * Checks if there is a column with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param columnName The name to check for
     * @return true if there is a column with this name, false if not.
     */
    @Override
    public boolean checkIfExistsColumn( long tableId, String columnName ) throws UnknownTableException {
        try {
            CatalogTable table = Objects.requireNonNull( tables.get( tableId ) );
            return columnNames.containsKey( new Object[]{ table.databaseId, table.schemaId, tableId, columnName } );
        } catch ( NullPointerException e ) {
            throw new UnknownTableException( tableId );
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
            //TODO also delete keys with that column?
            CatalogColumn column = Objects.requireNonNull( columns.get( columnId ) );

            columnNames.remove( new Object[]{ column.databaseId, column.schemaId, column.tableId, column.name } );
            List<Long> children = new ArrayList<>( Objects.requireNonNull( tableChildren.get( column.tableId ) ) );
            children.remove( columnId );
            tableChildren.replace( column.tableId, ImmutableList.copyOf( children ) );

            deleteDefaultValue( columnId );
            getColumnPlacements( columnId ).forEach( p -> deleteColumnPlacement( p.storeId, p.columnId ) );

            columns.remove( columnId );

            updateCombinedMaps( column );
        } catch ( NullPointerException | UnknownColumnException e ) {
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
    public void setDefaultValue( long columnId, PolySqlType type, String defaultValue ) throws UnknownColumnException {
        try {
            CatalogColumn old = Objects.requireNonNull( columns.get( columnId ) );
            // TODO DL also fix call
            CatalogColumn column = CatalogColumn.replaceDefaultValue( old, new CatalogDefaultValue( columnId, type, defaultValue, "defaultValue" ) );
            columns.replace( columnId, column );
            columnNames.replace( new Object[]{ column.databaseId, column.schemaId, column.tableId, column.name }, column );

            updateCombinedMaps( column );
        } catch ( NullPointerException | GenericCatalogException e ) {
            throw new UnknownColumnException( columnId );
        }
    }


    /**
     * Deletes an existing default value of a column. NoOp if there is no default value defined.
     *
     * @param columnId The id of the column
     */
    @Override
    public void deleteDefaultValue( long columnId ) throws UnknownColumnException {
        try {
            CatalogColumn column = Objects.requireNonNull( columns.get( columnId ) );
            if ( column.defaultValue != null ) {
                columns.replace( columnId, CatalogColumn.replaceDefaultValue( column, null ) );
                updateCombinedMaps( column );
            }
        } catch ( NullPointerException | GenericCatalogException e ) {
            throw new UnknownColumnException( columnId );
        }
    }


    /**
     * Returns a specified primary key
     *
     * @param key The id of the primary key
     * @return The primary key
     */
    @Override
    public CatalogPrimaryKey getPrimaryKey( long key ) throws UnknownKeyException {
        try {
            return primaryKeys.get( key );
        } catch ( NullPointerException e ) {
            throw new UnknownKeyException( key );
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
            // Check if the columns are set 'not null'
            List<CatalogColumn> nullableColumns = columnIds.stream().map( columns::get ).filter( Objects::nonNull ).filter( c -> c.nullable ).collect( Collectors.toList() );
            for ( CatalogColumn col : nullableColumns ) {
                throw new GenericCatalogException( "Primary key is not allowed to contain null values but the column '" + col.name + "' is declared nullable." );
            }

            // TODO: Check if the current values are unique

            // Check if there is already a primary key defined for this table and if so, delete it.
            CatalogTable table = Objects.requireNonNull( tables.get( tableId ) );

            if ( table.primaryKey != null ) {
                CatalogCombinedKey combinedKey = getCombinedKey( table.primaryKey );
                if ( combinedKey.getUniqueCount() == 1 && combinedKey.getReferencedBy().size() > 0 ) {
                    // This primary key is the only constraint for the uniqueness of this key.
                    throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. To drop this primary key, first drop the foreign keys or create a unique constraint." );
                }
                setPrimaryKey( tableId, null );
                deleteKeyIfNoLongerUsed( table.primaryKey );
            }
            long keyId = getOrAddKey( tableId, columnIds );
            setPrimaryKey( tableId, keyId );

            for ( Long columnId : columnIds ) {
                updateCombinedMaps( getColumn( columnId ) );
            }
        } catch ( NullPointerException | UnknownColumnException e ) {
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
    public List<CatalogForeignKey> getForeignKeys( long tableId ) {
        return foreignKeys.values().stream().filter( f -> f.tableId == tableId ).collect( Collectors.toList() );
    }


    /**
     * Returns all foreign keys that reference the specified table (exported keys).
     *
     * @param tableId The id of the table
     * @return List of foreign keys
     */
    @Override
    public List<CatalogForeignKey> getExportedKeys( long tableId ) {
        return foreignKeys.values().stream().filter( k -> k.referencedKeyTableId == tableId ).collect( Collectors.toList() );
    }


    /**
     * Get all constraints of the specified table
     *
     * @param tableId The id of the table
     * @return List of constraints
     */
    @Override
    public List<CatalogConstraint> getConstraints( long tableId ) {
        List<Long> keysOfTable = keys.values().stream().filter( k -> k.tableId == tableId ).map( k -> k.id ).collect( Collectors.toList() );
        return constraints.values().stream().filter( c -> keysOfTable.contains( c.keyId ) ).collect( Collectors.toList() );
    }


    /**
     * Returns the constraint with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param constraintName The name of the constraint
     * @return The constraint
     */
    @Override
    public CatalogConstraint getConstraint( long tableId, String constraintName ) throws UnknownConstraintException {
        try {
            return constraints.values().stream().filter( c -> c.key.tableId == tableId && c.name.equals( constraintName ) ).findFirst().orElseThrow( NullPointerException::new );
        } catch ( NullPointerException e ) {
            throw new UnknownConstraintException( tableId, constraintName );
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
    public CatalogForeignKey getForeignKey( long tableId, String foreignKeyName ) throws UnknownForeignKeyException {
        try {
            return foreignKeys.values().stream().filter( f -> f.tableId == tableId && f.name.equals( foreignKeyName ) ).findFirst().orElseThrow( NullPointerException::new );
        } catch ( NullPointerException e ) {
            throw new UnknownForeignKeyException( tableId, foreignKeyName );
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
            CatalogTable table = Objects.requireNonNull( tables.get( tableId ) );
            List<CatalogKey> childKeys = keys.values().stream().filter( k -> k.tableId == referencesTableId ).collect( Collectors.toList() );

            for ( CatalogKey refKey : childKeys ) {
                if ( refKey.columnIds.size() == referencesIds.size() && refKey.columnIds.containsAll( referencesIds ) && referencesIds.containsAll( refKey.columnIds ) ) {

                    CatalogCombinedKey combinedKey = getCombinedKey( refKey.id );

                    int i = 0;
                    for ( CatalogColumn referencedColumn : combinedKey.getColumns() ) {
                        CatalogColumn referencingColumn = Objects.requireNonNull( columns.get( columnIds.get( i++ ) ) );
                        if ( referencedColumn.type != referencingColumn.type ) {
                            throw new GenericCatalogException( "The data type of the referenced columns does not match the data type of the referencing column: " + referencingColumn.type.name() + " != " + referencedColumn.type );
                        }
                    }
                    // TODO same keys for key and foreignkey
                    if ( combinedKey.getUniqueCount() > 0 ) {
                        long keyId = getOrAddKey( tableId, columnIds );
                        List<String> keyColumnNames = columnIds.stream().map( id -> Objects.requireNonNull( columns.get( id ) ).name ).collect( Collectors.toList() );
                        List<String> referencesNames = referencesIds.stream().map( id -> Objects.requireNonNull( columns.get( id ) ).name ).collect( Collectors.toList() );
                        CatalogForeignKey key = new CatalogForeignKey( keyId, constraintName, tableId, table.name, table.schemaId, table.schemaName, table.databaseId, table.databaseName, refKey.id, refKey.tableId, refKey.tableName, refKey.schemaId, refKey.schemaName, refKey.databaseId, refKey.databaseName, columnIds, keyColumnNames, referencesIds, referencesNames, onUpdate, onDelete );

                        foreignKeys.put( keyId, key );

                        updateCombinedMaps( columnIds );
                        updateCombinedKeys( keyId );

                        return;
                    }

                }

            }
        } catch ( NullPointerException e ) {
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
        // TODO DL check with statements
        try {
            long keyId = getOrAddKey( tableId, columnIds );
            // Check if there is already a unique constraint
            List<CatalogConstraint> catalogConstraints = constraints.values().stream().filter( c -> c.keyId == keyId && c.type == ConstraintType.UNIQUE ).collect( Collectors.toList() );
            if ( catalogConstraints.size() > 0 ) {
                throw new GenericCatalogException( "There is already a unique constraint!" );
            }
            long id = constraintIdBuilder.getAndIncrement();
            constraints.put( id, new CatalogConstraint( id, keyId, ConstraintType.UNIQUE, constraintName, Objects.requireNonNull( keys.get( keyId ) ) ) );

            for ( Long columnId : columnIds ) {
                updateCombinedMaps( getColumn( columnId ) );
            }
            updateCombinedKeys( keyId );
        } catch ( NullPointerException | UnknownColumnException e ) {
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
    public List<CatalogIndex> getIndexes( long tableId, boolean onlyUnique ) {
        if ( !onlyUnique ) {
            return indices.values().stream().filter( i -> i.key.tableId == tableId ).collect( Collectors.toList() );
        } else {
            return indices.values().stream().filter( i -> i.key.tableId == tableId && i.unique ).collect( Collectors.toList() );
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
    public CatalogIndex getIndex( long tableId, String indexName ) throws UnknownIndexException {
        try {
            return indices.values().stream().filter( i -> i.key.tableId == tableId && i.name.equals( indexName ) ).findFirst().orElseThrow( NullPointerException::new );
        } catch ( NullPointerException e ) {
            throw new UnknownIndexException( tableId, indexName );
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
        long keyId = getOrAddKey( tableId, columnIds );
        if ( unique ) {
            // TODO DL: Check if the current values are unique
        }
        long id = indexIdBuilder.getAndIncrement();
        indices.put( id, new CatalogIndex( id, indexName, unique, type, null, keyId, Objects.requireNonNull( keys.get( keyId ) ) ) );
        updateCombinedKeys( keyId );
        return id;
    }


    /**
     * Delete the specified index
     *
     * @param indexId The id of the index to drop
     */
    @Override
    public void deleteIndex( long indexId ) throws GenericCatalogException {
        try {
            CatalogIndex index = Objects.requireNonNull( indices.get( indexId ) );
            if ( index.unique ) {
                CatalogCombinedKey combinedKey = getCombinedKey( index.keyId );
                if ( combinedKey.getUniqueCount() == 1 && combinedKey.getReferencedBy().size() > 0 ) {
                    // This unique index is the only constraint for the uniqueness of this key.
                    throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. To delete this index, first add a unique constraint." );
                }
            }
            indices.remove( indexId );
            deleteKeyIfNoLongerUsed( index.keyId );
        } catch ( NullPointerException e ) {
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
            CatalogTable table = Objects.requireNonNull( tables.get( tableId ) );

            // TODO: Check if the currently stored values are unique
            if ( table.primaryKey != null ) {
                // Check if this primary key is required to maintain to uniqueness
                CatalogCombinedKey key = getCombinedKey( table.primaryKey );
                if ( key.getReferencedBy().size() > 0 ) {
                    if ( key.getUniqueCount() < 2 ) {
                        throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. To drop this primary key either drop the foreign key or create an unique constraint." );
                    }
                }

                setPrimaryKey( tableId, null );
                deleteKeyIfNoLongerUsed( table.primaryKey );

                replaceCombinedTable( table );
            }
        } catch ( NullPointerException | UnknownDatabaseException | UnknownSchemaException e ) {
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
            CatalogForeignKey catalogForeignKey = Objects.requireNonNull( foreignKeys.get( foreignKeyId ) );
            foreignKeys.remove( catalogForeignKey.id );
            deleteKeyIfNoLongerUsed( catalogForeignKey.id );
        } catch ( NullPointerException e ) {
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
            CatalogConstraint catalogConstraint = Objects.requireNonNull( constraints.get( constraintId ) );

            CatalogCombinedKey key = getCombinedKey( catalogConstraint.keyId );
            if ( catalogConstraint.type == ConstraintType.UNIQUE && key.getReferencedBy().size() > 0 ) {
                if ( key.getUniqueCount() < 2 ) {
                    throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. Unable to drop unique constraint." );
                }
            }
            constraints.remove( catalogConstraint.id );
            deleteKeyIfNoLongerUsed( key.getKey().id );
        } catch ( NullPointerException e ) {
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
    public CatalogUser getUser( String userName ) throws UnknownUserException {
        try {
            return userNames.get( userName );
        } catch ( NullPointerException e ) {
            throw new UnknownUserException( userName );
        }
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
    public List<CatalogStore> getStores() {
        return new ArrayList<>( stores.values() );
    }


    /**
     * Get a store by its unique name
     *
     * @return List of stores
     */
    @Override
    public CatalogStore getStore( String uniqueName ) throws UnknownStoreException {
        uniqueName = uniqueName.toLowerCase();
        try {
            return Objects.requireNonNull( storeNames.get( uniqueName ) );
        } catch ( NullPointerException e ) {
            throw new UnknownStoreException( uniqueName );
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
    public int addStore( String uniqueName, String adapter, Map<String, String> settings ) {
        uniqueName = uniqueName.toLowerCase();

        int id = storeIdBuilder.getAndIncrement();
        CatalogStore store = new CatalogStore( id, uniqueName, adapter, settings );
        stores.put( id, store );
        storeNames.put( uniqueName, store );
        return id;
    }


    /**
     * Delete a store
     *
     * @param storeId The id of the store to delete
     */
    @Override
    public void deleteStore( int storeId ) throws UnknownStoreException {
        // TODO remove database/schemas/... as well? affectedRow?
        try {
            CatalogStore store = Objects.requireNonNull( stores.get( storeId ) );
            stores.remove( storeId );
            storeNames.remove( store.uniqueName );
        } catch ( NullPointerException e ) {
            throw new UnknownStoreException( storeId );
        }
    }


    @Override
    public CatalogCombinedDatabase getCombinedDatabase( long databaseId ) throws GenericCatalogException {
        if ( combinedDatabases.containsKey( databaseId ) ) {
            return combinedDatabases.get( databaseId );
        } else {
            throw new GenericCatalogException( "Combined database does not exist: " + databaseId );
        }

    }


    /**
     * Builds a complex/expensive combined database object
     *
     * @param database the "simple" database object
     * @return the combined database object
     */
    private CatalogCombinedDatabase buildCombinedDatabase( CatalogDatabase database ) throws GenericCatalogException {
        List<CatalogCombinedSchema> childSchemas = new ArrayList<>();
        for ( CatalogSchema s : schemaNames.prefixSubMap( new Object[]{ database.id } ).values() ) {
            CatalogCombinedSchema combinedSchema = getCombinedSchema( s.id );
            childSchemas.add( combinedSchema );
        }

        CatalogSchema defaultSchema = null;
        if ( database.defaultSchemaId != null ) {
            defaultSchema = schemas.get( database.defaultSchemaId );
        }

        CatalogUser owner = users.get( database.ownerId );
        return new CatalogCombinedDatabase( database, childSchemas, defaultSchema, owner );
    }


    // TODO move
    public List<CatalogKey> getKeys() {
        return new ArrayList<>( keys.values() );
    }


    @Override
    public CatalogCombinedSchema getCombinedSchema( long schemaId ) {
        return combinedSchemas.get( schemaId );
    }


    private CatalogCombinedSchema buildCombinedSchema( CatalogSchema schema, CatalogDatabase database ) throws GenericCatalogException {
        try {
            List<CatalogCombinedTable> childTables = new ArrayList<>();

            for ( long id : Objects.requireNonNull( schemaChildren.get( schema.id ) ) ) {
                CatalogCombinedTable combinedTable = getCombinedTable( id );
                if ( combinedTable != null ) {
                    childTables.add( combinedTable );
                }
            }

            CatalogUser owner = getUser( schema.ownerId );

            return new CatalogCombinedSchema( schema, childTables, database, owner );
        } catch ( NullPointerException | UnknownUserException e ) {
            throw new GenericCatalogException( e );
        }

    }


    @Override
    public CatalogCombinedTable getCombinedTable( long tableId ) {
        return combinedTables.get( tableId );
    }


    public CatalogCombinedTable buildCombinedTable( CatalogTable table, CatalogSchema schema, CatalogDatabase database ) throws GenericCatalogException {
        try {
            List<CatalogColumn> childColumns = new ArrayList<>( columnNames.prefixSubMap( new Object[]{ table.databaseId, table.schemaId, table.id } ).values() );

            Map<Integer, List<CatalogColumnPlacement>> columnPlacementByStore = new HashMap<>();
            stores.keySet().forEach( id -> {
                List<CatalogColumnPlacement> placement = getColumnPlacementsOnStore( id, table.id );
                if ( placement.size() > 0 ) {
                    columnPlacementByStore.put( id, placement );
                }
            } );

            Map<Long, List<CatalogColumnPlacement>> columnPlacementByColumn = new HashMap<>();

            childColumns.forEach( c -> columnPlacementByColumn.put( c.id, getColumnPlacements( c.id ) ) );

            List<CatalogKey> tableKeys = getKeys().stream().filter( k -> k.tableId == table.id ).collect( Collectors.toList() );
            CatalogUser owner = users.get( table.ownerId );

            return new CatalogCombinedTable( table, childColumns, schema, database, owner, columnPlacementByStore, columnPlacementByColumn, tableKeys );
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }

    }


    @Override
    public CatalogCombinedKey getCombinedKey( long keyId ) {
        return combinedKeys.get( keyId );
    }


    public CatalogCombinedKey buildCombinedKey( CatalogKey key ) throws GenericCatalogException {
        try {
            List<CatalogColumn> childColumns = key.columnIds.stream().map( columns::get ).filter( Objects::nonNull ).collect( Collectors.toList() );
            CatalogTable table = Objects.requireNonNull( tables.get( key.tableId ) );
            CatalogSchema schema = Objects.requireNonNull( schemas.get( key.schemaId ) );
            CatalogDatabase database = Objects.requireNonNull( databases.get( key.databaseId ) );
            List<CatalogForeignKey> childForeignKeys = foreignKeys.values().stream().filter( f -> f.referencedKeyId == key.id ).collect( Collectors.toList() );
            List<CatalogIndex> childIndices = indices.values().stream().filter( i -> i.keyId == key.id ).collect( Collectors.toList() );
            List<CatalogConstraint> childConstraints = constraints.values().stream().filter( c -> c.keyId == key.id ).collect( Collectors.toList() );

            return new CatalogCombinedKey( key, childColumns, table, schema, database, childForeignKeys, childIndices, childConstraints, childForeignKeys );
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    // Check if the specified key is used as primary key, index or constraint. If so, this is a NoOp. If it is not used, the key is deleted.
    private void deleteKeyIfNoLongerUsed( Long keyId ) throws GenericCatalogException {
        if ( keyId == null ) {
            return;
        }
        try {
            CatalogKey key = Objects.requireNonNull( keys.get( keyId ) );
            CatalogTable table = Objects.requireNonNull( tables.get( key.tableId ) );
            if ( table.primaryKey != null && table.primaryKey.equals( keyId ) ) {
                return;
            }
            if ( constraints.values().stream().anyMatch( c -> c.keyId == keyId ) ) {
                return;
            }
            if ( foreignKeys.values().stream().anyMatch( f -> f.referencedKeyId == keyId ) ) {
                return;
            }
            if ( indices.values().stream().anyMatch( i -> i.keyId == keyId ) ) {
                return;
            }
            keys.remove( keyId );
            combinedKeys.remove( keyId );
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
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
    }


    private long addKey( long tableId, List<Long> columnIds ) throws GenericCatalogException {
        try {
            CatalogTable table = Objects.requireNonNull( tables.get( tableId ) );
            long id = keyIdBuilder.getAndIncrement();
            List<String> names = columnIds.stream().map( columns::get ).filter( Objects::nonNull ).map( c -> c.name ).collect( Collectors.toList() );
            CatalogKey key = new CatalogKey( id, table.id, table.name, table.schemaId, table.schemaName, table.databaseId, table.databaseName, columnIds, names );
            keys.put( id, key );
            combinedKeys.put( id, buildCombinedKey( key ) );

            updateCombinedMaps( columnIds );

            observers.firePropertyChange( "key", null, key );
            return id;
        } catch ( NullPointerException | GenericCatalogException e ) {
            throw new GenericCatalogException( e );
        }
    }


}

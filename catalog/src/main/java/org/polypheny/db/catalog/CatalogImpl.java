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
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import org.mapdb.DBException.SerializationError;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;
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
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownColumnIdRuntimeException;
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
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.FileSystemManager;


@Slf4j
public class CatalogImpl extends Catalog {


    private static final String FILE_PATH = "mapDB";
    private static DB db;

    private static HTreeMap<Integer, CatalogUser> users;
    private static HTreeMap<String, CatalogUser> userNames;

    private static BTreeMap<Long, CatalogDatabase> databases;
    private static BTreeMap<String, CatalogDatabase> databaseNames;
    private static HTreeMap<Long, ImmutableList<Long>> databaseChildren;


    private static BTreeMap<Long, CatalogSchema> schemas;
    private static BTreeMap<Object[], CatalogSchema> schemaNames;
    private static HTreeMap<Long, ImmutableList<Long>> schemaChildren;


    private static BTreeMap<Long, CatalogTable> tables;
    private static BTreeMap<Object[], CatalogTable> tableNames;
    private static HTreeMap<Long, ImmutableList<Long>> tableChildren;

    private static BTreeMap<Long, CatalogColumn> columns;
    private static BTreeMap<Object[], CatalogColumn> columnNames;
    private static BTreeMap<Object[], CatalogColumnPlacement> columnPlacements;

    private static HTreeMap<Integer, CatalogStore> stores;
    private static HTreeMap<String, CatalogStore> storeNames;

    private static HTreeMap<Long, CatalogKey> keys;
    private static HTreeMap<long[], Long> keyColumns;

    private static HTreeMap<Long, CatalogPrimaryKey> primaryKeys;
    private static HTreeMap<Long, CatalogForeignKey> foreignKeys;
    private static HTreeMap<Long, CatalogConstraint> constraints;
    private static HTreeMap<Long, CatalogIndex> indexes;

    private static Long openTable;


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

    private static final AtomicLong physicalPositionBuilder = new AtomicLong();

    private final String path;
    Comparator<CatalogColumn> columnComparator = Comparator.comparingInt( o -> o.position );
    // TODO DL check solution for all


    public CatalogImpl() {
        this( FILE_PATH, true, true, false );
    }


    /**
     * Creates a new catalog after the given parameters
     *
     * @param fileName name of persistent catalog file
     * @param doInitSchema if the default schema is initiated
     * @param doInitInformationPage if a new informationPage should be created
     * @param deleteAfter if the file is deleted when the catalog is closed
     */
    public CatalogImpl( String fileName, boolean doInitSchema, boolean doInitInformationPage, boolean deleteAfter ) {
        super();
        this.path = fileName;

        if ( db != null ) {
            db.close();
        }
        synchronized ( this ) {

            if ( Catalog.memoryCatalog || Catalog.testMode ) {
                isPersistent = false;
            } else {
                isPersistent = isPersistent();
            }

            if ( isPersistent ) {
                log.info( "Making the catalog persistent." );
                File folder = FileSystemManager.getInstance().registerDataFolder( "catalog" );

                if ( Catalog.resetCatalog ) {
                    log.info( "Reseting catalog on startup." );
                    if ( new File( folder, fileName ).exists() ) {
                        //noinspection ResultOfMethodCallIgnored
                        new File( folder, fileName ).delete();
                    }
                }

                if ( !deleteAfter ) {

                    db = DBMaker
                            .fileDB( new File( folder, fileName ) )
                            .closeOnJvmShutdown()
                            .transactionEnable()
                            .fileMmapEnableIfSupported()
                            .fileMmapPreclearDisable()
                            .make();
                } else {
                    db = DBMaker
                            .fileDB( new File( folder, fileName ) )
                            .closeOnJvmShutdown()
                            .fileDeleteAfterClose()
                            .transactionEnable()
                            .fileMmapEnableIfSupported()
                            .fileMmapPreclearDisable()
                            .make();
                }
                db.getStore().fileLoad();

            } else {
                log.info( "Making the catalog in-memory." );
                db = DBMaker
                        .memoryDB()
                        .transactionEnable()
                        .closeOnJvmShutdown()
                        .make();
            }

            initDBLayout( db );

            // mirrors default data from old sql file
            restoreAllIdBuilders();
            try {

                if ( doInitSchema ) {
                    insertDefaultData();
                }

            } catch ( GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownTableException | UnknownSchemaException | UnknownStoreException | UnknownColumnException e ) {
                throw new RuntimeException( e );
            }
            if ( doInitInformationPage ) {
                new CatalogInfoPage( this );
            }

            new CatalogValidator().startCheck();

        }
    }


    @Override
    public void commit() throws NoTablePrimaryKeyException {
        if ( openTable != null ) {
            throw new NoTablePrimaryKeyException();
        }
        db.commit();
    }


    @Override
    public void rollback() {
        db.rollback();
    }


    /**
     * checks if a file can be created on the system, accessed and changed
     *
     * @return if it was possible
     */
    private boolean isPersistent() {
        File file = new File( "testfile" );
        try {
            if ( !file.exists() ) {
                boolean res = file.createNewFile();
                if ( !res ) {
                    return false;
                }
            }
        } catch ( IOException e ) {
            return false;
        }
        if ( !file.canRead() || !file.canWrite() ) {
            return false;
        }
        file.delete();

        return true;
    }


    /**
     * Initializes the default catalog layout
     *
     * @param db the databases object on which the layout is created
     */
    private void initDBLayout( DB db ) {
        try {
            initUserInfo( db );
            initDatabaseInfo( db );
            initSchemaInfo( db );
            initTableInfo( db );
            initColumnInfo( db );
            initKeysAndConstraintsInfo( db );
            initStoreInfo( db );
        } catch ( SerializationError e ) {
            log.error( "!!!!!!!!!!! Error while restoring the catalog !!!!!!!!!!!" );
            log.error( "This usually means that there have been changes to the internal structure of the catalog with the last update of Polypheny-DB." );
            log.error( "To fix this, you must reset the catalog. To do this, please start Polypheny-DB once with the argument \"-resetCatalog\"." );
            System.exit( 1 );
        }
    }


    /**
     * Restores all columnPlacements in the dedicated store
     */
    @Override
    public void restoreColumnPlacements( Transaction transaction ) throws GenericCatalogException {
        StoreManager manager = StoreManager.getInstance();

        Map<Integer, List<Long>> restoredTables = new HashMap<>();

        for ( CatalogColumn c : columns.values() ) {
            List<CatalogColumnPlacement> placements = getColumnPlacements( c.id );
            if ( placements.size() == 0 ) {
                // no placements shouldn't happen
            } else if ( placements.size() == 1 ) {
                Store store = manager.getStore( placements.get( 0 ).storeId );
                if ( !store.isPersistent() ) {

                    CatalogTable catalogTable = null;
                    try {
                        catalogTable = getTable( c.tableId );
                    } catch ( UnknownTableException e ) {
                        throw new GenericCatalogException( e );
                    }
                    // TODO only full placements atm here

                    if ( !restoredTables.containsKey( store.getStoreId() ) ) {
                        store.createTable( transaction.createStatement().getPrepareContext(), catalogTable );
                        restoredTables.put( store.getStoreId(), Collections.singletonList( catalogTable.id ) );

                    } else if ( !(restoredTables.containsKey( store.getStoreId() ) && restoredTables.get( store.getStoreId() ).contains( catalogTable.id )) ) {
                        store.createTable( transaction.createStatement().getPrepareContext(), catalogTable );
                        List<Long> ids = new ArrayList<>( restoredTables.get( store.getStoreId() ) );
                        ids.add( catalogTable.id );
                        restoredTables.put( store.getStoreId(), ids );
                    }

                }
            } else {
                Map<Integer, Boolean> persistent = placements.stream().collect( Collectors.toMap( p -> p.storeId, p -> manager.getStore( p.storeId ).isPersistent() ) );

                if ( !persistent.containsValue( true ) ) {
                    // no persistent placement for this column
                    try {
                        CatalogTable table = getTable( c.tableId );
                        for ( CatalogColumnPlacement p : placements ) {
                            Store store = manager.getStore( p.storeId );

                            if ( !restoredTables.containsKey( store.getStoreId() ) ) {
                                store.createTable( transaction.createStatement().getPrepareContext(), table );
                                List<Long> ids = new ArrayList<>();
                                ids.add( table.id );
                                restoredTables.put( store.getStoreId(), ids );

                            } else if ( !(restoredTables.containsKey( store.getStoreId() ) && restoredTables.get( store.getStoreId() ).contains( table.id )) ) {
                                store.createTable( transaction.createStatement().getPrepareContext(), table );
                                List<Long> ids = new ArrayList<>( restoredTables.get( store.getStoreId() ) );
                                ids.add( table.id );
                                restoredTables.put( store.getStoreId(), ids );
                            }
                        }
                    } catch ( UnknownTableException e ) {
                        throw new GenericCatalogException( e );
                    }

                } else if ( persistent.containsValue( true ) && persistent.containsValue( false ) ) {
                    // TODO DL change so column gets copied
                    for ( Entry<Integer, Boolean> p : persistent.entrySet() ) {
                        if ( !p.getValue() ) {
                            deleteColumnPlacement( p.getKey(), c.id );
                        }
                    }
                }
            }
        }
    }


    private String buildStoreSchemaName( String storeName, String logicalSchema, String physicalSchema ) {
        return storeName + "_" + logicalSchema + "_" + physicalSchema;
    }


    /**
     * Sets the idBuilder for a given map to the new starting position
     *
     * @param map the map to which the idBuilder belongs
     * @param idBuilder which is creates new unique ids
     */
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
        restoreIdBuilder( indexes, indexIdBuilder );
        restoreIdBuilder( stores, storeIdBuilder );
        restoreIdBuilder( foreignKeys, foreignKeyIdBuilder );

    }


    /**
     * initiates all needed maps for stores
     *
     * stores: storeId -> CatalogStore
     * storeNames: storeName -> CatalogStore
     */
    private void initStoreInfo( DB db ) {
        stores = db.hashMap( "stores", Serializer.INTEGER, new GenericSerializer<CatalogStore>() ).createOrOpen();
        storeNames = db.hashMap( "storeNames", Serializer.STRING, new GenericSerializer<CatalogStore>() ).createOrOpen();
    }


    /**
     * creates all needed maps for keys and constraints
     *
     * keyColumns: [columnId1, columnId2,...] -> keyId
     * keys: keyId -> CatalogKey
     * primaryKeys: keyId -> CatalogPrimaryKey
     * foreignKeys: keyId -> CatalogForeignKey
     * constraints: constraintId -> CatalogConstraint
     * indexes: indexId -> CatalogIndex
     */
    private void initKeysAndConstraintsInfo( DB db ) {
        keyColumns = db.hashMap( "keyColumns", Serializer.LONG_ARRAY, Serializer.LONG ).createOrOpen();
        keys = db.hashMap( "keys", Serializer.LONG, new GenericSerializer<CatalogKey>() ).createOrOpen();
        primaryKeys = db.hashMap( "primaryKeys", Serializer.LONG, new GenericSerializer<CatalogPrimaryKey>() ).createOrOpen();
        foreignKeys = db.hashMap( "foreignKeys", Serializer.LONG, new GenericSerializer<CatalogForeignKey>() ).createOrOpen();
        constraints = db.hashMap( "constraints", Serializer.LONG, new GenericSerializer<CatalogConstraint>() ).createOrOpen();
        indexes = db.hashMap( "indexes", Serializer.LONG, new GenericSerializer<CatalogIndex>() ).createOrOpen();
    }


    /**
     * creates all needed maps for users
     *
     * users: userId -> CatalogUser
     * userNames: name -> CatalogUser
     */
    private void initUserInfo( DB db ) {
        users = db.hashMap( "users", Serializer.INTEGER, new GenericSerializer<CatalogUser>() ).createOrOpen();
        userNames = db.hashMap( "usersNames", Serializer.STRING, new GenericSerializer<CatalogUser>() ).createOrOpen();
    }


    /**
     * initialize the column maps
     *
     * columns: columnId -> CatalogColumn
     * columnNames: new Object[]{databaseId, schemaId, tableId, columnName} -> CatalogColumn
     * columnPlacements: new Object[]{storeId, columnId} -> CatalogPlacement
     */
    private void initColumnInfo( DB db ) {
        //noinspection unchecked
        columns = db.treeMap( "columns", Serializer.LONG, Serializer.JAVA ).createOrOpen();
        //noinspection unchecked
        columnNames = db.treeMap( "columnNames", new SerializerArrayTuple( Serializer.LONG, Serializer.LONG, Serializer.LONG, Serializer.STRING ), Serializer.JAVA ).createOrOpen();
        //noinspection unchecked
        columnPlacements = db.treeMap( "columnPlacement", new SerializerArrayTuple( Serializer.INTEGER, Serializer.LONG ), Serializer.JAVA ).createOrOpen();
    }


    /**
     * creates all maps needed for tables
     *
     * tables: tableId -> CatalogTable
     * tableChildren: tableId -> [columnId, columnId,..]
     * tableNames: new Object[]{databaseId, schemaId, tableName} -> CatalogTable
     */
    private void initTableInfo( DB db ) {
        //noinspection unchecked
        tables = db.treeMap( "tables", Serializer.LONG, Serializer.JAVA ).createOrOpen();
        tableChildren = db.hashMap( "tableChildren", Serializer.LONG, new GenericSerializer<ImmutableList<Long>>() ).createOrOpen();
        //noinspection unchecked
        tableNames = db.treeMap( "tableNames" )
                .keySerializer( new SerializerArrayTuple( Serializer.LONG, Serializer.LONG, Serializer.STRING ) )
                .valueSerializer( Serializer.JAVA )
                .createOrOpen();
    }


    /**
     * creates all needed maps for schemas
     *
     * schemas: schemaId -> CatalogSchema
     * schemaChildren: schemaId -> [tableId, tableId, etc]
     * schemaNames: new Object[]{databaseId, schemaName} -> CatalogSchema
     */
    private void initSchemaInfo( DB db ) {
        //noinspection unchecked
        schemas = db.treeMap( "schemas", Serializer.LONG, Serializer.JAVA ).createOrOpen();
        schemaChildren = db.hashMap( "schemaChildren", Serializer.LONG, new GenericSerializer<ImmutableList<Long>>() ).createOrOpen();
        //noinspection unchecked
        schemaNames = db.treeMap( "schemaNames", new SerializerArrayTuple( Serializer.LONG, Serializer.STRING ), Serializer.JAVA ).createOrOpen();
    }


    /**
     * creates maps for databases
     *
     * databases: databaseId -> CatalogDatabase
     * databaseNames: databaseName -> CatalogDatabase
     * databaseChildren: databaseId -> [tableId, tableId,...]
     */
    private void initDatabaseInfo( DB db ) {
        //noinspection unchecked
        databases = db.treeMap( "databases", Serializer.LONG, Serializer.JAVA ).createOrOpen();
        //noinspection unchecked
        databaseNames = db.treeMap( "databaseNames", Serializer.STRING, Serializer.JAVA ).createOrOpen();
        databaseChildren = db.hashMap( "databaseChildren", Serializer.LONG, new GenericSerializer<ImmutableList<Long>>() ).createOrOpen();
    }


    /**
     * Fills the catalog database with default data, skips if data is already inserted
     */
    private void insertDefaultData() throws GenericCatalogException, UnknownUserException, UnknownDatabaseException, UnknownTableException, UnknownSchemaException, UnknownStoreException, UnknownColumnException {

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
        // init store
        if ( !storeNames.containsKey( "hsqldb" ) ) {
            Map<String, String> hsqldbSettings = new HashMap<>();
            hsqldbSettings.put( "type", "Memory" );
            hsqldbSettings.put( "tableType", "Memory" );
            hsqldbSettings.put( "path", "./" );
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

        if ( !testMode ) {

            //////////////
            // init schema
            if ( !tableNames.containsKey( new Object[]{ databaseId, schemaId, "depts" } ) ) {
                addTable( "depts", schemaId, systemId, TableType.TABLE, null );
            }
            if ( !tableNames.containsKey( new Object[]{ databaseId, schemaId, "emps" } ) ) {
                addTable( "emps", schemaId, systemId, TableType.TABLE, null );
            }
            if ( !tableNames.containsKey( new Object[]{ databaseId, schemaId, "emp" } ) ) {
                addTable( "emp", schemaId, systemId, TableType.TABLE, null );
            }
            if ( !tableNames.containsKey( new Object[]{ databaseId, schemaId, "work" } ) ) {
                addTable( "work", schemaId, systemId, TableType.TABLE, null );
            }

            addDefaultCsvColumns( csv );
        }

        try {
            commit();
        } catch ( NoTablePrimaryKeyException e ) {
            throw new RuntimeException( e );
        }

    }


    /**
     * Initiates default columns for csv files
     */
    private void addDefaultCsvColumns( CatalogStore csv ) throws UnknownSchemaException, UnknownTableException, GenericCatalogException, UnknownColumnException {
        CatalogSchema schema = getSchema( "APP", "public" );
        CatalogTable depts = getTable( schema.id, "depts" );

        addDefaultColumn( csv, depts, "deptno", PolyType.INTEGER, null, 1, null );
        addDefaultColumn( csv, depts, "name", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 2, 20 );

        CatalogTable emps = getTable( schema.id, "emps" );
        addDefaultColumn( csv, emps, "empid", PolyType.INTEGER, null, 1, null );
        addDefaultColumn( csv, emps, "deptno", PolyType.INTEGER, null, 2, null );
        addDefaultColumn( csv, emps, "name", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 3, 20 );
        addDefaultColumn( csv, emps, "salary", PolyType.INTEGER, null, 4, null );
        addDefaultColumn( csv, emps, "commission", PolyType.INTEGER, null, 5, null );

        CatalogTable emp = getTable( schema.id, "emp" );
        addDefaultColumn( csv, emp, "employeeno", PolyType.INTEGER, null, 1, null );
        addDefaultColumn( csv, emp, "age", PolyType.INTEGER, null, 2, null );
        addDefaultColumn( csv, emp, "gender", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 3, 20 );
        addDefaultColumn( csv, emp, "maritalstatus", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 4, 20 );
        addDefaultColumn( csv, emp, "worklifebalance", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 5, 20 );
        addDefaultColumn( csv, emp, "education", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 6, 20 );
        addDefaultColumn( csv, emp, "monthlyincome", PolyType.INTEGER, null, 7, null );
        addDefaultColumn( csv, emp, "relationshipjoy", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 8, 20 );
        addDefaultColumn( csv, emp, "workingyears", PolyType.INTEGER, null, 9, null );
        addDefaultColumn( csv, emp, "yearsatcompany", PolyType.INTEGER, null, 10, null );

        CatalogTable work = getTable( schema.id, "work" );
        addDefaultColumn( csv, work, "employeeno", PolyType.INTEGER, null, 1, null );
        addDefaultColumn( csv, work, "educationfield", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 2, 20 );
        addDefaultColumn( csv, work, "jobinvolvement", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 3, 20 );
        addDefaultColumn( csv, work, "joblevel", PolyType.INTEGER, null, 4, null );
        addDefaultColumn( csv, work, "jobrole", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 5, 30 );
        addDefaultColumn( csv, work, "businesstravel", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 6, 20 );
        addDefaultColumn( csv, work, "department", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 7, 25 );
        addDefaultColumn( csv, work, "attrition", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 8, 20 );
        addDefaultColumn( csv, work, "dailyRate", PolyType.INTEGER, null, 9, null );

        // set all needed primary keys
        addPrimaryKey( depts.id, Collections.singletonList( getColumn( depts.id, "deptno" ).id ) );
        addPrimaryKey( emps.id, Collections.singletonList( getColumn( emps.id, "empid" ).id ) );
        addPrimaryKey( emp.id, Collections.singletonList( getColumn( emp.id, "employeeno" ).id ) );
        addPrimaryKey( work.id, Collections.singletonList( getColumn( work.id, "employeeno" ).id ) );

    }


    private void addDefaultColumn( CatalogStore csv, CatalogTable table, String name, PolyType type, Collation collation, int position, Integer length ) throws GenericCatalogException, UnknownTableException {
        if ( !checkIfExistsColumn( table.id, name ) ) {
            long colId = addColumn( name, table.id, position, type, null, length, null, null, null, false, collation );
            addColumnPlacement( csv.id, colId, PlacementType.AUTOMATIC, null, table.name, name );
        }
    }


    @Override
    public void validateColumns() {
        CatalogValidator validator = new CatalogValidator();
        db.rollback();
        try {
            validator.validate();
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void close() {
        db.close();
    }


    @Override
    public void clear() {
        db.getAll().clear();
        initDBLayout( db );
        restoreAllIdBuilders();
    }


    /**
     * Inserts a new database,
     * if a database with the same name already exists, it throws an error // TODO should it?
     *
     * @return the id of the newly inserted database
     */
    @Override
    public long addDatabase( String name, int ownerId, String ownerName, long defaultSchemaId, String defaultSchemaName ) {
        long id = databaseIdBuilder.getAndIncrement();
        CatalogDatabase database = new CatalogDatabase( id, name, ownerId, ownerName, defaultSchemaId, defaultSchemaName );
        synchronized ( this ) {
            databases.put( id, database );
            databaseNames.put( name, database );
            databaseChildren.put( id, ImmutableList.<Long>builder().build() );
        }
        listeners.firePropertyChange( "database", null, database );
        return id;
    }


    @Override
    public void deleteDatabase( long databaseId ) throws UnknownDatabaseException {
        CatalogDatabase database = getDatabase( databaseId );
        if ( database != null ) {
            synchronized ( this ) {
                databases.remove( databaseId );
                databaseNames.remove( database.name );
                databaseChildren.remove( databaseId );
            }
        }
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
        synchronized ( this ) {
            users.put( user.id, user );
            userNames.put( user.name, user );
        }
        listeners.firePropertyChange( "user", null, user );
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
        if ( pattern != null ) {
            if ( pattern.containsWildcards ) {
                return databaseNames.entrySet().stream().filter( e -> e.getKey().matches( pattern.toRegex() ) ).map( Entry::getValue ).sorted().collect( Collectors.toList() );
            } else {
                if ( databaseNames.containsKey( pattern.pattern ) ) {
                    return Collections.singletonList( databaseNames.get( pattern.pattern ) );
                } else {
                    return new ArrayList<>();
                }
            }
        } else {
            return new ArrayList<>( databases.values() );
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
            return Objects.requireNonNull( databaseNames.get( databaseName ) );
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
            return Objects.requireNonNull( databases.get( databaseId ) );
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

        if ( catalogDatabases.size() > 0 ) {
            Stream<CatalogSchema> catalogSchemas = catalogDatabases.stream().filter( d -> databaseChildren.containsKey( d.id ) ).flatMap( d -> Objects.requireNonNull( databaseChildren.get( d.id ) ).stream() ).map( schemas::get );

            if ( schemaNamePattern != null ) {
                catalogSchemas = catalogSchemas.filter( s -> s.name.matches( schemaNamePattern.toRegex() ) );
            }
            return catalogSchemas.collect( Collectors.toList() );
        }

        return new ArrayList<>();

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


    @Override
    public CatalogSchema getSchema( long schemaId ) throws UnknownSchemaException {
        try {
            return Objects.requireNonNull( schemas.get( schemaId ) );
        } catch ( NullPointerException e ) {
            throw new UnknownSchemaException( schemaId );
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
    public CatalogSchema getSchema( String databaseName, String schemaName ) throws UnknownSchemaException {
        try {
            return Objects.requireNonNull( schemaNames.get( new Object[]{ Objects.requireNonNull( databaseNames.get( databaseName ) ).id, schemaName } ) );
        } catch ( NullPointerException e ) {
            throw new UnknownSchemaException( databaseName, schemaName );
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
            return Objects.requireNonNull( schemaNames.get( new Object[]{ databaseId, schemaName } ) );
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
            CatalogUser owner = Objects.requireNonNull( users.get( ownerId ) );
            long id = schemaIdBuilder.getAndIncrement();
            CatalogSchema schema = new CatalogSchema( id, name, databaseId, ownerId, owner.name, schemaType );
            synchronized ( this ) {
                schemas.put( id, schema );
                schemaNames.put( new Object[]{ databaseId, name }, schema );
                schemaChildren.put( id, ImmutableList.<Long>builder().build() );
                List<Long> children = new ArrayList<>( Objects.requireNonNull( databaseChildren.get( databaseId ) ) );
                children.add( id );
                databaseChildren.replace( databaseId, ImmutableList.copyOf( children ) );
            }
            listeners.firePropertyChange( "schema", null, schema );
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
            CatalogSchema schema = new CatalogSchema( old.id, name, old.databaseId, old.ownerId, old.ownerName, old.schemaType );

            synchronized ( this ) {
                schemas.replace( schemaId, schema );
                schemaNames.remove( new Object[]{ old.databaseId, old.name } );
                schemaNames.put( new Object[]{ old.databaseId, name }, schema );
            }
            listeners.firePropertyChange( "schema", old, schema );
        } catch ( NullPointerException e ) {
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
            CatalogSchema schema = new CatalogSchema( old.id, old.name, old.databaseId, (int) ownerId, old.ownerName, old.schemaType );
            synchronized ( this ) {
                schemas.replace( schemaId, schema );
                schemaNames.replace( new Object[]{ schema.databaseId, schema.name }, schema );
            }
            listeners.firePropertyChange( "schema", old, schema );

        } catch ( NullPointerException e ) {
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
            synchronized ( this ) {
                schemaNames.remove( new Object[]{ schema.databaseId, schema.name } );
                List<Long> oldChildren = new ArrayList<>( Objects.requireNonNull( databaseChildren.get( schema.databaseId ) ) );
                oldChildren.remove( schemaId );
                databaseChildren.replace( schema.databaseId, ImmutableList.copyOf( oldChildren ) );

                for ( Long id : Objects.requireNonNull( schemaChildren.get( schemaId ) ) ) {
                    deleteTable( id );
                }
                schemaChildren.remove( schemaId );
                schemas.remove( schemaId );

            }
            listeners.firePropertyChange( "Schema", schema, null );
        } catch ( NullPointerException e ) {
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
        if ( schemas.containsKey( schemaId ) ) {

            CatalogSchema schema = Objects.requireNonNull( schemas.get( schemaId ) );
            if ( tableNamePattern != null ) {
                return Collections.singletonList( tableNames.get( new Object[]{ schema.databaseId, schemaId, tableNamePattern.pattern } ) );
            } else {
                return new ArrayList<>( tableNames.prefixSubMap( new Object[]{ schema.databaseId, schemaId } ).values() );
            }
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

        if ( schemaNamePattern != null && tableNamePattern != null ) {
            CatalogSchema schema = schemaNames.get( new Object[]{ databaseId, schemaNamePattern.pattern } );
            if ( schema != null ) {
                return Collections.singletonList( Objects.requireNonNull( tableNames.get( new Object[]{ databaseId, schema.id, tableNamePattern.pattern } ) ) );
            }
        } else if ( schemaNamePattern != null ) {
            CatalogSchema schema = schemaNames.get( new Object[]{ databaseId, schemaNamePattern.pattern } );
            if ( schema != null ) {
                return new ArrayList<>( tableNames.prefixSubMap( new Object[]{ databaseId, schema.id } ).values() );
            }
        } else {
            return new ArrayList<>( tableNames.prefixSubMap( new Object[]{ databaseId } ).values() );
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

        if ( catalogSchemas.size() > 0 ) {
            Stream<CatalogTable> catalogTables = catalogSchemas.stream().filter( t -> schemaChildren.containsKey( t.id ) ).flatMap( t -> Objects.requireNonNull( schemaChildren.get( t.id ) ).stream() ).map( tables::get );

            if ( tableNamePattern != null ) {
                catalogTables = catalogTables.filter( t -> t.name.matches( tableNamePattern.toRegex() ) );
            }
            return catalogTables.collect( Collectors.toList() );
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
            return Objects.requireNonNull( tableNames.get( new Object[]{ schema.databaseId, schemaId, tableName } ) );
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
            return Objects.requireNonNull( tableNames.get( new Object[]{ databaseId, schemaId, tableName } ) );
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
            return Objects.requireNonNull( tableNames.get( new Object[]{ databaseId, schemaId, tableName } ) );
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
            CatalogUser owner = Objects.requireNonNull( users.get( ownerId ) );
            CatalogTable table = new CatalogTable(
                    id,
                    name,
                    ImmutableList.of(),
                    schemaId,
                    schema.databaseId,
                    ownerId,
                    owner.name,
                    tableType,
                    definition,
                    null,
                    ImmutableMap.of() );

            synchronized ( this ) {
                tables.put( id, table );

                tableChildren.put( id, ImmutableList.<Long>builder().build() );
                tableNames.put( new Object[]{ schema.databaseId, schemaId, name }, table );
                List<Long> children = new ArrayList<>( Objects.requireNonNull( schemaChildren.get( schemaId ) ) );
                children.add( id );
                schemaChildren.replace( schemaId, ImmutableList.copyOf( children ) );
            }
            openTable = id;
            listeners.firePropertyChange( "table", null, table );
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
            CatalogTable old = Objects.requireNonNull( tables.get( tableId ) );
            CatalogTable table = new CatalogTable( old.id, name, old.columnIds, old.schemaId, old.databaseId, old.ownerId, old.ownerName, old.tableType, old.definition, old.primaryKey, old.placementsByStore );
            synchronized ( this ) {
                tables.replace( tableId, table );
                tableNames.remove( new Object[]{ table.databaseId, table.schemaId, old.name } );
                tableNames.put( new Object[]{ table.databaseId, table.schemaId, name }, table );
            }
            listeners.firePropertyChange( "table", old, table );
        } catch ( NullPointerException e ) {
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
            CatalogTable table = Objects.requireNonNull( tables.get( tableId ) );
            List<Long> children = new ArrayList<>( Objects.requireNonNull( schemaChildren.get( table.schemaId ) ) );
            children.remove( tableId );
            synchronized ( this ) {
                schemaChildren.replace( table.schemaId, ImmutableList.copyOf( children ) );

                for ( Long columnId : Objects.requireNonNull( tableChildren.get( tableId ) ) ) {
                    deleteColumn( columnId );
                }

                tableChildren.remove( tableId );
                tables.remove( tableId );
                tableNames.remove( new Object[]{ table.databaseId, table.schemaId, table.name } );
                // primary key was deleted and open table has to be closed
                if ( openTable != null && openTable == tableId ) {
                    openTable = null;
                }

            }
            listeners.firePropertyChange( "table", table, null );
        } catch ( NullPointerException e ) {
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
            CatalogTable old = Objects.requireNonNull( tables.get( tableId ) );
            CatalogUser user = Objects.requireNonNull( users.get( ownerId ) );
            CatalogTable table = new CatalogTable( old.id, old.name, old.columnIds, old.schemaId, old.databaseId, ownerId, user.name, old.tableType, old.definition, old.primaryKey, old.placementsByStore );
            synchronized ( this ) {
                tables.replace( tableId, table );
                tableNames.replace( new Object[]{ table.databaseId, table.schemaId, table.name }, table );
            }
            listeners.firePropertyChange( "table", old, table );
        } catch ( NullPointerException e ) {
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
            CatalogTable old = Objects.requireNonNull( tables.get( tableId ) );
            CatalogTable table = new CatalogTable( old.id, old.name, old.columnIds, old.schemaId, old.databaseId, old.ownerId, old.ownerName, old.tableType, old.definition, keyId, old.placementsByStore );
            synchronized ( this ) {
                tables.replace( tableId, table );
                tableNames.replace( new Object[]{ table.databaseId, table.schemaId, table.name }, table );

                if ( keyId == null ) {
                    openTable = tableId;
                } else {
                    primaryKeys.put( keyId, new CatalogPrimaryKey( Objects.requireNonNull( keys.get( keyId ) ) ) );
                    openTable = null;
                }
            }
            listeners.firePropertyChange( "table", old, table );
        } catch ( NullPointerException e ) {
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
            CatalogColumn column = Objects.requireNonNull( columns.get( columnId ) );
            CatalogStore store = Objects.requireNonNull( stores.get( storeId ) );
            CatalogColumnPlacement placement = new CatalogColumnPlacement(
                    column.tableId,
                    columnId,
                    storeId,
                    store.uniqueName,
                    placementType,
                    physicalSchemaName,
                    physicalTableName,
                    physicalColumnName,
                    physicalPositionBuilder.getAndIncrement() );

            synchronized ( this ) {
                columnPlacements.put( new Object[]{ storeId, columnId }, placement );

                CatalogTable old = Objects.requireNonNull( tables.get( column.tableId ) );
                Map<Integer, ImmutableList<Long>> placementsByStore = new HashMap<>( old.placementsByStore );
                if ( placementsByStore.containsKey( storeId ) ) {
                    List<Long> placements = new ArrayList<>( placementsByStore.get( storeId ) );
                    placements.add( columnId );
                    placementsByStore.replace( storeId, ImmutableList.copyOf( placements ) );
                } else {
                    placementsByStore.put( storeId, ImmutableList.of( columnId ) );
                }
                CatalogTable table = new CatalogTable( old.id, old.name, old.columnIds, old.schemaId, old.databaseId, old.ownerId, old.ownerName, old.tableType, old.definition, old.primaryKey, ImmutableMap.copyOf( placementsByStore ) );

                tables.replace( column.tableId, table );
                tableNames.replace( new Object[]{ table.databaseId, table.schemaId, table.name }, table );
            }
            listeners.firePropertyChange( "columnPlacement", null, placement );
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
    public void deleteColumnPlacement( int storeId, long columnId ) throws GenericCatalogException {

        try {
            CatalogTable oldTable = getTable( getColumn( columnId ).tableId );
            Map<Integer, ImmutableList<Long>> placementsByStore = new HashMap<>( oldTable.placementsByStore );
            List<Long> placements = new ArrayList<>( placementsByStore.get( storeId ) );
            placements.remove( columnId );
            if ( placements.size() != 0 ) {
                placementsByStore.put( storeId, ImmutableList.copyOf( placements ) );
            } else {
                placementsByStore.remove( storeId );
            }
            CatalogTable table = new CatalogTable( oldTable.id, oldTable.name, oldTable.columnIds, oldTable.schemaId, oldTable.databaseId, oldTable.ownerId, oldTable.ownerName, oldTable.tableType, oldTable.definition, oldTable.primaryKey, ImmutableMap.copyOf( placementsByStore ) );
            synchronized ( this ) {
                tables.replace( table.id, table );
                tableNames.replace( new Object[]{ table.databaseId, table.schemaId, table.name }, table );
                columnPlacements.remove( new Object[]{ storeId, columnId } );
            }
            listeners.firePropertyChange( "columnPlacement", table, null );
        } catch ( UnknownTableException | UnknownColumnIdRuntimeException e ) {
            throw new GenericCatalogException( e );
        }


    }


    @Override
    public CatalogColumnPlacement getColumnPlacement( int storeId, long columnId ) throws GenericCatalogException {
        try {
            return Objects.requireNonNull( columnPlacements.get( new Object[]{ storeId, columnId } ) );
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public boolean checkIfExistsColumnPlacement( int storeId, long columnId ) {
        CatalogColumnPlacement placement = columnPlacements.get( new Object[]{ storeId, columnId } );
        return placement != null;
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


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnStore( int storeId, long tableId ) {
        final Comparator<CatalogColumnPlacement> columnPlacementComparator = Comparator.comparingInt( p -> getColumn( p.columnId ).position );
        return getColumnPlacementsOnStore( storeId ).stream().filter( p -> p.tableId == tableId ).sorted( columnPlacementComparator ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnStoreSortedByPhysicalPosition( int storeId, long tableId ) {
        final Comparator<CatalogColumnPlacement> columnPlacementComparator = Comparator.comparingLong( p -> p.physicalPosition );
        return getColumnPlacementsOnStore( storeId ).stream().filter( p -> p.tableId == tableId ).sorted( columnPlacementComparator ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsByColumn( long columnId ) {
        return columnPlacements.values().stream().filter( p -> p.columnId == columnId ).collect( Collectors.toList() );
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


    @Override
    public void updateColumnPlacementType( int storeId, long columnId, PlacementType placementType ) throws UnknownColumnPlacementException {
        try {
            CatalogColumnPlacement old = Objects.requireNonNull( columnPlacements.get( new Object[]{ storeId, columnId } ) );
            CatalogColumnPlacement placement = new CatalogColumnPlacement(
                    old.tableId,
                    old.columnId,
                    old.storeId,
                    old.storeUniqueName,
                    placementType,
                    old.physicalSchemaName,
                    old.physicalTableName,
                    old.physicalColumnName,
                    physicalPositionBuilder.getAndIncrement() );
            synchronized ( this ) {
                columnPlacements.replace( new Object[]{ storeId, columnId }, placement );
            }
            listeners.firePropertyChange( "columnPlacement", old, placement );
        } catch ( NullPointerException e ) {
            throw new UnknownColumnPlacementException( storeId, columnId );
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
            CatalogColumnPlacement placement = new CatalogColumnPlacement(
                    old.tableId,
                    old.columnId,
                    old.storeId,
                    old.storeUniqueName,
                    old.placementType,
                    physicalSchemaName,
                    physicalTableName,
                    physicalColumnName,
                    physicalPositionBuilder.getAndIncrement() );
            synchronized ( this ) {
                columnPlacements.replace( new Object[]{ storeId, columnId }, placement );
            }
            listeners.firePropertyChange( "columnPlacement", old, placement );
        } catch ( NullPointerException e ) {
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
    public List<CatalogColumn> getColumns( long tableId ) {
        try {
            CatalogTable table = Objects.requireNonNull( tables.get( tableId ) );
            return columnNames.prefixSubMap( new Object[]{ table.databaseId, table.schemaId, table.id } ).values().stream().sorted( columnComparator ).collect( Collectors.toList() );

        } catch ( NullPointerException e ) {
            return new ArrayList<>();
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

        if ( catalogTables.size() > 0 ) {
            Stream<CatalogColumn> catalogColumns = catalogTables.stream().filter( t -> tableChildren.containsKey( t.id ) ).flatMap( t -> Objects.requireNonNull( tableChildren.get( t.id ) ).stream() ).map( columns::get );

            if ( columnNamePattern != null ) {
                catalogColumns = catalogColumns.filter( c -> c.name.matches( columnNamePattern.toRegex() ) );
            }
            return catalogColumns.collect( Collectors.toList() );
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
    public CatalogColumn getColumn( long columnId ) {
        try {
            return Objects.requireNonNull( columns.get( columnId ) );
        } catch ( NullPointerException e ) {
            throw new UnknownColumnIdRuntimeException( columnId );
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
            return Objects.requireNonNull( columnNames.get( new Object[]{ table.databaseId, table.schemaId, table.id, columnName } ) );
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
            return Objects.requireNonNull( columnNames.get( new Object[]{ table.databaseId, table.schemaId, table.id, columnName } ) );
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
    public long addColumn( String name, long tableId, int position, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality, boolean nullable, Collation collation ) throws GenericCatalogException {
        try {
            CatalogTable table = Objects.requireNonNull( tables.get( tableId ) );
            if ( type.getFamily() == PolyTypeFamily.CHARACTER && collation == null ) {
                throw new RuntimeException( "Collation is not allowed to be null for char types." );
            }
            if ( scale != null && scale > length ) {
                throw new RuntimeException( "Invalid scale! Scale can not be larger than length." );
            }

            long id = columnIdBuilder.getAndIncrement();
            CatalogColumn column = new CatalogColumn(
                    id,
                    name,
                    tableId,
                    table.schemaId,
                    table.databaseId,
                    position,
                    type,
                    collectionsType,
                    length,
                    scale,
                    dimension,
                    cardinality,
                    nullable,
                    collation,
                    null );

            synchronized ( this ) {
                columns.put( id, column );
                columnNames.put( new Object[]{ table.databaseId, table.schemaId, table.id, name }, column );
                List<Long> children = new ArrayList<>( Objects.requireNonNull( tableChildren.get( tableId ) ) );
                children.add( id );
                tableChildren.replace( tableId, ImmutableList.copyOf( children ) );

                List<Long> columnIds = new ArrayList<>( table.columnIds );
                columnIds.add( id );
                CatalogTable updatedTable = new CatalogTable( table.id, table.name, ImmutableList.copyOf( columnIds ), table.schemaId, table.databaseId, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, table.placementsByStore );

                tables.replace( tableId, updatedTable );
                tableNames.replace( new Object[]{ updatedTable.databaseId, updatedTable.schemaId, updatedTable.name }, updatedTable );
            }
            listeners.firePropertyChange( "column", null, column );
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
            CatalogColumn column = new CatalogColumn( old.id, name, old.tableId, old.schemaId, old.databaseId, old.position, old.type, old.collectionsType, old.length, old.scale, old.dimension, old.cardinality, old.nullable, old.collation, old.defaultValue );
            synchronized ( this ) {
                columns.replace( columnId, column );
                columnNames.remove( new Object[]{ column.databaseId, column.schemaId, column.tableId, old.name } );
                columnNames.put( new Object[]{ column.databaseId, column.schemaId, column.tableId, name }, column );
            }
            listeners.firePropertyChange( "column", old, column );
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
    public void setColumnPosition( long columnId, int position ) {
        try {
            CatalogColumn old = Objects.requireNonNull( columns.get( columnId ) );
            CatalogColumn column = new CatalogColumn( old.id, old.name, old.tableId, old.schemaId, old.databaseId, position, old.type, old.collectionsType, old.length, old.scale, old.dimension, old.cardinality, old.nullable, old.collation, old.defaultValue );
            synchronized ( this ) {
                columns.replace( columnId, column );
                columnNames.replace( new Object[]{ column.databaseId, column.schemaId, column.tableId, column.name }, column );
            }
            listeners.firePropertyChange( "column", old, column );
        } catch ( NullPointerException e ) {
            throw new UnknownColumnIdRuntimeException( columnId );
        }
    }


    /**
     * Change the data type of an column.
     *
     * @param columnId The id of the column
     * @param type The new type of the column
     */
    @Override
    public void setColumnType( long columnId, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality ) throws GenericCatalogException {
        try {
            CatalogColumn old = Objects.requireNonNull( columns.get( columnId ) );

            if ( scale != null && scale > length ) {
                throw new RuntimeException( "Invalid scale! Scale can not be larger than length." );
            }

            // Check that the column is not part of a key
            for ( CatalogKey key : getKeys() ) {
                if ( key.columnIds.contains( columnId ) ) {
                    String name = "UNKNOWN";
                    if ( key instanceof CatalogPrimaryKey ) {
                        name = "PRIMARY KEY";
                    } else if ( key instanceof CatalogForeignKey ) {
                        name = ((CatalogForeignKey) key).name;
                    } else {
                        List<CatalogConstraint> constraints = getConstraints( key );
                        if ( constraints.size() > 0 ) {
                            name = constraints.get( 0 ).name;
                        }
                    }
                    throw new GenericCatalogException( "The column \"" + old.name + "\" is part of the key \"" + name + "\". Unable to change the type of a column that is part of a key." );
                }
            }

            Collation collation = type.getFamily() == PolyTypeFamily.CHARACTER
                    ? Collation.getById( RuntimeConfig.DEFAULT_COLLATION.getInteger() )
                    : null;
            CatalogColumn column = new CatalogColumn( old.id, old.name, old.tableId, old.schemaId, old.databaseId, old.position, type, collectionsType, length, scale, dimension, cardinality, old.nullable, collation, old.defaultValue );
            synchronized ( this ) {
                columns.replace( columnId, column );
                columnNames.replace( new Object[]{ old.databaseId, old.schemaId, old.tableId, old.name }, column );
            }
            listeners.firePropertyChange( "column", old, column );
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
            CatalogColumn column = new CatalogColumn( old.id, old.name, old.tableId, old.schemaId, old.databaseId, old.position, old.type, old.collectionsType, old.length, old.scale, old.dimension, old.cardinality, nullable, old.collation, old.defaultValue );
            synchronized ( this ) {
                columns.replace( columnId, column );
                columnNames.replace( new Object[]{ old.databaseId, old.schemaId, old.tableId, old.name }, column );
            }
            listeners.firePropertyChange( "column", old, column );
        } catch ( NullPointerException | UnknownKeyException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Set the collation of a column.
     * If the column already has the specified collation set, this method is a NoOp.
     *
     * @param columnId  The id of the column
     * @param collation The collation to set
     */
    @Override
    public void setCollation( long columnId, Collation collation ) {
        try {
            CatalogColumn old = Objects.requireNonNull( columns.get( columnId ) );

            if ( old.type.getFamily() != PolyTypeFamily.CHARACTER ) {
                throw new RuntimeException( "Illegal attempt to set collation for a non-char column!" );
            }
            CatalogColumn column = new CatalogColumn( old.id, old.name, old.tableId, old.schemaId, old.databaseId, old.position, old.type, old.collectionsType, old.length, old.scale, old.dimension, old.cardinality, old.nullable, collation, old.defaultValue );
            synchronized ( this ) {
                columns.replace( columnId, column );
                columnNames.replace( new Object[]{ old.databaseId, old.schemaId, old.tableId, old.name }, column );
            }
            listeners.firePropertyChange( "column", old, column );
        } catch ( NullPointerException e ) {
            throw new UnknownColumnIdRuntimeException( columnId );
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

            List<Long> children = new ArrayList<>( Objects.requireNonNull( tableChildren.get( column.tableId ) ) );
            children.remove( columnId );

            CatalogTable old = getTable( column.tableId );
            List<Long> columnIds = new ArrayList<>( old.columnIds );
            columnIds.remove( columnId );
            CatalogTable table = new CatalogTable( old.id, old.name, ImmutableList.copyOf( columnIds ), old.schemaId, old.databaseId, old.ownerId, old.ownerName, old.tableType, old.definition, old.primaryKey, old.placementsByStore );

            synchronized ( this ) {
                columnNames.remove( new Object[]{ column.databaseId, column.schemaId, column.tableId, column.name } );
                tableChildren.replace( column.tableId, ImmutableList.copyOf( children ) );

                deleteDefaultValue( columnId );
                for ( CatalogColumnPlacement p : getColumnPlacements( columnId ) ) {
                    deleteColumnPlacement( p.storeId, p.columnId );
                }
                tables.replace( column.tableId, table );
                tableNames.replace( new Object[]{ table.databaseId, table.schemaId, table.name }, table );

                columns.remove( columnId );
            }
            listeners.firePropertyChange( "column", column, null );
        } catch ( NullPointerException | UnknownColumnIdRuntimeException | UnknownTableException e ) {
            throw new GenericCatalogException( e );
        }
    }

    // TODO: String is only a temporary solution


    /**
     * Adds a default value for a column. If there already is a default values, it being replaced.
     *
     * @param columnId     The id of the column
     * @param type         The type of the default value
     * @param defaultValue The default value
     */
    @Override
    public void setDefaultValue( long columnId, PolyType type, String defaultValue ) {
        try {
            CatalogColumn old = Objects.requireNonNull( columns.get( columnId ) );
            CatalogColumn column = new CatalogColumn(
                    old.id,
                    old.name,
                    old.tableId,
                    old.schemaId,
                    old.databaseId,
                    old.position,
                    old.type,
                    old.collectionsType,
                    old.length,
                    old.scale,
                    old.dimension,
                    old.cardinality,
                    old.nullable,
                    old.collation,
                    new CatalogDefaultValue( columnId, type, defaultValue, "defaultValue" ) );
            synchronized ( this ) {
                columns.replace( columnId, column );
                columnNames.replace( new Object[]{ column.databaseId, column.schemaId, column.tableId, column.name }, column );
            }
            listeners.firePropertyChange( "column", old, column );
        } catch ( NullPointerException e ) {
            throw new UnknownColumnIdRuntimeException( columnId );
        }
    }


    /**
     * Deletes an existing default value of a column. NoOp if there is no default value defined.
     *
     * @param columnId The id of the column
     */
    @Override
    public void deleteDefaultValue( long columnId ) {
        try {
            CatalogColumn old = Objects.requireNonNull( columns.get( columnId ) );
            CatalogColumn column = new CatalogColumn(
                    old.id,
                    old.name,
                    old.tableId,
                    old.schemaId,
                    old.databaseId,
                    old.position,
                    old.type,
                    old.collectionsType,
                    old.length,
                    old.scale,
                    old.dimension,
                    old.cardinality,
                    old.nullable,
                    old.collation,
                    null );
            if ( column.defaultValue != null ) {
                synchronized ( this ) {
                    columns.replace( columnId, column );
                    columnNames.replace( new Object[]{ old.databaseId, old.schemaId, old.tableId, old.name }, column );
                }
                listeners.firePropertyChange( "column", old, column );
            }
        } catch ( NullPointerException e ) {
            throw new UnknownColumnIdRuntimeException( columnId );
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
            return Objects.requireNonNull( primaryKeys.get( key ) );
        } catch ( NullPointerException e ) {
            throw new UnknownKeyException( key );
        }
    }


    @Override
    public boolean isPrimaryKey( long key ) {
        try {
            Long primary = getTable( Objects.requireNonNull( keys.get( key ) ).tableId ).primaryKey;
            return primary != null && primary == key;
        } catch ( UnknownTableException e ) {
            throw new RuntimeException( e );
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
                // CatalogCombinedKey combinedKey = getCombinedKey( table.primaryKey );
                if ( getKeyUniqueCount( table.primaryKey ) == 1 && isForeignKey( tableId ) ) {
                    // This primary key is the only constraint for the uniqueness of this key.
                    throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. To drop this primary key, first drop the foreign keys or create a unique constraint." );
                }
                synchronized ( this ) {
                    setPrimaryKey( tableId, null );
                    deleteKeyIfNoLongerUsed( table.primaryKey );
                }
            }
            long keyId = getOrAddKey( tableId, columnIds );
            setPrimaryKey( tableId, keyId );
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    private int getKeyUniqueCount( long keyId ) {
        CatalogKey key = keys.get( keyId );
        int count = 0;
        if ( isPrimaryKey( keyId ) ) {
            count++;
        }

        for ( CatalogConstraint constraint : getConstraints( key ) ) {
            if ( constraint.type == ConstraintType.UNIQUE ) {
                count++;
            }
        }

        for ( CatalogIndex index : getIndexes( key ) ) {
            if ( index.unique ) {
                count++;
            }
        }

        return count;
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

                    // CatalogKey combinedKey = getCombinedKey( refKey.id );

                    int i = 0;
                    for ( long referencedColumnId : refKey.columnIds ) {
                        CatalogColumn referencingColumn = getColumn( columnIds.get( i++ ) );
                        CatalogColumn referencedColumn = getColumn( referencedColumnId );
                        if ( referencedColumn.type != referencingColumn.type ) {
                            throw new GenericCatalogException( "The data type of the referenced columns does not match the data type of the referencing column: " + referencingColumn.type.name() + " != " + referencedColumn.type );
                        }
                    }
                    // TODO same keys for key and foreignkey
                    if ( getKeyUniqueCount( refKey.id ) > 0 ) {
                        long keyId = getOrAddKey( tableId, columnIds );
                        List<String> keyColumnNames = columnIds.stream().map( id -> Objects.requireNonNull( columns.get( id ) ).name ).collect( Collectors.toList() );
                        List<String> referencesNames = referencesIds.stream().map( id -> Objects.requireNonNull( columns.get( id ) ).name ).collect( Collectors.toList() );
                        CatalogForeignKey key = new CatalogForeignKey(
                                keyId,
                                constraintName,
                                tableId,
                                table.schemaId,
                                table.databaseId,
                                refKey.id,
                                refKey.tableId,
                                refKey.schemaId,
                                refKey.databaseId,
                                columnIds,
                                referencesIds,
                                onUpdate,
                                onDelete );
                        synchronized ( this ) {
                            foreignKeys.put( keyId, key );

                        }
                        listeners.firePropertyChange( "foreignKey", null, key );
                        return;
                    }
                }
            }
        } catch ( NullPointerException | UnknownColumnIdRuntimeException e ) {
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
            synchronized ( this ) {
                constraints.put( id, new CatalogConstraint( id, keyId, ConstraintType.UNIQUE, constraintName, Objects.requireNonNull( keys.get( keyId ) ) ) );

            }
            listeners.firePropertyChange( "constraint", null, keyId );
        } catch ( NullPointerException e ) {
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
            return indexes.values().stream().filter( i -> i.key.tableId == tableId ).collect( Collectors.toList() );
        } else {
            return indexes.values().stream().filter( i -> i.key.tableId == tableId && i.unique ).collect( Collectors.toList() );
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
            return indexes.values().stream().filter( i -> i.key.tableId == tableId && i.name.equals( indexName ) ).findFirst().orElseThrow( NullPointerException::new );
        } catch ( NullPointerException e ) {
            throw new UnknownIndexException( tableId, indexName );
        }
    }


    /**
     * Returns the index with the specified id
     *
     * @param indexId The id of the index
     * @return The Index
     */
    @Override
    public CatalogIndex getIndex( long indexId ) throws UnknownIndexException {
        try {
            return indexes.values().stream().filter( i -> i.id == indexId ).findFirst().orElseThrow( NullPointerException::new );
        } catch ( NullPointerException e ) {
            throw new UnknownIndexException( indexId );
        }
    }


    /**
     * Checks if there is an index with the specified name in the specified table.
     *
     * @param tableId The id of the table
     * @param indexName The name to check for
     * @return true if there is an index with this name, false if not.
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public boolean checkIfExistsIndex( long tableId, String indexName ) throws GenericCatalogException {
        try {
            CatalogTable table = getTable( tableId );
            getIndex( table.id, indexName );
            return true;
        } catch ( UnknownTableException e ) {
            throw new GenericCatalogException( e );
        } catch ( UnknownIndexException e ) {
            return false;
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
            // TODO: Check if the current values are unique
        }
        long id = indexIdBuilder.getAndIncrement();
        synchronized ( this ) {
            indexes.put( id, new CatalogIndex( id, indexName, unique, type, null, keyId, Objects.requireNonNull( keys.get( keyId ) ) ) );
        }
        listeners.firePropertyChange( "index", null, keyId );
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
            CatalogIndex index = Objects.requireNonNull( indexes.get( indexId ) );
            if ( index.unique ) {
                if ( getKeyUniqueCount( index.keyId ) == 1 && isForeignKey( index.keyId ) ) {
                    // This unique index is the only constraint for the uniqueness of this key.
                    throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. To delete this index, first add a unique constraint." );
                }
            }
            synchronized ( this ) {
                indexes.remove( indexId );

            }
            listeners.firePropertyChange( "index", index.key, null );
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
                // CatalogCombinedKey key = getCombinedKey( table.primaryKey );
                if ( isForeignKey( table.primaryKey ) ) {
                    if ( getKeyUniqueCount( table.primaryKey ) < 2 ) {
                        throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. To drop this primary key either drop the foreign key or create a unique constraint." );
                    }
                }

                setPrimaryKey( tableId, null );
                deleteKeyIfNoLongerUsed( table.primaryKey );
            }
        } catch ( NullPointerException e ) {
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
            synchronized ( this ) {
                foreignKeys.remove( catalogForeignKey.id );

                deleteKeyIfNoLongerUsed( catalogForeignKey.id );
            }
            listeners.firePropertyChange( "foreignKey", foreignKeyId, null );
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

            //CatalogCombinedKey key = getCombinedKey( catalogConstraint.keyId );
            if ( catalogConstraint.type == ConstraintType.UNIQUE && isForeignKey( catalogConstraint.keyId ) ) {
                if ( getKeyUniqueCount( catalogConstraint.keyId ) < 2 ) {
                    throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. Unable to drop unique constraint." );
                }
            }
            synchronized ( this ) {
                constraints.remove( catalogConstraint.id );

            }
            listeners.firePropertyChange( "constraint", catalogConstraint, null );
            deleteKeyIfNoLongerUsed( catalogConstraint.keyId );
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
            return Objects.requireNonNull( userNames.get( userName ) );
        } catch ( NullPointerException e ) {
            throw new UnknownUserException( userName );
        }
    }


    public CatalogUser getUser( int userId ) throws UnknownUserException {
        try {
            return Objects.requireNonNull( users.get( userId ) );
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
     * Get a store by its id
     *
     * @return List of stores
     */
    @Override
    public CatalogStore getStore( int storeId ) throws UnknownStoreException {
        try {
            return Objects.requireNonNull( stores.get( storeId ) );
        } catch ( NullPointerException e ) {
            throw new UnknownStoreException( storeId );
        }
    }


    /**
     * Add a store
     *
     * @param uniqueName The unique name of the store
     * @param adapter    The class name of the adapter
     * @param settings   The configuration of the store
     * @return The id of the newly added store
     */
    @Override
    public int addStore( String uniqueName, String adapter, Map<String, String> settings ) {
        uniqueName = uniqueName.toLowerCase();

        int id = storeIdBuilder.getAndIncrement();
        Map<String, String> temp = new HashMap<>();
        settings.forEach( temp::put );
        CatalogStore store = new CatalogStore( id, uniqueName, adapter, temp );
        synchronized ( this ) {
            stores.put( id, store );
            storeNames.put( uniqueName, store );
        }
        try {
            commit();
        } catch ( NoTablePrimaryKeyException e ) {
            throw new RuntimeException( "An error occurred while creating the store." );
        }
        listeners.firePropertyChange( "store", null, store );
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
            synchronized ( this ) {
                stores.remove( storeId );
                storeNames.remove( store.uniqueName );

            }
            listeners.firePropertyChange( "store", store, null );
        } catch ( NullPointerException e ) {
            throw new UnknownStoreException( storeId );
        }
    }


    // TODO move
    @Override
    public List<CatalogKey> getKeys() {
        return new ArrayList<>( keys.values() );
    }


    @Override
    public List<CatalogKey> getTableKeys( long tableId ) {
        return keys.values().stream().filter( k -> k.tableId == tableId ).collect( Collectors.toList() );
    }


    public List<CatalogIndex> getIndexes( CatalogKey key ) {
        return indexes.values().stream().filter( i -> i.keyId == key.id ).collect( Collectors.toList() );
    }


    public List<CatalogIndex> getIndexes() {
        return new ArrayList<>( indexes.values() );
    @Override
    public List<CatalogIndex> getIndices( CatalogKey key ) {
        return indices.values().stream().filter( i -> i.keyId == key.id ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogIndex> getForeignKeys( CatalogKey key ) {
        return indexes.values().stream().filter( i -> i.keyId == key.id ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogConstraint> getConstraints( CatalogKey key ) {
        return constraints.values().stream().filter( c -> c.keyId == key.id ).collect( Collectors.toList() );
    }


    @Override
    public boolean isIndex( long keyId ) {
        return indexes.values().stream().anyMatch( i -> i.keyId == keyId );
    }


    @Override
    public boolean isConstraint( long keyId ) {
        return constraints.values().stream().anyMatch( c -> c.keyId == keyId );
    }


    @Override
    public boolean isForeignKey( long keyId ) {
        return foreignKeys.values().stream().anyMatch( f -> f.referencedKeyId == keyId );
    }


    /**
     * Check if the specified key is used as primary key, index or constraint. If so, this is a NoOp. If it is not used, the key is deleted.
     */
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
            if ( foreignKeys.values().stream().anyMatch( f -> f.id == keyId ) ) {
                return;
            }
            if ( indexes.values().stream().anyMatch( i -> i.keyId == keyId ) ) {
                return;
            }
            synchronized ( this ) {
                keys.remove( keyId );
                keyColumns.remove( key.columnIds.stream().mapToLong( Long::longValue ).toArray() );
            }
            listeners.firePropertyChange( "key", key, null );
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the id of they defined by the specified column ids. If this key does not yet exist, create it.
     *
     * @param tableId on which the key is defined
     * @param columnIds all involved columns
     * @return the id of the key
     * @throws GenericCatalogException if the key does not exist
     */
    private long getOrAddKey( long tableId, List<Long> columnIds ) throws GenericCatalogException {
        Long keyId = keyColumns.get( columnIds.stream().mapToLong( Long::longValue ).toArray() );
        if ( keyId != null ) {
            return keyId;
        }
        return addKey( tableId, columnIds );
    }


    private long addKey( long tableId, List<Long> columnIds ) throws GenericCatalogException {
        try {
            CatalogTable table = Objects.requireNonNull( tables.get( tableId ) );
            long id = keyIdBuilder.getAndIncrement();
            CatalogKey key = new CatalogKey( id, table.id, table.schemaId, table.databaseId, columnIds );
            synchronized ( this ) {
                keys.put( id, key );
                keyColumns.put( columnIds.stream().mapToLong( Long::longValue ).toArray(), id );
            }
            listeners.firePropertyChange( "key", null, key );
            return id;
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    static class CatalogValidator {

        public void validate() throws GenericCatalogException {

        }


        public void startCheck() {
            columns.forEach( ( key, column ) -> {
                assert (databases.containsKey( column.databaseId ));
                assert (Objects.requireNonNull( databaseChildren.get( column.databaseId ) ).contains( column.schemaId ));

                assert (schemas.containsKey( column.schemaId ));
                assert (Objects.requireNonNull( schemaChildren.get( column.schemaId ) ).contains( column.tableId ));

                assert (tables.containsKey( column.tableId ));
                assert (Objects.requireNonNull( tableChildren.get( column.tableId ) ).contains( column.id ));

                assert (columnNames.containsKey( new Object[]{ column.databaseId, column.schemaId, column.tableId, column.name } ));
            } );

            columnPlacements.forEach( ( key, placement ) -> {
                assert (columns.containsKey( placement.columnId ));
                assert (stores.containsKey( placement.storeId ));
            } );
        }
    }
}

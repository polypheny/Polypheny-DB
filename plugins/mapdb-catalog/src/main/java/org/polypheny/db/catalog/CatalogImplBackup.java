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

package org.polypheny.db.catalog;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBException.SerializationError;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;
import org.pf4j.Extension;
import org.polypheny.db.StatusService;
import org.polypheny.db.StatusService.ErrorConfig;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogCollectionMapping;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogDefaultValue;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogGraphMapping;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogKey.EnforcementTime;
import org.polypheny.db.catalog.entity.CatalogMaterializedView;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPartitionGroup;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogQueryInterface;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.entity.CatalogView;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.MaterializedCriteria;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.GraphAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterIdRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownCollectionException;
import org.polypheny.db.catalog.exceptions.UnknownCollectionPlacementException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownColumnIdRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownColumnPlacementRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseIdRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyException;
import org.polypheny.db.catalog.exceptions.UnknownGraphException;
import org.polypheny.db.catalog.exceptions.UnknownGraphPlacementsException;
import org.polypheny.db.catalog.exceptions.UnknownIndexException;
import org.polypheny.db.catalog.exceptions.UnknownIndexIdRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownKeyIdRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionGroupIdRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionPlacementException;
import org.polypheny.db.catalog.exceptions.UnknownQueryInterfaceException;
import org.polypheny.db.catalog.exceptions.UnknownQueryInterfaceRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaIdRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownTableIdRuntimeException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.catalog.exceptions.UnknownUserIdRuntimeException;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.DataPlacementRole;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.logistic.IndexType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.partition.FrequencyMap;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.processing.ExtendedQueryParameters;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.view.MaterializedViewManager;

@Extension
@Slf4j
public class CatalogImplBackup extends Catalog {

    private static final String FILE_PATH = "mapDB";
    private static DB db;

    private static HTreeMap<Integer, CatalogUser> users;
    private static HTreeMap<String, CatalogUser> userNames;

    private static BTreeMap<Long, CatalogDatabase> databases;
    private static BTreeMap<String, CatalogDatabase> databaseNames;
    private static HTreeMap<Long, ImmutableList<Long>> databaseChildren;

    private static BTreeMap<Long, LogicalNamespace> schemas;
    private static BTreeMap<Object[], LogicalNamespace> schemaNames;
    private static HTreeMap<Long, ImmutableList<Long>> schemaChildren;

    private static BTreeMap<Long, LogicalTable> tables;
    private static BTreeMap<Object[], LogicalTable> tableNames;
    private static HTreeMap<Long, ImmutableList<Long>> tableChildren;

    private static BTreeMap<Long, LogicalCollection> collections;
    private static BTreeMap<Object[], LogicalCollection> collectionNames;

    private static BTreeMap<Object[], CatalogCollectionPlacement> collectionPlacements;

    private static BTreeMap<Long, CatalogCollectionMapping> documentMappings;

    private static BTreeMap<Long, LogicalColumn> columns;
    private static BTreeMap<Object[], LogicalColumn> columnNames;
    private static BTreeMap<Object[], CatalogColumnPlacement> columnPlacements;

    private static HTreeMap<Integer, CatalogAdapter> adapters;
    private static HTreeMap<String, CatalogAdapter> adapterNames;

    private static HTreeMap<Integer, CatalogQueryInterface> queryInterfaces;
    private static HTreeMap<String, CatalogQueryInterface> queryInterfaceNames;

    private static HTreeMap<Long, CatalogKey> keys;
    private static HTreeMap<long[], Long> keyColumns;

    private static HTreeMap<Long, CatalogPrimaryKey> primaryKeys;
    private static HTreeMap<Long, CatalogForeignKey> foreignKeys;
    private static HTreeMap<Long, CatalogConstraint> constraints;
    private static HTreeMap<Long, CatalogIndex> indexes;

    private static BTreeMap<Long, CatalogPartitionGroup> partitionGroups;
    private static BTreeMap<Long, CatalogPartition> partitions;
    private static BTreeMap<Object[], CatalogPartitionPlacement> partitionPlacements; // (AdapterId, Partition)

    // Container Object that contains all other placements
    private static BTreeMap<Object[], CatalogDataPlacement> dataPlacements; // (AdapterId, TableId) -> CatalogDataPlacement

    private static BTreeMap<Long, LogicalGraph> graphs;
    private static BTreeMap<String, LogicalGraph> graphAliases;
    private static BTreeMap<Object[], LogicalGraph> graphNames;
    private static BTreeMap<Object[], CatalogGraphPlacement> graphPlacements;

    private static BTreeMap<Long, CatalogGraphMapping> graphMappings;

    private static Long openTable;

    private static final AtomicInteger adapterIdBuilder = new AtomicInteger( 1 );
    private static final AtomicInteger queryInterfaceIdBuilder = new AtomicInteger( 1 );
    private static final AtomicInteger userIdBuilder = new AtomicInteger( 1 );

    private static final AtomicLong databaseIdBuilder = new AtomicLong( 1 );
    private static final AtomicLong namespaceIdBuilder = new AtomicLong( 1 );
    private static final AtomicLong entityIdBuilder = new AtomicLong( 1 );
    private static final AtomicLong columnIdBuilder = new AtomicLong( 1 );

    private static final AtomicLong partitionGroupIdBuilder = new AtomicLong( 1 );
    private static final AtomicLong partitionIdBuilder = new AtomicLong( 1000 );

    private static final AtomicLong keyIdBuilder = new AtomicLong( 1 );
    private static final AtomicLong constraintIdBuilder = new AtomicLong( 1 );
    private static final AtomicLong indexIdBuilder = new AtomicLong( 1 );
    private static final AtomicLong foreignKeyIdBuilder = new AtomicLong( 1 );

    private static final AtomicLong physicalPositionBuilder = new AtomicLong();

    private static Set<Long> frequencyDependentTables = new HashSet<>(); // All tables to consider for periodic processing

    // Keeps a list of all tableIDs which are going to be deleted. This is required to avoid constraints when recursively
    // removing a table and all placements and partitions. Otherwise **validatePartitionDistribution()** inside the Catalog
    // would throw an error.
    private static final List<Long> tablesFlaggedForDeletion = new ArrayList<>();

    Comparator<LogicalColumn> columnComparator = Comparator.comparingInt( o -> o.position );

    // {@link AlgNode} used to create view and materialized view
    @Getter
    private final Map<Long, AlgNode> nodeInfo = new HashMap<>();


    public CatalogImplBackup() {
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
    public CatalogImplBackup( String fileName, boolean doInitSchema, boolean doInitInformationPage, boolean deleteAfter ) {
        super();

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
                StatusService.printInfo( "Making the catalog persistent." );
                File folder = PolyphenyHomeDirManager.getInstance().registerNewFolder( "catalog" );

                if ( Catalog.resetCatalog ) {
                    StatusService.printInfo( "Resetting catalog on startup." );
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
                StatusService.printInfo( "Making the catalog in-memory." );
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

            } catch ( GenericCatalogException | UnknownUserException | UnknownTableException |
                      UnknownSchemaException | UnknownAdapterException | UnknownColumnException e ) {
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
     * Checks if a file can be created on the system, accessed and changed
     *
     * @return if it was possible
     */
    private boolean isPersistent() {
        File file = PolyphenyHomeDirManager.getInstance().registerNewFile( "testfile" );
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
            initGraphInfo( db );
            initDocumentInfo( db );
            initColumnInfo( db );
            initKeysAndConstraintsInfo( db );
            initAdapterInfo( db );
            initQueryInterfaceInfo( db );
        } catch ( SerializationError e ) {
            log.error( "!!!!!!!!!!! Error while restoring the catalog !!!!!!!!!!!" );
            log.error( "This usually means that there have been changes to the internal structure of the catalog with the last update of Polypheny-DB." );
            log.error( "To fix this, you must reset the catalog. To do this, please start Polypheny-DB once with the argument \"-resetCatalog\"." );
            StatusService.printError(
                    "Unsupported version of catalog! Unable to restore the schema.",
                    ErrorConfig.builder().func( ErrorConfig.DO_NOTHING ).doExit( true ).showButton( true ).buttonMessage( "Exit" ).build() );
        }
    }


    @Override
    public void restoreColumnPlacements( Transaction transaction ) {
        AdapterManager manager = AdapterManager.getInstance();

        Map<Integer, List<Long>> restoredTables = new HashMap<>();

        for ( LogicalColumn c : columns.values() ) {
            List<CatalogColumnPlacement> placements = getColumnPlacement( c.id );
            LogicalTable catalogTable = getTable( c.tableId );

            // No column placements need to be restored if it is a view
            if ( catalogTable.entityType != EntityType.VIEW ) {
                if ( placements.size() == 0 ) {
                    // No placements shouldn't happen
                    throw new RuntimeException( "There seems to be no placement for the column with the id " + c.id );
                } else if ( placements.size() == 1 ) {
                    Adapter adapter = manager.getAdapter( placements.get( 0 ).adapterId );
                    if ( adapter instanceof DataStore ) {
                        DataStore store = (DataStore) adapter;
                        if ( !store.isPersistent() ) {

                            // TODO only full placements atm here

                            if ( !restoredTables.containsKey( store.getAdapterId() ) ) {
                                store.createPhysicalTable( transaction.createStatement().getPrepareContext(), catalogTable, null );
                                restoredTables.put( store.getAdapterId(), Collections.singletonList( catalogTable.id ) );

                            } else if ( !(restoredTables.containsKey( store.getAdapterId() ) && restoredTables.get( store.getAdapterId() ).contains( catalogTable.id )) ) {
                                store.createPhysicalTable( transaction.createStatement().getPrepareContext(), catalogTable, null );
                                List<Long> ids = new ArrayList<>( restoredTables.get( store.getAdapterId() ) );
                                ids.add( catalogTable.id );
                                restoredTables.put( store.getAdapterId(), ids );
                            }
                        }
                    }
                } else {
                    Map<Integer, Boolean> persistent = placements.stream().collect( Collectors.toMap( p -> p.adapterId, p -> manager.getStore( p.adapterId ).isPersistent() ) );

                    if ( !persistent.containsValue( true ) ) { // no persistent placement for this column
                        LogicalTable table = getTable( c.tableId );
                        for ( CatalogColumnPlacement p : placements ) {
                            DataStore store = manager.getStore( p.adapterId );

                            if ( !restoredTables.containsKey( store.getAdapterId() ) ) {
                                store.createPhysicalTable( transaction.createStatement().getPrepareContext(), table, null );
                                List<Long> ids = new ArrayList<>();
                                ids.add( table.id );
                                restoredTables.put( store.getAdapterId(), ids );

                            } else if ( !(restoredTables.containsKey( store.getAdapterId() ) && restoredTables.get( store.getAdapterId() ).contains( table.id )) ) {
                                store.createPhysicalTable( transaction.createStatement().getPrepareContext(), table, null );
                                List<Long> ids = new ArrayList<>( restoredTables.get( store.getAdapterId() ) );
                                ids.add( table.id );
                                restoredTables.put( store.getAdapterId(), ids );
                            }
                        }
                    } else if ( persistent.containsValue( true ) && persistent.containsValue( false ) ) {
                        // TODO DL change so column gets copied
                        for ( Entry<Integer, Boolean> p : persistent.entrySet() ) {
                            if ( !p.getValue() ) {
                                deleteColumnPlacement( p.getKey(), c.id, false );
                            }
                        }
                    }
                }
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreViews( Transaction transaction ) {
        Statement statement = transaction.createStatement();

        for ( LogicalTable c : tables.values() ) {
            if ( c.entityType == EntityType.VIEW || c.entityType == EntityType.MATERIALIZED_VIEW ) {
                String query;
                QueryLanguage language;
                if ( c.entityType == EntityType.VIEW ) {
                    query = ((CatalogView) c).getQuery();
                    language = ((CatalogView) c).getLanguage();
                } else {
                    query = ((CatalogMaterializedView) c).getQuery();
                    language = ((CatalogMaterializedView) c).getLanguage();
                }

                switch ( language.getSerializedName() ) {
                    case "sql":
                        Processor sqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.from( "rel" ) );
                        Node sqlNode = sqlProcessor.parse( query ).get( 0 );
                        AlgRoot algRoot = sqlProcessor.translate(
                                statement,
                                sqlProcessor.validate( statement.getTransaction(), sqlNode, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() ).left,
                                new QueryParameters( query, c.getNamespaceType() ) );
                        nodeInfo.put( c.id, algRoot.alg );
                        break;

                    case "rel":
                        Processor jsonRelProcessor = statement.getTransaction().getProcessor( QueryLanguage.from( "rel" ) );
                        AlgNode result = jsonRelProcessor.translate( statement, null, new QueryParameters( query, c.getNamespaceType() ) ).alg;

                        final AlgDataType rowType = result.getRowType();
                        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
                        final AlgCollation collation =
                                result instanceof Sort
                                        ? ((Sort) result).collation
                                        : AlgCollations.EMPTY;
                        AlgRoot root = new AlgRoot( result, result.getRowType(), Kind.SELECT, fields, collation );

                        nodeInfo.put( c.id, root.alg );
                        break;

                    case "mongo":
                        Processor mqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.from( "mongo" ) );
                        Node mqlNode = mqlProcessor.parse( query ).get( 0 );

                        AlgRoot mqlRel = mqlProcessor.translate(
                                statement,
                                mqlNode,
                                new ExtendedQueryParameters( query, NamespaceType.DOCUMENT, getNamespace( defaultDatabaseId ).name ) );
                        nodeInfo.put( c.id, mqlRel.alg );
                        break;
                }
                if ( c.entityType == EntityType.MATERIALIZED_VIEW ) {
                    log.info( "Updating materialized view: {}", c.getNamespaceName() + "." + c.name );
                    MaterializedViewManager materializedManager = MaterializedViewManager.getInstance();
                    materializedManager.addMaterializedInfo( c.id, ((CatalogMaterializedView) c).getMaterializedCriteria() );
                    materializedManager.updateData( statement.getTransaction(), c.id );
                    materializedManager.updateMaterializedTime( c.id );
                }
            }
        }
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
        restoreIdBuilder( schemas, namespaceIdBuilder );
        restoreIdBuilder( databases, databaseIdBuilder );
        restoreIdBuilder( tables, entityIdBuilder );
        restoreIdBuilder( columns, columnIdBuilder );
        restoreIdBuilder( users, userIdBuilder );
        restoreIdBuilder( keys, keyIdBuilder );
        restoreIdBuilder( constraints, columnIdBuilder );
        restoreIdBuilder( indexes, indexIdBuilder );
        restoreIdBuilder( adapters, adapterIdBuilder );
        restoreIdBuilder( queryInterfaces, queryInterfaceIdBuilder );
        restoreIdBuilder( foreignKeys, foreignKeyIdBuilder );
        restoreIdBuilder( partitionGroups, partitionGroupIdBuilder );
        restoreIdBuilder( partitions, partitionIdBuilder );

        // Restore physical position builder
        if ( columnPlacements.size() > 0 ) {
            long highestPosition = 0;
            for ( CatalogColumnPlacement placement : columnPlacements.values() ) {
                if ( placement.physicalPosition > highestPosition ) {
                    highestPosition = placement.physicalPosition;
                }
            }
            physicalPositionBuilder.set( highestPosition + 1 );
        }
    }


    /**
     * Initiates all needed maps for adapters
     *
     * adapters: adapterId {@code ->} CatalogAdapter
     * adapterName: adapterName {@code ->}  CatalogAdapter
     */
    private void initAdapterInfo( DB db ) {
        adapters = db.hashMap( "adapters", Serializer.INTEGER, new GenericSerializer<CatalogAdapter>() ).createOrOpen();
        adapterNames = db.hashMap( "adapterNames", Serializer.STRING, new GenericSerializer<CatalogAdapter>() ).createOrOpen();
    }


    /**
     * Initiates all needed maps for query interfaces
     *
     * queryInterfaces: ifaceId  CatalogQueryInterface
     * queryInterfaceNames: ifaceName  CatalogQueryInterface
     */
    private void initQueryInterfaceInfo( DB db ) {
        queryInterfaces = db.hashMap( "queryInterfaces", Serializer.INTEGER, new GenericSerializer<CatalogQueryInterface>() ).createOrOpen();
        queryInterfaceNames = db.hashMap( "queryInterfaceNames", Serializer.STRING, new GenericSerializer<CatalogQueryInterface>() ).createOrOpen();
    }


    /**
     * Creates all needed maps for keys and constraints
     *
     * keyColumns: [columnId1, columnId2,...]  keyId
     * keys: keyId  CatalogKey
     * primaryKeys: keyId  CatalogPrimaryKey
     * foreignKeys: keyId  CatalogForeignKey
     * constraints: constraintId  CatalogConstraint
     * indexes: indexId {@code ->} CatalogIndex
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
     * Creates all needed maps for users
     *
     * users: userId {@code ->} CatalogUser
     * userNames: name {@code ->} CatalogUser
     */
    private void initUserInfo( DB db ) {
        users = db.hashMap( "users", Serializer.INTEGER, new GenericSerializer<CatalogUser>() ).createOrOpen();
        userNames = db.hashMap( "usersNames", Serializer.STRING, new GenericSerializer<CatalogUser>() ).createOrOpen();
    }


    /**
     * Initialize the column maps
     *
     * columns: columnId {@code ->} CatalogColumn
     * columnNames: new Object[]{databaseId, schemaId, tableId, columnName} {@code ->} CatalogColumn
     * columnPlacements: new Object[]{adapterId, columnId} {@code ->} CatalogPlacement
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
     * Creates all maps needed for tables
     *
     * tables: tableId {@code ->} CatalogEntity
     * tableChildren: tableId {@code ->} [columnId, columnId,..]
     * tableNames: new Object[]{databaseId, schemaId, tableName} {@code ->} CatalogEntity
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
        dataPlacements = db.treeMap( "dataPlacement", new SerializerArrayTuple( Serializer.INTEGER, Serializer.LONG ), Serializer.JAVA ).createOrOpen();
        partitionGroups = db.treeMap( "partitionGroups", Serializer.LONG, Serializer.JAVA ).createOrOpen();
        partitions = db.treeMap( "partitions", Serializer.LONG, Serializer.JAVA ).createOrOpen();

        partitionPlacements = db.treeMap( "partitionPlacements", new SerializerArrayTuple( Serializer.INTEGER, Serializer.LONG ), Serializer.JAVA ).createOrOpen();

        // Restores all Tables dependent on periodic checks like TEMPERATURE Partitioning
        frequencyDependentTables = tables.values().stream().filter( t -> t.partitionProperty.reliesOnPeriodicChecks ).map( t -> t.id ).collect( Collectors.toSet() );
    }


    @SuppressWarnings("unchecked")
    private void initGraphInfo( DB db ) {
        graphs = db.treeMap( "graphs", Serializer.LONG, Serializer.JAVA ).createOrOpen();
        graphNames = db.treeMap( "graphNames", new SerializerArrayTuple( Serializer.LONG, Serializer.STRING ), Serializer.JAVA ).createOrOpen();
        graphPlacements = db.treeMap( "graphPlacements", new SerializerArrayTuple( Serializer.LONG, Serializer.INTEGER ), Serializer.JAVA ).createOrOpen();

        graphMappings = db.treeMap( "graphMappings", Serializer.LONG, Serializer.JAVA ).createOrOpen();
        graphAliases = db.treeMap( "graphAliases", Serializer.STRING, Serializer.JAVA ).createOrOpen();
    }


    @SuppressWarnings("unchecked")
    private void initDocumentInfo( DB db ) {
        collections = db.treeMap( "collections", Serializer.LONG, Serializer.JAVA ).createOrOpen();
        collectionNames = db.treeMap( "collectionNames", new SerializerArrayTuple( Serializer.LONG, Serializer.LONG, Serializer.STRING ), Serializer.JAVA ).createOrOpen();

        documentMappings = db.treeMap( "documentMappings", Serializer.LONG, Serializer.JAVA ).createOrOpen();

        collectionPlacements = db.treeMap( "collectionPlacements", new SerializerArrayTuple( Serializer.LONG, Serializer.INTEGER ), Serializer.JAVA ).createOrOpen();
    }


    /**
     * Creates all needed maps for schemas
     *
     * schemas: schemaId {@code ->} CatalogNamespace
     * schemaChildren: schemaId {@code ->} [tableId, tableId, etc]
     * schemaNames: new Object[]{databaseId, schemaName} {@code ->} CatalogNamespace
     */
    private void initSchemaInfo( DB db ) {
        //noinspection unchecked
        schemas = db.treeMap( "schemas", Serializer.LONG, Serializer.JAVA ).createOrOpen();
        schemaChildren = db.hashMap( "schemaChildren", Serializer.LONG, new GenericSerializer<ImmutableList<Long>>() ).createOrOpen();
        //noinspection unchecked
        schemaNames = db.treeMap( "schemaNames", new SerializerArrayTuple( Serializer.LONG, Serializer.STRING ), Serializer.JAVA ).createOrOpen();
    }


    /**
     * Creates maps for databases
     *
     * databases: databaseId {@code ->} CatalogDatabase
     * databaseNames: databaseName {@code ->} CatalogDatabase
     * databaseChildren: databaseId {@code ->} [tableId, tableId,...]
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
    private void insertDefaultData() throws GenericCatalogException, UnknownUserException, UnknownTableException, UnknownSchemaException, UnknownAdapterException, UnknownColumnException {

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
        Catalog.defaultUserId = systemId;

        //////////////
        // init schema

        long schemaId;
        if ( !schemaNames.containsKey( new Object[]{ "public" } ) ) {
            schemaId = addNamespace( "public", NamespaceType.getDefault(), false );
        } else {
            schemaId = getNamespace( "public" ).id;
        }

        //////////////
        // init adapters
        if ( adapterNames.size() == 0 ) {
            // Deploy default store
            addAdapter( "hsqldb", defaultStore.getAdapterName(), AdapterType.STORE, defaultStore.getDefaultSettings() );

            // Deploy default CSV view
            addAdapter( "hr", defaultSource.getAdapterName(), AdapterType.SOURCE, defaultSource.getDefaultSettings() );

            // init schema
            CatalogAdapter csv = getAdapter( "hr" );
            if ( !testMode ) {
                if ( !tableNames.containsKey( new Object[]{ schemaId, "depts" } ) ) {
                    addTable( "depts", schemaId, systemId, EntityType.SOURCE, false );
                }
                if ( !tableNames.containsKey( new Object[]{ schemaId, "emps" } ) ) {
                    addTable( "emps", schemaId, systemId, EntityType.SOURCE, false );
                }
                if ( !tableNames.containsKey( new Object[]{ schemaId, "emp" } ) ) {
                    addTable( "emp", schemaId, systemId, EntityType.SOURCE, false );
                }
                if ( !tableNames.containsKey( new Object[]{ schemaId, "work" } ) ) {
                    addTable( "work", schemaId, systemId, EntityType.SOURCE, false );
                    addDefaultCsvColumns( csv );
                }
            }
        }

        try {
            commit();
        } catch ( NoTablePrimaryKeyException e ) {
            throw new RuntimeException( e );
        }

    }


    @Override
    public void restoreInterfacesIfNecessary() {
        ////////////////////////
        // init query interfaces
        if ( queryInterfaceNames.size() == 0 ) {
            QueryInterfaceManager.getREGISTER().values().forEach( i -> addQueryInterface( i.interfaceName, i.clazz.getName(), i.defaultSettings ) );

            try {
                commit();
            } catch ( NoTablePrimaryKeyException e ) {
                throw new RuntimeException( e );
            }
        }
    }


    /**
     * Initiates default columns for csv files
     */
    private void addDefaultCsvColumns( CatalogAdapter csv ) throws UnknownSchemaException, UnknownTableException, GenericCatalogException, UnknownColumnException {
        LogicalNamespace schema = getNamespace( "public" );
        LogicalTable depts = getTable( schema.id, "depts" );

        addDefaultCsvColumn( csv, depts, "deptno", PolyType.INTEGER, null, 1, null );
        addDefaultCsvColumn( csv, depts, "name", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 2, 20 );

        LogicalTable emps = getTable( schema.id, "emps" );
        addDefaultCsvColumn( csv, emps, "empid", PolyType.INTEGER, null, 1, null );
        addDefaultCsvColumn( csv, emps, "deptno", PolyType.INTEGER, null, 2, null );
        addDefaultCsvColumn( csv, emps, "name", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 3, 20 );
        addDefaultCsvColumn( csv, emps, "salary", PolyType.INTEGER, null, 4, null );
        addDefaultCsvColumn( csv, emps, "commission", PolyType.INTEGER, null, 5, null );

        LogicalTable emp = getTable( schema.id, "emp" );
        addDefaultCsvColumn( csv, emp, "employeeno", PolyType.INTEGER, null, 1, null );
        addDefaultCsvColumn( csv, emp, "age", PolyType.INTEGER, null, 2, null );
        addDefaultCsvColumn( csv, emp, "gender", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 3, 20 );
        addDefaultCsvColumn( csv, emp, "maritalstatus", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 4, 20 );
        addDefaultCsvColumn( csv, emp, "worklifebalance", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 5, 20 );
        addDefaultCsvColumn( csv, emp, "education", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 6, 20 );
        addDefaultCsvColumn( csv, emp, "monthlyincome", PolyType.INTEGER, null, 7, null );
        addDefaultCsvColumn( csv, emp, "relationshipjoy", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 8, 20 );
        addDefaultCsvColumn( csv, emp, "workingyears", PolyType.INTEGER, null, 9, null );
        addDefaultCsvColumn( csv, emp, "yearsatcompany", PolyType.INTEGER, null, 10, null );

        LogicalTable work = getTable( schema.id, "work" );
        addDefaultCsvColumn( csv, work, "employeeno", PolyType.INTEGER, null, 1, null );
        addDefaultCsvColumn( csv, work, "educationfield", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 2, 20 );
        addDefaultCsvColumn( csv, work, "jobinvolvement", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 3, 20 );
        addDefaultCsvColumn( csv, work, "joblevel", PolyType.INTEGER, null, 4, null );
        addDefaultCsvColumn( csv, work, "jobrole", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 5, 30 );
        addDefaultCsvColumn( csv, work, "businesstravel", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 6, 20 );
        addDefaultCsvColumn( csv, work, "department", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 7, 25 );
        addDefaultCsvColumn( csv, work, "attrition", PolyType.VARCHAR, Collation.CASE_INSENSITIVE, 8, 20 );
        addDefaultCsvColumn( csv, work, "dailyrate", PolyType.INTEGER, null, 9, null );

        // set all needed primary keys
        addPrimaryKey( depts.id, Collections.singletonList( getColumn( depts.id, "deptno" ).id ) );
        addPrimaryKey( emps.id, Collections.singletonList( getColumn( emps.id, "empid" ).id ) );
        addPrimaryKey( emp.id, Collections.singletonList( getColumn( emp.id, "employeeno" ).id ) );
        addPrimaryKey( work.id, Collections.singletonList( getColumn( work.id, "employeeno" ).id ) );

        // set foreign keys
        addForeignKey(
                emps.id,
                ImmutableList.of( getColumn( emps.id, "deptno" ).id ),
                depts.id,
                ImmutableList.of( getColumn( depts.id, "deptno" ).id ),
                "fk_emps_depts",
                ForeignKeyOption.NONE,
                ForeignKeyOption.NONE );
        addForeignKey(
                work.id,
                ImmutableList.of( getColumn( work.id, "employeeno" ).id ),
                emp.id,
                ImmutableList.of( getColumn( emp.id, "employeeno" ).id ),
                "fk_work_emp",
                ForeignKeyOption.NONE,
                ForeignKeyOption.NONE );
    }


    private void addDefaultCsvColumn( CatalogAdapter csv, LogicalTable table, String name, PolyType type, Collation collation, int position, Integer length ) {
        if ( !checkIfExistsColumn( table.id, name ) ) {
            long colId = addColumn( name, table.id, position, type, null, length, null, null, null, false, collation );
            String filename = table.name + ".csv";
            if ( table.name.equals( "emp" ) || table.name.equals( "work" ) ) {
                filename += ".gz";
            }

            addColumnPlacement( csv.id, colId, PlacementType.AUTOMATIC, filename, table.name, name );
            updateColumnPlacementPhysicalPosition( csv.id, colId, position );

            long partitionId = table.partitionProperty.partitionIds.get( 0 );
            addPartitionPlacement( table.namespaceId, csv.id, table.id, partitionId, PlacementType.AUTOMATIC, filename, table.name, DataPlacementRole.UPTODATE );
        }
    }


    private void addDefaultColumn( CatalogAdapter adapter, LogicalTable table, String name, PolyType type, Collation collation, int position, Integer length ) {
        if ( !checkIfExistsColumn( table.id, name ) ) {
            long colId = addColumn( name, table.id, position, type, null, length, null, null, null, false, collation );
            addColumnPlacement( adapter.id, colId, PlacementType.AUTOMATIC, "col" + colId, table.name, name );
            updateColumnPlacementPhysicalPosition( adapter.id, colId, position );
        }
    }


    /**
     * {@inheritDoc}
     */
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


    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        db.close();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        db.getAll().clear();
        initDBLayout( db );
        restoreAllIdBuilders();
    }


    @Override
    public Snapshot getSnapshot( long id ) {
        return null;
    }


    /**
     * {@inheritDoc}
     */
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


    /**
     * {@inheritDoc}
     */
    public void deleteDatabase( long databaseId ) {
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
     * {@inheritDoc}
     */
    @Override
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
     * {@inheritDoc}
     */
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
     * {@inheritDoc}
     */
    private CatalogDatabase getDatabase( long databaseId ) {
        try {
            return Objects.requireNonNull( databases.get( databaseId ) );
        } catch ( NullPointerException e ) {
            throw new UnknownDatabaseIdRuntimeException( databaseId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public @NonNull List<LogicalNamespace> getNamespaces( Pattern name ) {
        if ( name != null ) {
            return schemaNames.values().stream().filter( s -> s.name.matches( name.toRegex() ) ).collect( Collectors.toList() );
        }
        return new ArrayList<>();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public LogicalNamespace getNamespace( long id ) {
        try {
            return Objects.requireNonNull( schemas.get( id ) );
        } catch ( NullPointerException e ) {
            throw new UnknownSchemaIdRuntimeException( id );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public LogicalNamespace getNamespace( final String schemaName ) throws UnknownSchemaException {
        String name = schemaName.toLowerCase();
        try {
            return Objects.requireNonNull( schemaNames.get( new Object[]{ name } ) );
        } catch ( NullPointerException e ) {
            throw new UnknownSchemaException( schemaName );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addNamespace( String name, NamespaceType namespaceType, boolean caseSensitive ) {
        name = name.toLowerCase();
        CatalogUser owner = getUser( ownerId );
        long id = namespaceIdBuilder.getAndIncrement();
        LogicalNamespace schema = new LogicalNamespace( id, name, ownerId, owner.name, namespaceType, namespaceType == NamespaceType.DOCUMENT || namespaceType == NamespaceType.GRAPH );
        synchronized ( this ) {
            schemas.put( id, schema );
            schemaNames.put( new Object[]{ name }, schema );
            schemaChildren.put( id, ImmutableList.<Long>builder().build() );
        }
        listeners.firePropertyChange( "namespace", null, schema );
        return id;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkIfExistsNamespace( String name ) {
        name = name.toLowerCase();
        return schemaNames.containsKey( new Object[]{ name } );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void renameNamespace( long schemaId, String name ) {
        name = name.toLowerCase();
        try {
            LogicalNamespace old = Objects.requireNonNull( schemas.get( schemaId ) );
            LogicalNamespace schema = new LogicalNamespace( old.id, name, old.ownerId, old.ownerName, old.namespaceType, false );

            synchronized ( this ) {
                schemas.replace( schemaId, schema );
                schemaNames.remove( new Object[]{ old.name } );
                schemaNames.put( new Object[]{ name }, schema );
            }
            listeners.firePropertyChange( "schema", old, schema );
        } catch ( NullPointerException e ) {
            throw new UnknownSchemaIdRuntimeException( schemaId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addGraph( String name, List<DataStore> stores, boolean modifiable, boolean ifNotExists, boolean replace ) {
        if ( getGraphs( new Pattern( name ) ).size() != 0 && !ifNotExists ) {
            throw new GraphAlreadyExistsException( name );
        }

        long id = addNamespace( name, NamespaceType.GRAPH, false );

        LogicalGraph graph = new LogicalGraph( id, name, Catalog.defaultUserId, modifiable, ImmutableList.of(), true );

        synchronized ( this ) {
            graphs.put( id, graph );
            graphNames.put( new Object[]{ name }, graph );
        }

        listeners.firePropertyChange( "graph", null, graph );
        return id;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void addGraphAlias( long graphId, String alias, boolean ifNotExists ) {
        LogicalGraph graph = Objects.requireNonNull( getGraph( graphId ) );

        if ( graphAliases.containsKey( alias ) ) {
            if ( !ifNotExists ) {
                throw new RuntimeException( "Error while creating alias: " + alias );
            }
            return;
        }

        synchronized ( this ) {
            graphAliases.put( alias, graph );
        }
        listeners.firePropertyChange( "graphAlias", null, alias );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void removeGraphAlias( long graphId, String alias, boolean ifExists ) {
        if ( !graphAliases.containsKey( alias ) ) {
            if ( !ifExists ) {
                throw new RuntimeException( "Error while removing alias: " + alias );
            }
            return;
        }
        synchronized ( this ) {
            graphAliases.remove( alias );
        }
        listeners.firePropertyChange( "graphAlias", alias, null );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogGraphMapping getGraphMapping( long graphId ) {
        return Objects.requireNonNull( graphMappings.get( graphId ) );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void addGraphLogistics( long id, List<DataStore> stores, boolean onlyPlacement ) throws GenericCatalogException, UnknownTableException, UnknownColumnException {
        /// --- nodes
        // table id nodes -> id, node, labels
        long nodesId;
        if ( !onlyPlacement ) {
            nodesId = addTable( "_nodes_", id, Catalog.defaultUserId, EntityType.ENTITY, true );
        } else {
            nodesId = getTable( id, "_nodes_" ).id;
        }

        stores.forEach( store -> addDataPlacement( store.getAdapterId(), nodesId ) );

        long idNodeId;
        long labelNodeId;
        if ( !onlyPlacement ) {
            idNodeId = addColumn( "_id_", nodesId, 0, PolyType.VARCHAR, null, 255, null, null, null, false, Collation.getDefaultCollation() );
            labelNodeId = addColumn( "_label_", nodesId, 1, PolyType.VARCHAR, null, 255, null, null, null, false, Collation.getDefaultCollation() );
        } else {
            idNodeId = getColumn( nodesId, "_id_" ).id;
            labelNodeId = getColumn( nodesId, "_label_" ).id;
        }

        for ( DataStore s : stores ) {
            addColumnPlacement(
                    s.getAdapterId(),
                    idNodeId,
                    PlacementType.AUTOMATIC,
                    null,
                    null,
                    null
            );

            addColumnPlacement(
                    s.getAdapterId(),
                    labelNodeId,
                    PlacementType.AUTOMATIC,
                    null,
                    null,
                    null
            );
        }

        if ( !onlyPlacement ) {
            addPrimaryKey( nodesId, List.of( idNodeId, labelNodeId ) );
        }

        /// --- node properties

        // table id nodes -> id, node, labels
        long nodesPropertyId;
        if ( !onlyPlacement ) {
            nodesPropertyId = addTable( "_n_properties_", id, Catalog.defaultUserId, EntityType.ENTITY, true );
        } else {
            nodesPropertyId = getTable( id, "_n_properties_" ).id;
        }

        stores.forEach( store -> addDataPlacement( store.getAdapterId(), nodesPropertyId ) );

        long idNodesPropertyId;
        long keyNodePropertyId;
        long valueNodePropertyId;

        if ( !onlyPlacement ) {
            idNodesPropertyId = addColumn( "_id_", nodesPropertyId, 0, PolyType.VARCHAR, null, 255, null, null, null, false, Collation.getDefaultCollation() );
            keyNodePropertyId = addColumn( "_key_", nodesPropertyId, 1, PolyType.VARCHAR, null, 255, null, null, null, false, Collation.getDefaultCollation() );
            valueNodePropertyId = addColumn( "_value_", nodesPropertyId, 2, PolyType.VARCHAR, null, 255, null, null, null, false, Collation.getDefaultCollation() );
        } else {
            idNodesPropertyId = getColumn( nodesPropertyId, "_id_" ).id;
            keyNodePropertyId = getColumn( nodesPropertyId, "_key_" ).id;
            valueNodePropertyId = getColumn( nodesPropertyId, "_value_" ).id;
        }

        for ( DataStore s : stores ) {
            addColumnPlacement(
                    s.getAdapterId(),
                    idNodesPropertyId,
                    PlacementType.AUTOMATIC,
                    null,
                    null,
                    null
            );

            addColumnPlacement(
                    s.getAdapterId(),
                    keyNodePropertyId,
                    PlacementType.AUTOMATIC,
                    null,
                    null,
                    null
            );

            addColumnPlacement(
                    s.getAdapterId(),
                    valueNodePropertyId,
                    PlacementType.AUTOMATIC,
                    null,
                    null,
                    null
            );
        }

        if ( !onlyPlacement ) {
            addPrimaryKey( nodesPropertyId, List.of( idNodesPropertyId, keyNodePropertyId ) );
        }

        /// --- edges

        // table id relationships -> id, rel, labels
        long edgesId;
        if ( !onlyPlacement ) {
            edgesId = addTable( "_edges_", id, Catalog.defaultUserId, EntityType.ENTITY, true );
        } else {
            edgesId = getTable( id, "_edges_" ).id;
        }

        stores.forEach( store -> addDataPlacement( store.getAdapterId(), edgesId ) );

        long idEdgeId;
        long labelEdgeId;
        long sourceEdgeId;
        long targetEdgeId;

        if ( !onlyPlacement ) {
            idEdgeId = addColumn(
                    "_id_",
                    edgesId,
                    0,
                    PolyType.VARCHAR,
                    null,
                    36,
                    null,
                    null,
                    null,
                    false,
                    Collation.getDefaultCollation() );
            labelEdgeId = addColumn(
                    "_label_",
                    edgesId,
                    1,
                    PolyType.VARCHAR,
                    null,
                    255,
                    null,
                    null,
                    null,
                    false,
                    Collation.getDefaultCollation() );
            sourceEdgeId = addColumn(
                    "_l_id_",
                    edgesId,
                    2,
                    PolyType.VARCHAR,
                    null,
                    36,
                    null,
                    null,
                    null,
                    false,
                    Collation.getDefaultCollation() );
            targetEdgeId = addColumn(
                    "_r_id_",
                    edgesId,
                    3,
                    PolyType.VARCHAR,
                    null,
                    36,
                    null,
                    null,
                    null,
                    false,
                    Collation.getDefaultCollation() );
        } else {
            idEdgeId = getColumn( edgesId, "_id_" ).id;
            labelEdgeId = getColumn( edgesId, "_label_" ).id;
            sourceEdgeId = getColumn( edgesId, "_l_id_" ).id;
            targetEdgeId = getColumn( edgesId, "_r_id_" ).id;
        }

        for ( DataStore store : stores ) {
            addColumnPlacement(
                    store.getAdapterId(),
                    idEdgeId,
                    PlacementType.AUTOMATIC,
                    null,
                    null,
                    null
            );

            addColumnPlacement(
                    store.getAdapterId(),
                    labelEdgeId,
                    PlacementType.AUTOMATIC,
                    null,
                    null,
                    null
            );

            addColumnPlacement(
                    store.getAdapterId(),
                    sourceEdgeId,
                    PlacementType.AUTOMATIC,
                    null,
                    null,
                    null
            );

            addColumnPlacement(
                    store.getAdapterId(),
                    targetEdgeId,
                    PlacementType.AUTOMATIC,
                    null,
                    null,
                    null
            );
        }

        if ( !onlyPlacement ) {
            addPrimaryKey( edgesId, Collections.singletonList( idEdgeId ) );
        }

        /// --- edge properties

        // table id nodes -> id, node, labels
        long edgesPropertyId;
        if ( !onlyPlacement ) {
            edgesPropertyId = addTable( "_properties_", id, Catalog.defaultUserId, EntityType.ENTITY, true );
        } else {
            edgesPropertyId = getTable( id, "_properties_" ).id;
        }

        stores.forEach( store -> addDataPlacement( store.getAdapterId(), edgesPropertyId ) );

        long idEdgePropertyId;
        long keyEdgePropertyId;
        long valueEdgePropertyId;

        if ( !onlyPlacement ) {
            idEdgePropertyId = addColumn(
                    "_id_",
                    edgesPropertyId,
                    0,
                    PolyType.VARCHAR,
                    null,
                    255,
                    null,
                    null,
                    null,
                    false,
                    Collation.getDefaultCollation() );
            keyEdgePropertyId = addColumn(
                    "_key_",
                    edgesPropertyId,
                    1,
                    PolyType.VARCHAR,
                    null,
                    255,
                    null,
                    null,
                    null,
                    false,
                    Collation.getDefaultCollation() );
            valueEdgePropertyId = addColumn(
                    "_value_",
                    edgesPropertyId,
                    2,
                    PolyType.VARCHAR,
                    null,
                    255,
                    null,
                    null,
                    null,
                    false,
                    Collation.getDefaultCollation() );
        } else {
            idEdgePropertyId = getColumn( edgesPropertyId, "_id_" ).id;
            keyEdgePropertyId = getColumn( edgesPropertyId, "_key_" ).id;
            valueEdgePropertyId = getColumn( edgesPropertyId, "_value_" ).id;
        }

        for ( DataStore s : stores ) {
            addColumnPlacement(
                    s.getAdapterId(),
                    idEdgePropertyId,
                    PlacementType.AUTOMATIC,
                    null,
                    null,
                    null
            );

            addColumnPlacement(
                    s.getAdapterId(),
                    keyEdgePropertyId,
                    PlacementType.AUTOMATIC,
                    null,
                    null,
                    null
            );

            addColumnPlacement(
                    s.getAdapterId(),
                    valueEdgePropertyId,
                    PlacementType.AUTOMATIC,
                    null,
                    null,
                    null
            );
        }

        if ( !onlyPlacement ) {
            addPrimaryKey( edgesPropertyId, List.of( idEdgePropertyId, keyEdgePropertyId ) );

            CatalogGraphMapping mapping = new CatalogGraphMapping(
                    id,
                    nodesId,
                    idNodeId,
                    labelNodeId,
                    nodesPropertyId,
                    idNodesPropertyId,
                    keyNodePropertyId,
                    valueNodePropertyId,
                    edgesId,
                    idEdgeId,
                    labelEdgeId,
                    sourceEdgeId,
                    targetEdgeId,
                    edgesPropertyId,
                    idEdgePropertyId,
                    keyEdgePropertyId,
                    valueEdgePropertyId );

            graphMappings.put( id, mapping );
        }

    }


    private void removeGraphLogistics( long graphId ) {
        if ( !graphMappings.containsKey( graphId ) ) {
            throw new UnknownGraphException( graphId );
        }

        deleteNamespace( graphId );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteGraph( long id ) {
        if ( !graphs.containsKey( id ) ) {
            throw new UnknownGraphException( id );
        }

        LogicalGraph old = Objects.requireNonNull( graphs.get( id ) );

        removeGraphLogistics( id );

        synchronized ( this ) {
            old.placements.forEach( a -> graphPlacements.remove( new Object[]{ old.id, a } ) );
            graphs.remove( id );
            graphNames.remove( new Object[]{ old.name } );
            graphMappings.remove( id );
        }
        listeners.firePropertyChange( "graph", old, null );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public LogicalGraph getGraph( long id ) {
        if ( !graphs.containsKey( id ) ) {
            throw new UnknownGraphException( id );
        }
        return graphs.get( id );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<LogicalGraph> getGraphs( Pattern graphName ) {
        if ( graphName != null ) {
            return ImmutableList.copyOf(
                    Stream.concat(
                                    graphAliases.values().stream(),
                                    graphs.values().stream() ).filter( g -> g.name.matches( graphName.pattern.toLowerCase() ) )
                            .collect( Collectors.toList() ) );
        } else {
            return ImmutableList.copyOf( graphs.values() );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteNamespace( long schemaId ) {
        LogicalNamespace schema = getNamespace( schemaId );
        synchronized ( this ) {
            schemaNames.remove( new Object[]{ schema.name } );

            for ( Long id : Objects.requireNonNull( schemaChildren.get( schemaId ) ) ) {
                deleteTable( id );
            }
            schemaChildren.remove( schemaId );
            schemas.remove( schemaId );

        }
        listeners.firePropertyChange( "Schema", schema, null );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<LogicalTable> getTables( long schemaId, Pattern tableNamePattern ) {
        if ( schemas.containsKey( schemaId ) ) {

            LogicalNamespace schema = Objects.requireNonNull( schemas.get( schemaId ) );
            if ( tableNamePattern != null ) {
                return Collections.singletonList( tableNames.get( new Object[]{ schemaId, tableNamePattern.pattern } ) );
            } else {
                return new ArrayList<>( tableNames.prefixSubMap( new Object[]{ schemaId } ).values() );
            }
        }
        return new ArrayList<>();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<LogicalTable> getTables( Pattern schemaNamePattern, Pattern tableNamePattern ) {
        if ( schemaNamePattern != null && tableNamePattern != null ) {
            LogicalNamespace schema = schemaNames.get( new Object[]{ schemaNamePattern.pattern } );
            if ( schema != null ) {
                return Collections.singletonList( Objects.requireNonNull( tableNames.get( new Object[]{ schema.id, tableNamePattern.pattern } ) ) );
            }
        } else if ( schemaNamePattern != null ) {
            LogicalNamespace schema = schemaNames.get( new Object[]{ schemaNamePattern.pattern } );
            if ( schema != null ) {
                return new ArrayList<>( tableNames.prefixSubMap( new Object[]{ schema.id } ).values() );
            }
        } else {
            return new ArrayList<>( tableNames.values() );
        }

        return new ArrayList<>();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public LogicalTable getTable( long tableId ) {
        try {
            return Objects.requireNonNull( tables.get( tableId ) );
        } catch ( NullPointerException e ) {
            throw new UnknownTableIdRuntimeException( tableId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public LogicalTable getTable( long schemaId, String tableName ) throws UnknownTableException {
        try {
            LogicalNamespace schema = getNamespace( schemaId );
            if ( !schema.caseSensitive ) {
                tableName = tableName.toLowerCase();
            }
            return Objects.requireNonNull( tableNames.get( new Object[]{ schemaId, tableName } ) );
        } catch ( NullPointerException e ) {
            throw new UnknownTableException( schemaId, tableName );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public LogicalTable getTableFromPartition( long partitionId ) {
        return getTable( getPartition( partitionId ).tableId );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public LogicalTable getTable( String schemaName, String tableName ) throws UnknownTableException, UnknownSchemaException {
        try {
            LogicalNamespace schema = getNamespace( schemaName );
            if ( !schema.caseSensitive ) {
                tableName = tableName.toLowerCase();
            }

            return Objects.requireNonNull( tableNames.get( new Object[]{ schema.id, tableName } ) );
        } catch ( NullPointerException e ) {
            throw new UnknownTableException( schemaName, tableName );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addTable( String name, long namespaceId, int ownerId, EntityType entityType, boolean modifiable ) {
        long id = entityIdBuilder.getAndIncrement();
        LogicalNamespace schema = getNamespace( namespaceId );
        if ( !schema.caseSensitive ) {
            name = name.toLowerCase();
        }

        try {
            //Technically every Table is partitioned. But tables classified as UNPARTITIONED only consist of one PartitionGroup and one large partition
            List<Long> partitionGroupIds = new ArrayList<>();
            partitionGroupIds.add( addPartitionGroup( id, "full", namespaceId, PartitionType.NONE, 1, new ArrayList<>(), true ) );
            //get All(only one) PartitionGroups and then get all partitionIds  for each PG and add them to completeList of partitionIds
            CatalogPartitionGroup defaultUnpartitionedGroup = getPartitionGroup( partitionGroupIds.get( 0 ) );

            PartitionProperty partitionProperty = PartitionProperty.builder()
                    .partitionType( PartitionType.NONE )
                    .isPartitioned( false )
                    .partitionGroupIds( ImmutableList.copyOf( partitionGroupIds ) )
                    .partitionIds( ImmutableList.copyOf( defaultUnpartitionedGroup.partitionIds ) )
                    .reliesOnPeriodicChecks( false )
                    .build();

            LogicalTable table = new LogicalTable(
                    id,
                    name,
                    ImmutableList.of(),
                    namespaceId,
                    ownerId,
                    entityType,
                    null,
                    ImmutableList.of(),
                    modifiable,
                    partitionProperty,
                    ImmutableList.of() );

            updateEntityLogistics( name, namespaceId, id, schema, table );
            if ( schema.namespaceType != NamespaceType.DOCUMENT ) {
                openTable = id;
            }

        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( "Error when adding table " + name, e );
        }
        return id;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addView( String name, long namespaceId, int ownerId, EntityType entityType, boolean modifiable, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList, String query, QueryLanguage language ) {
        long id = entityIdBuilder.getAndIncrement();
        LogicalNamespace schema = getNamespace( namespaceId );

        if ( !schema.caseSensitive ) {
            name = name.toLowerCase();
        }

        PartitionProperty partitionProperty = PartitionProperty.builder()
                .partitionType( PartitionType.NONE )
                .reliesOnPeriodicChecks( false )
                .partitionIds( ImmutableList.copyOf( new ArrayList<>() ) )
                .partitionGroupIds( ImmutableList.copyOf( new ArrayList<>() ) )
                .build();

        if ( entityType != EntityType.VIEW ) {
            // Should not happen, addViewTable is only called with EntityType.View
            throw new RuntimeException( "addViewTable is only possible with EntityType = VIEW" );
        }
        CatalogView viewTable = new CatalogView(
                id,
                name,
                ImmutableList.of(),
                namespaceId,
                ownerId,
                entityType,
                query,//definition,
                null,
                ImmutableList.of(),
                modifiable,
                partitionProperty,
                algCollation,
                ImmutableList.of(),
                underlyingTables,
                language.getSerializedName() //fieldList
        );
        addConnectedViews( underlyingTables, viewTable.id );
        updateEntityLogistics( name, namespaceId, id, schema, viewTable );
        nodeInfo.put( id, definition );

        return id;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addMaterializedView( String name, long namespaceId, int ownerId, EntityType entityType, boolean modifiable, AlgNode definition, AlgCollation algCollation, Map<Long, List<Long>> underlyingTables, AlgDataType fieldList, MaterializedCriteria materializedCriteria, String query, QueryLanguage language, boolean ordered ) throws GenericCatalogException {
        long id = entityIdBuilder.getAndIncrement();
        LogicalNamespace schema = getNamespace( namespaceId );

        if ( !schema.caseSensitive ) {
            name = name.toLowerCase();
        }

        // Technically every Table is partitioned. But tables classified as UNPARTITIONED only consist of one PartitionGroup and one large partition
        List<Long> partitionGroupIds = new ArrayList<>();
        partitionGroupIds.add( addPartitionGroup( id, "full", namespaceId, PartitionType.NONE, 1, new ArrayList<>(), true ) );

        // Get the single PartitionGroup and consequently retrieve all contained partitionIds to add them to completeList of partitionIds in the partitionProperty
        CatalogPartitionGroup defaultUnpartitionedGroup = getPartitionGroup( partitionGroupIds.get( 0 ) );

        PartitionProperty partitionProperty = PartitionProperty.builder()
                .partitionType( PartitionType.NONE )
                .isPartitioned( false )
                .partitionGroupIds( ImmutableList.copyOf( partitionGroupIds ) )
                .partitionIds( ImmutableList.copyOf( defaultUnpartitionedGroup.partitionIds ) )
                .reliesOnPeriodicChecks( false )
                .build();

        if ( entityType == EntityType.MATERIALIZED_VIEW ) {
            Map<Long, ImmutableList<Long>> map = new HashMap<>();
            for ( Entry<Long, List<Long>> e : underlyingTables.entrySet() ) {
                if ( map.put( e.getKey(), ImmutableList.copyOf( e.getValue() ) ) != null ) {
                    throw new IllegalStateException( "Duplicate key" );
                }
            }
            CatalogMaterializedView materializedViewTable = new CatalogMaterializedView(
                    id,
                    name,
                    List.of(),
                    namespaceId,
                    ownerId,
                    entityType,
                    query,
                    null,
                    List.of(),
                    modifiable,
                    partitionProperty,
                    algCollation,
                    List.of(),
                    Map.copyOf( map ),
                    language.getSerializedName(),
                    materializedCriteria,
                    ordered
            );
            addConnectedViews( underlyingTables, materializedViewTable.id );
            updateEntityLogistics( name, namespaceId, id, schema, materializedViewTable );

            nodeInfo.put( id, definition );
        } else {
            // Should not happen, addViewTable is only called with EntityType.View
            throw new RuntimeException( "addMaterializedViewTable is only possible with EntityType = MATERIALIZED_VIEW" );
        }
        return id;
    }


    /**
     * Update all information after the addition of all kind of tables
     */
    private void updateEntityLogistics( String name, long namespaceId, long id, LogicalNamespace schema, LogicalTable entity ) {
        synchronized ( this ) {
            tables.put( id, entity );
            tableChildren.put( id, ImmutableList.<Long>builder().build() );
            tableNames.put( new Object[]{ namespaceId, name }, entity );
            List<Long> children = new ArrayList<>( Objects.requireNonNull( schemaChildren.get( namespaceId ) ) );
            children.add( id );
            schemaChildren.replace( namespaceId, ImmutableList.copyOf( children ) );
        }

        listeners.firePropertyChange( "entity", null, entity );
    }


    /**
     * Add additional Information to Table, what Views are connected to table
     */
    public void addConnectedViews( Map<Long, List<Long>> underlyingTables, long viewId ) {
        for ( long id : underlyingTables.keySet() ) {
            LogicalTable old = getTable( id );
            List<Long> connectedViews;
            connectedViews = new ArrayList<>( old.connectedViews );
            connectedViews.add( viewId );
            LogicalTable table = old.withConnectedViews( ImmutableList.copyOf( connectedViews ) );
            synchronized ( this ) {
                tables.replace( id, table );
                assert table != null;
                tableNames.replace( new Object[]{ table.namespaceId, old.name }, table );
            }
            listeners.firePropertyChange( "table", old, table );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteViewDependencies( CatalogView catalogView ) {
        for ( long id : catalogView.getUnderlyingTables().keySet() ) {
            LogicalTable old = getTable( id );
            List<Long> connectedViews = old.connectedViews.stream().filter( e -> e != catalogView.id ).collect( Collectors.toList() );

            LogicalTable table = old.withConnectedViews( ImmutableList.copyOf( connectedViews ) );

            synchronized ( this ) {
                tables.replace( id, table );
                assert table != null;
                tableNames.replace( new Object[]{ table.namespaceId, old.name }, table );
            }
            listeners.firePropertyChange( "table", old, table );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkIfExistsEntity( long namespaceId, String entityName ) {
        LogicalNamespace schema = getNamespace( namespaceId );
        if ( !schema.caseSensitive ) {
            entityName = entityName.toLowerCase();
        }
        return tableNames.containsKey( new Object[]{ namespaceId, entityName } );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkIfExistsEntity( long tableId ) {
        return tables.containsKey( tableId );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void renameTable( long tableId, String name ) {
        LogicalTable old = getTable( tableId );
        if ( !getNamespace( old.namespaceId ).caseSensitive ) {
            name = name.toLowerCase();
        }

        LogicalTable table = old.withName( name );
        synchronized ( this ) {
            tables.replace( tableId, table );
            tableNames.remove( new Object[]{ table.namespaceId, old.name } );
            tableNames.put( new Object[]{ table.namespaceId, name }, table );
        }
        listeners.firePropertyChange( "table", old, table );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteTable( long tableId ) {
        LogicalTable table = getTable( tableId );
        List<Long> children = new ArrayList<>( Objects.requireNonNull( schemaChildren.get( table.namespaceId ) ) );
        children.remove( tableId );
        synchronized ( this ) {
            schemaChildren.replace( table.namespaceId, ImmutableList.copyOf( children ) );

            if ( table.partitionProperty.reliesOnPeriodicChecks ) {
                removeTableFromPeriodicProcessing( tableId );
            }

            if ( table.partitionProperty.isPartitioned ) {
                for ( Long partitionGroupId : Objects.requireNonNull( table.partitionProperty.partitionGroupIds ) ) {
                    deletePartitionGroup( table.id, table.namespaceId, partitionGroupId );
                }
            }

            for ( Long columnId : Objects.requireNonNull( tableChildren.get( tableId ) ) ) {
                deleteColumn( columnId );
            }

            // Remove all placement containers along with all placements
            table.dataPlacements.forEach( adapterId -> removeDataPlacement( adapterId, tableId ) );

            tableChildren.remove( tableId );
            tables.remove( tableId );
            tableNames.remove( new Object[]{ table.namespaceId, table.name } );
            flagTableForDeletion( table.id, false );
            // primary key was deleted and open table has to be closed
            if ( openTable != null && openTable == tableId ) {
                openTable = null;
            }

        }
        listeners.firePropertyChange( "table", table, null );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setTableOwner( long tableId, int ownerId ) {
        LogicalTable old = getTable( tableId );
        LogicalTable table = old.withOwnerId( ownerId );

        synchronized ( this ) {
            tables.replace( tableId, table );
            tableNames.replace( new Object[]{ table.namespaceId, table.name }, table );
        }
        listeners.firePropertyChange( "table", old, table );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setPrimaryKey( long tableId, Long keyId ) {
        LogicalTable old = getTable( tableId );

        LogicalTable table = old.withPrimaryKey( keyId );

        synchronized ( this ) {
            tables.replace( tableId, table );
            tableNames.replace( new Object[]{ table.namespaceId, table.name }, table );

            if ( keyId == null ) {
                openTable = tableId;
            } else {
                primaryKeys.put( keyId, new CatalogPrimaryKey( Objects.requireNonNull( keys.get( keyId ) ) ) );
                openTable = null;
            }
        }
        listeners.firePropertyChange( "table", old, table );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void addColumnPlacement( int adapterId, long columnId, PlacementType placementType, String physicalSchemaName, String physicalTableName, String physicalColumnName ) {
        LogicalColumn column = Objects.requireNonNull( columns.get( columnId ) );
        CatalogAdapter store = Objects.requireNonNull( adapters.get( adapterId ) );

        CatalogColumnPlacement columnPlacement = new CatalogColumnPlacement(
                column.schemaId,
                column.tableId,
                columnId,
                adapterId,
                store.uniqueName,
                placementType,
                physicalSchemaName,
                physicalColumnName,
                physicalPositionBuilder.getAndIncrement() );

        synchronized ( this ) {
            columnPlacements.put( new Object[]{ adapterId, columnId }, columnPlacement );

            // Adds this ColumnPlacement to existing DataPlacement container
            addColumnsToDataPlacement( adapterId, column.tableId, List.of( columnId ) );
        }
        listeners.firePropertyChange( "columnPlacement", null, columnPlacement );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateMaterializedViewRefreshTime( long materializedViewId ) {
        CatalogMaterializedView old = (CatalogMaterializedView) getTable( materializedViewId );

        MaterializedCriteria materializedCriteria = old.getMaterializedCriteria();
        materializedCriteria.setLastUpdate( new Timestamp( System.currentTimeMillis() ) );

        CatalogMaterializedView view = old.withMaterializedCriteria( materializedCriteria );

        synchronized ( this ) {
            tables.replace( materializedViewId, view );
            tableNames.replace(
                    new Object[]{ view.namespaceId, view.name },
                    view );
        }
        listeners.firePropertyChange( "table", old, view );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public LogicalCollection getCollection( long id ) {
        if ( !collections.containsKey( id ) ) {
            throw new UnknownTableIdRuntimeException( id );
        }
        return Objects.requireNonNull( collections.get( id ) );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<LogicalCollection> getCollections( long namespaceId, Pattern namePattern ) {
        if ( schemas.containsKey( namespaceId ) ) {
            LogicalNamespace schema = Objects.requireNonNull( schemas.get( namespaceId ) );
            if ( namePattern != null ) {
                LogicalCollection collection = collectionNames.get( new Object[]{ namespaceId, namePattern.pattern } );
                if ( collection == null ) {
                    return new ArrayList<>();
                }
                return Collections.singletonList( collection );
            } else {
                return new ArrayList<>( collectionNames.prefixSubMap( new Object[]{ namespaceId } ).values() );
            }
        }
        return new ArrayList<>();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addCollection( Long id, String name, long schemaId, int currentUserId, EntityType entity, boolean modifiable ) {
        long collectionId = entityIdBuilder.getAndIncrement();
        if ( id != null ) {
            collectionId = id;
        }

        LogicalNamespace namespace = getNamespace( schemaId );
        LogicalCollection collection = new LogicalCollection(
                Catalog.defaultDatabaseId,
                schemaId,
                collectionId,
                name,
                List.of(),
                EntityType.ENTITY,
                null );

        synchronized ( this ) {
            collections.put( collectionId, collection );
            collectionNames.put( new Object[]{ schemaId, name }, collection );
        }
        listeners.firePropertyChange( "collection", null, entity );

        return collectionId;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addCollectionPlacement( long namespaceId, int adapterId, long collectionId, PlacementType placementType ) {
        long id = partitionIdBuilder.getAndIncrement();
        CatalogCollectionPlacement placement = new CatalogCollectionPlacement( namespaceId, adapterId, collectionId, null, null, id );
        LogicalCollection old = collections.get( collectionId );
        if ( old == null ) {
            throw new UnknownCollectionException( collectionId );
        }

        LogicalCollection collection = old.addPlacement( adapterId );

        synchronized ( this ) {
            collectionPlacements.put( new Object[]{ collectionId, adapterId }, placement );
            collections.replace( collectionId, collection );
            collectionNames.replace( new Object[]{ collection.databaseId, collection.namespaceId, collection.name }, collection );
        }
        listeners.firePropertyChange( "collectionPlacement", null, placement );
        return id;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogCollectionMapping getCollectionMapping( long id ) {
        if ( !documentMappings.containsKey( id ) ) {
            throw new UnknownTableIdRuntimeException( id );
        }
        return Objects.requireNonNull( documentMappings.get( id ) );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addCollectionLogistics( long schemaId, String name, List<DataStore> stores, boolean onlyPlacement ) throws GenericCatalogException {
        long tableId;
        if ( onlyPlacement ) {
            try {
                tableId = getTable( schemaId, name ).id;
            } catch ( UnknownTableException e ) {
                throw new RuntimeException( e );
            }
        } else {
            tableId = addTable( name, schemaId, Catalog.defaultUserId, EntityType.ENTITY, true );
        }

        stores.forEach( store -> addDataPlacement( store.getAdapterId(), tableId ) );

        long idId;
        long dataId;
        if ( !onlyPlacement ) {
            idId = addColumn( "_id_", tableId, 0, PolyType.VARCHAR, null, 255, null, null, null, false, Collation.getDefaultCollation() );
            dataId = addColumn( "_data_", tableId, 1, PolyType.JSON, null, null, null, null, null, false, Collation.getDefaultCollation() );
        } else {
            try {
                idId = getColumn( tableId, "_id_" ).id;
                dataId = getColumn( tableId, "_data_" ).id;
            } catch ( UnknownColumnException e ) {
                throw new RuntimeException( "Error while adding a document placement." );
            }
        }

        for ( DataStore s : stores ) {
            addColumnPlacement(
                    s.getAdapterId(),
                    idId,
                    PlacementType.AUTOMATIC,
                    null,
                    null,
                    null
            );

            addColumnPlacement(
                    s.getAdapterId(),
                    dataId,
                    PlacementType.AUTOMATIC,
                    null,
                    null,
                    null
            );
        }

        addPrimaryKey( tableId, List.of( idId, dataId ) );

        if ( !onlyPlacement ) {
            CatalogCollectionMapping mapping = new CatalogCollectionMapping( tableId, idId, dataId );
            documentMappings.put( tableId, mapping );
        }

        return tableId;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteCollection( long id ) {
        LogicalCollection collection = getCollection( id );

        synchronized ( this ) {
            collections.remove( collection.namespaceId );
            collectionNames.remove( new Object[]{ collection.databaseId, collection.namespaceId, collection.name } );
            collection.placements.forEach( p -> collectionPlacements.remove( new Object[]{ collection.id, p } ) );
        }
        listeners.firePropertyChange( "collection", null, null );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void dropCollectionPlacement( long id, int adapterId ) {
        LogicalCollection oldCollection = Objects.requireNonNull( collections.get( id ) );
        LogicalCollection collection = oldCollection.removePlacement( adapterId );

        synchronized ( this ) {
            collectionPlacements.remove( new Object[]{ id, adapterId } );
            collections.replace( id, collection );
            collectionNames.replace( new Object[]{ collection.databaseId, collection.namespaceId, collection.name }, collection );
        }
        listeners.firePropertyChange( "collectionPlacement", null, null );
    }


    /**
     * {@inheritDoc}
     */
    public List<CatalogGraphPlacement> getGraphPlacements( int adapterId ) {
        return graphPlacements.entrySet().stream().filter( e -> e.getKey()[1].equals( adapterId ) ).map( Entry::getValue ).collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogCollectionPlacement> getCollectionPlacementsByAdapter( int adapterId ) {
        return collectionPlacements.values().stream().filter( p -> p.adapter == adapterId ).collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogCollectionPlacement getCollectionPlacement( long collectionId, int adapterId ) {
        if ( !collectionPlacements.containsKey( new Object[]{ collectionId, adapterId } ) ) {
            throw new UnknownCollectionPlacementException( collectionId, adapterId );
        }

        return collectionPlacements.get( new Object[]{ collectionId, adapterId } );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteColumnPlacement( int adapterId, long columnId, boolean columnOnly ) {
        LogicalTable oldTable = getTable( getColumn( columnId ).tableId );

        synchronized ( this ) {
            if ( log.isDebugEnabled() ) {
                log.debug( "Is flagged for deletion {}", isTableFlaggedForDeletion( oldTable.id ) );
            }

            if ( oldTable.partitionProperty.isPartitioned ) {
                if ( !isTableFlaggedForDeletion( oldTable.id ) ) {
                    if ( !columnOnly ) {
                        if ( !validateDataPlacementsConstraints( oldTable.id, adapterId, Arrays.asList( columnId ), new ArrayList<>() ) ) {
                            throw new RuntimeException( "Partition Distribution failed" );
                        }
                    }
                }
            }

            removeColumnsFromDataPlacement( adapterId, oldTable.id, Arrays.asList( columnId ) );
            columnPlacements.remove( new Object[]{ adapterId, columnId } );
        }
        listeners.firePropertyChange( "columnPlacement", oldTable, null );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogColumnPlacement getColumnPlacement( int adapterId, long columnId ) {
        try {
            return Objects.requireNonNull( columnPlacements.get( new Object[]{ adapterId, columnId } ) );
        } catch ( NullPointerException e ) {
            getAdapter( adapterId );
            getColumn( columnId );
            throw new UnknownColumnPlacementRuntimeException( adapterId, columnId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkIfExistsColumnPlacement( int adapterId, long columnId ) {
        CatalogColumnPlacement placement = columnPlacements.get( new Object[]{ adapterId, columnId } );
        return placement != null;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapter( int adapterId ) {
        return new ArrayList<>( columnPlacements.prefixSubMap( new Object[]{ adapterId } ).values() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapterPerTable( int adapterId, long tableId ) {
        final Comparator<CatalogColumnPlacement> columnPlacementComparator = Comparator.comparingInt( p -> getColumn( p.columnId ).position );
        return getColumnPlacementsOnAdapter( adapterId )
                .stream()
                .filter( p -> p.tableId == tableId )
                .sorted( columnPlacementComparator )
                .collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsByColumn( long columnId ) {
        return columnPlacements.values()
                .stream()
                .filter( p -> p.columnId == columnId )
                .collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public ImmutableMap<Integer, ImmutableList<Long>> getColumnPlacementsByAdapter( long tableId ) {
        LogicalTable table = getTable( tableId );
        Map<Integer, ImmutableList<Long>> columnPlacementsByAdapter = new HashMap<>();

        table.dataPlacements.forEach( adapterId -> columnPlacementsByAdapter.put(
                        adapterId,
                        ImmutableList.copyOf(
                                getDataPlacement( adapterId, tableId ).columnPlacementsOnAdapter )
                )
        );

        return ImmutableMap.copyOf( columnPlacementsByAdapter );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<PhysicalEntity> getPhysicalsOnAdapter( long tableId ) {
        LogicalTable table = getTable( tableId );
        Map<Integer, ImmutableList<Long>> partitionPlacementsByAdapter = new HashMap<>();

        table.dataPlacements.forEach( adapterId -> partitionPlacementsByAdapter.put(
                        adapterId,
                        ImmutableList.copyOf(
                                getDataPlacement( adapterId, tableId ).getAllPartitionIds() )
                )
        );

        return ImmutableMap.copyOf( partitionPlacementsByAdapter );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long getPartitionGroupByPartition( long partitionId ) {
        return getPartition( partitionId ).partitionGroupId;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogColumnPlacement> getColumnPlacement( long columnId ) {
        return columnPlacements.values()
                .stream()
                .filter( p -> p.columnId == columnId )
                .collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapterAndSchema( int adapterId, long schemaId ) {
        try {
            return getColumnPlacementsOnAdapter( adapterId ).stream().filter( p -> Objects.requireNonNull( columns.get( p.columnId ) ).schemaId == schemaId ).collect( Collectors.toList() );
        } catch ( NullPointerException e ) {
            getAdapter( adapterId );
            getNamespace( schemaId );
            return new ArrayList<>();
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateColumnPlacementType( int adapterId, long columnId, PlacementType placementType ) {
        try {
            CatalogColumnPlacement old = Objects.requireNonNull( columnPlacements.get( new Object[]{ adapterId, columnId } ) );
            CatalogColumnPlacement placement = new CatalogColumnPlacement(
                    old.namespaceId,
                    old.tableId,
                    old.columnId,
                    old.adapterId,
                    old.adapterUniqueName,
                    placementType,
                    old.physicalSchemaName,
                    old.physicalColumnName,
                    old.physicalPosition );
            synchronized ( this ) {
                columnPlacements.replace( new Object[]{ adapterId, columnId }, placement );
            }
            listeners.firePropertyChange( "columnPlacement", old, placement );
        } catch ( NullPointerException e ) {
            getAdapter( adapterId );
            getColumn( columnId );
            throw new UnknownColumnPlacementRuntimeException( adapterId, columnId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateColumnPlacementPhysicalPosition( int adapterId, long columnId, long position ) {
        try {
            CatalogColumnPlacement old = Objects.requireNonNull( columnPlacements.get( new Object[]{ adapterId, columnId } ) );
            CatalogColumnPlacement placement = new CatalogColumnPlacement(
                    old.namespaceId,
                    old.tableId,
                    old.columnId,
                    old.adapterId,
                    old.adapterUniqueName,
                    old.placementType,
                    old.physicalSchemaName,
                    old.physicalColumnName,
                    position );
            synchronized ( this ) {
                columnPlacements.replace( new Object[]{ adapterId, columnId }, placement );
            }
            listeners.firePropertyChange( "columnPlacement", old, placement );
        } catch ( NullPointerException e ) {
            getAdapter( adapterId );
            getColumn( columnId );
            throw new UnknownColumnPlacementRuntimeException( adapterId, columnId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateColumnPlacementPhysicalPosition( int adapterId, long columnId ) {
        try {
            CatalogColumnPlacement old = Objects.requireNonNull( columnPlacements.get( new Object[]{ adapterId, columnId } ) );
            CatalogColumnPlacement placement = new CatalogColumnPlacement(
                    old.namespaceId,
                    old.tableId,
                    old.columnId,
                    old.adapterId,
                    old.adapterUniqueName,
                    old.placementType,
                    old.physicalSchemaName,
                    old.physicalColumnName,
                    physicalPositionBuilder.getAndIncrement() );
            synchronized ( this ) {
                columnPlacements.replace( new Object[]{ adapterId, columnId }, placement );
            }
            listeners.firePropertyChange( "columnPlacement", old, placement );
        } catch ( NullPointerException e ) {
            getAdapter( adapterId );
            getColumn( columnId );
            throw new UnknownColumnPlacementRuntimeException( adapterId, columnId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateColumnPlacementPhysicalNames( int adapterId, long columnId, String physicalSchemaName, String physicalColumnName, boolean updatePhysicalColumnPosition ) {
        try {
            CatalogColumnPlacement old = Objects.requireNonNull( columnPlacements.get( new Object[]{ adapterId, columnId } ) );
            CatalogColumnPlacement placement = new CatalogColumnPlacement(
                    old.namespaceId,
                    old.tableId,
                    old.columnId,
                    old.adapterId,
                    old.adapterUniqueName,
                    old.placementType,
                    physicalSchemaName,
                    physicalColumnName,
                    updatePhysicalColumnPosition ? physicalPositionBuilder.getAndIncrement() : old.physicalPosition );
            synchronized ( this ) {
                columnPlacements.replace( new Object[]{ adapterId, columnId }, placement );
            }
            listeners.firePropertyChange( "columnPlacement", old, placement );
        } catch ( NullPointerException e ) {
            getAdapter( adapterId );
            getColumn( columnId );
            throw new UnknownColumnPlacementRuntimeException( adapterId, columnId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<LogicalColumn> getColumns( long tableId ) {
        try {
            LogicalTable table = Objects.requireNonNull( tables.get( tableId ) );
            return columnNames.prefixSubMap( new Object[]{ table.namespaceId, table.id } ).values().stream().sorted( columnComparator ).collect( Collectors.toList() );
        } catch ( NullPointerException e ) {
            return new ArrayList<>();
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<LogicalColumn> getColumns( Pattern schemaNamePattern, Pattern tableNamePattern, Pattern columnNamePattern ) {
        List<LogicalTable> catalogEntities = getTables( schemaNamePattern, tableNamePattern );

        if ( catalogEntities.size() > 0 ) {
            Stream<LogicalColumn> catalogColumns = catalogEntities.stream().filter( t -> tableChildren.containsKey( t.id ) ).flatMap( t -> Objects.requireNonNull( tableChildren.get( t.id ) ).stream() ).map( columns::get );

            if ( columnNamePattern != null ) {
                catalogColumns = catalogColumns.filter( c -> c.name.matches( columnNamePattern.toRegex() ) );
            }
            return catalogColumns.collect( Collectors.toList() );
        }

        return new ArrayList<>();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public LogicalColumn getColumn( long columnId ) {
        try {
            return Objects.requireNonNull( columns.get( columnId ) );
        } catch ( NullPointerException e ) {
            throw new UnknownColumnIdRuntimeException( columnId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public LogicalColumn getColumn( long tableId, String columnName ) throws UnknownColumnException {
        try {
            LogicalTable table = getTable( tableId );
            if ( !getNamespace( table.namespaceId ).caseSensitive ) {
                columnName = columnName.toLowerCase();
            }
            return Objects.requireNonNull( columnNames.get( new Object[]{ table.namespaceId, table.id, columnName } ) );
        } catch ( NullPointerException e ) {
            throw new UnknownColumnException( tableId, columnName );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public LogicalColumn getColumn( String schemaName, String tableName, String columnName ) throws UnknownColumnException, UnknownSchemaException, UnknownTableException {
        try {
            LogicalTable table = getTable( schemaName, tableName );
            return Objects.requireNonNull( columnNames.get( new Object[]{ table.namespaceId, table.id, columnName } ) );
        } catch ( NullPointerException e ) {
            throw new UnknownColumnException( schemaName, tableName, columnName );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addColumn( String name, long tableId, int position, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality, boolean nullable, Collation collation ) {
        LogicalTable table = getTable( tableId );

        if ( !getNamespace( table.namespaceId ).caseSensitive ) {
            name = name.toLowerCase();
        }

        if ( type.getFamily() == PolyTypeFamily.CHARACTER && collation == null ) {
            throw new RuntimeException( "Collation is not allowed to be null for char types." );
        }
        if ( scale != null && length != null ) {
            if ( scale > length ) {
                throw new RuntimeException( "Invalid scale! Scale can not be larger than length." );
            }
        }

        long id = columnIdBuilder.getAndIncrement();
        LogicalColumn column = new LogicalColumn(
                id,
                name,
                tableId,
                table.namespaceId,
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
            columnNames.put( new Object[]{ table.namespaceId, table.id, name }, column );
            List<Long> children = new ArrayList<>( Objects.requireNonNull( tableChildren.get( tableId ) ) );
            children.add( id );
            tableChildren.replace( tableId, ImmutableList.copyOf( children ) );

            List<Long> columnIds = new ArrayList<>( table.fieldIds );
            columnIds.add( id );

            LogicalTable updatedTable;

            updatedTable = table.withConnectedViews( ImmutableList.copyOf( columnIds ) );
            tables.replace( tableId, updatedTable );
            tableNames.replace( new Object[]{ updatedTable.namespaceId, updatedTable.name }, updatedTable );
        }
        listeners.firePropertyChange( "column", null, column );
        return id;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void renameColumn( long columnId, String name ) {
        LogicalColumn old = getColumn( columnId );

        if ( !getNamespace( old.schemaId ).caseSensitive ) {
            name = name.toLowerCase();
        }

        LogicalColumn column = new LogicalColumn( old.id, name, old.tableId, old.schemaId, old.position, old.type, old.collectionsType, old.length, old.scale, old.dimension, old.cardinality, old.nullable, old.collation, old.defaultValue );
        synchronized ( this ) {
            columns.replace( columnId, column );
            columnNames.remove( new Object[]{ column.schemaId, column.tableId, old.name } );
            columnNames.put( new Object[]{ column.schemaId, column.tableId, name }, column );
        }
        listeners.firePropertyChange( "column", old, column );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setColumnPosition( long columnId, int position ) {
        LogicalColumn old = getColumn( columnId );
        LogicalColumn column = new LogicalColumn( old.id, old.name, old.tableId, old.schemaId, position, old.type, old.collectionsType, old.length, old.scale, old.dimension, old.cardinality, old.nullable, old.collation, old.defaultValue );
        synchronized ( this ) {
            columns.replace( columnId, column );
            columnNames.replace( new Object[]{ column.schemaId, column.tableId, column.name }, column );
        }
        listeners.firePropertyChange( "column", old, column );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setColumnType( long columnId, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality ) throws GenericCatalogException {
        try {
            LogicalColumn old = Objects.requireNonNull( columns.get( columnId ) );

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
            LogicalColumn column = new LogicalColumn( old.id, old.name, old.tableId, old.schemaId, old.position, type, collectionsType, length, scale, dimension, cardinality, old.nullable, collation, old.defaultValue );
            synchronized ( this ) {
                columns.replace( columnId, column );
                columnNames.replace( new Object[]{ old.schemaId, old.tableId, old.name }, column );
            }
            listeners.firePropertyChange( "column", old, column );
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setNullable( long columnId, boolean nullable ) throws GenericCatalogException {
        try {
            LogicalColumn old = Objects.requireNonNull( columns.get( columnId ) );
            if ( nullable ) {
                // Check if the column is part of a primary key (pk's are not allowed to contain null values)
                LogicalTable table = Objects.requireNonNull( tables.get( old.tableId ) );
                if ( table.primaryKey != null ) {
                    CatalogKey catalogKey = getPrimaryKey( table.primaryKey );
                    if ( catalogKey.columnIds.contains( columnId ) ) {
                        throw new GenericCatalogException( "Unable to allow null values in a column that is part of the primary key." );
                    }
                }
            } else {
                // TODO: Check that the column does not contain any null values
                getColumnPlacement( columnId );
            }
            LogicalColumn column = new LogicalColumn(
                    old.id,
                    old.name,
                    old.tableId,
                    old.schemaId,
                    old.position,
                    old.type,
                    old.collectionsType,
                    old.length,
                    old.scale,
                    old.dimension,
                    old.cardinality,
                    nullable,
                    old.collation,
                    old.defaultValue );
            synchronized ( this ) {
                columns.replace( columnId, column );
                columnNames.replace( new Object[]{ old.schemaId, old.tableId, old.name }, column );
            }
            listeners.firePropertyChange( "column", old, column );
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setCollation( long columnId, Collation collation ) {
        LogicalColumn old = getColumn( columnId );

        if ( old.type.getFamily() != PolyTypeFamily.CHARACTER ) {
            throw new RuntimeException( "Illegal attempt to set collation for a non-char column!" );
        }
        LogicalColumn column = new LogicalColumn(
                old.id,
                old.name,
                old.tableId,
                old.schemaId,
                old.position,
                old.type,
                old.collectionsType,
                old.length,
                old.scale,
                old.dimension,
                old.cardinality,
                old.nullable,
                collation,
                old.defaultValue );
        synchronized ( this ) {
            columns.replace( columnId, column );
            columnNames.replace( new Object[]{ old.schemaId, old.tableId, old.name }, column );
        }
        listeners.firePropertyChange( "column", old, column );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkIfExistsColumn( long tableId, String columnName ) {
        LogicalTable table = getTable( tableId );
        return columnNames.containsKey( new Object[]{ table.namespaceId, tableId, columnName } );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteColumn( long columnId ) {
        //TODO also delete keys with that column?
        LogicalColumn column = getColumn( columnId );

        List<Long> children = new ArrayList<>( Objects.requireNonNull( tableChildren.get( column.tableId ) ) );
        children.remove( columnId );

        LogicalTable old = getTable( column.tableId );
        List<Long> columnIds = new ArrayList<>( old.fieldIds );
        columnIds.remove( columnId );

        LogicalTable table = old.withFieldIds( ImmutableList.copyOf( columnIds ) );

        synchronized ( this ) {
            columnNames.remove( new Object[]{ column.schemaId, column.tableId, column.name } );
            tableChildren.replace( column.tableId, ImmutableList.copyOf( children ) );

            deleteDefaultValue( columnId );
            for ( CatalogColumnPlacement p : getColumnPlacement( columnId ) ) {
                deleteColumnPlacement( p.adapterId, p.columnId, false );
            }
            tables.replace( column.tableId, table );
            tableNames.replace( new Object[]{ table.namespaceId, table.name }, table );

            columns.remove( columnId );
        }
        listeners.firePropertyChange( "column", column, null );
    }


    /**
     * {@inheritDoc}
     *
     * TODO: String is only a temporary solution
     */
    @Override
    public void setDefaultValue( long columnId, PolyType type, String defaultValue ) {
        LogicalColumn old = getColumn( columnId );
        LogicalColumn column = new LogicalColumn(
                old.id,
                old.name,
                old.tableId,
                old.schemaId,
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
            columnNames.replace( new Object[]{ column.schemaId, column.tableId, column.name }, column );
        }
        listeners.firePropertyChange( "column", old, column );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteDefaultValue( long columnId ) {
        LogicalColumn old = getColumn( columnId );
        LogicalColumn column = new LogicalColumn(
                old.id,
                old.name,
                old.tableId,
                old.schemaId,
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
        if ( old.defaultValue != null ) {
            synchronized ( this ) {
                columns.replace( columnId, column );
                columnNames.replace( new Object[]{ old.schemaId, old.tableId, old.name }, column );
            }
            listeners.firePropertyChange( "column", old, column );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogPrimaryKey getPrimaryKey( long key ) {
        try {
            return Objects.requireNonNull( primaryKeys.get( key ) );
        } catch ( NullPointerException e ) {
            throw new UnknownKeyIdRuntimeException( key );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isPrimaryKey( long key ) {
        try {
            Long primary = getTable( Objects.requireNonNull( keys.get( key ) ).tableId ).primaryKey;
            return primary != null && primary == key;
        } catch ( NullPointerException e ) {
            throw new UnknownKeyIdRuntimeException( key );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void addPrimaryKey( long tableId, List<Long> columnIds ) throws GenericCatalogException {
        try {
            // Check if the columns are set 'not null'
            List<LogicalColumn> nullableColumns = columnIds.stream().map( columns::get ).filter( Objects::nonNull ).filter( c -> c.nullable ).collect( Collectors.toList() );
            for ( LogicalColumn col : nullableColumns ) {
                throw new GenericCatalogException( "Primary key is not allowed to contain null values but the column '" + col.name + "' is declared nullable." );
            }

            // TODO: Check if the current values are unique

            // Check if there is already a primary key defined for this table and if so, delete it.
            LogicalTable table = getTable( tableId );

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
            long keyId = getOrAddKey( tableId, columnIds, EnforcementTime.ON_QUERY );
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
     * {@inheritDoc}
     */
    @Override
    public List<CatalogForeignKey> getForeignKeys( long tableId ) {
        return foreignKeys.values().stream().filter( f -> f.tableId == tableId ).collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogForeignKey> getExportedKeys( long tableId ) {
        return foreignKeys.values().stream().filter( k -> k.referencedKeyTableId == tableId ).collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogConstraint> getConstraints( long tableId ) {
        List<Long> keysOfTable = keys.values().stream().filter( k -> k.tableId == tableId ).map( k -> k.id ).collect( Collectors.toList() );
        return constraints.values().stream().filter( c -> keysOfTable.contains( c.keyId ) ).collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogConstraint getConstraint( long tableId, String constraintName ) throws UnknownConstraintException {
        try {
            return constraints.values().stream()
                    .filter( c -> c.key.tableId == tableId && c.name.equals( constraintName ) )
                    .findFirst()
                    .orElseThrow( NullPointerException::new );
        } catch ( NullPointerException e ) {
            throw new UnknownConstraintException( tableId, constraintName );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogForeignKey getForeignKey( long tableId, String foreignKeyName ) throws UnknownForeignKeyException {
        try {
            return foreignKeys.values().stream()
                    .filter( f -> f.tableId == tableId && f.name.equals( foreignKeyName ) )
                    .findFirst()
                    .orElseThrow( NullPointerException::new );
        } catch ( NullPointerException e ) {
            throw new UnknownForeignKeyException( tableId, foreignKeyName );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void addForeignKey( long tableId, List<Long> columnIds, long referencesTableId, List<Long> referencesIds, String constraintName, ForeignKeyOption onUpdate, ForeignKeyOption onDelete ) throws GenericCatalogException {
        try {
            LogicalTable table = Objects.requireNonNull( tables.get( tableId ) );
            List<CatalogKey> childKeys = keys.values().stream().filter( k -> k.tableId == referencesTableId ).collect( Collectors.toList() );

            for ( CatalogKey refKey : childKeys ) {
                if ( refKey.columnIds.size() == referencesIds.size() && refKey.columnIds.containsAll( referencesIds ) && referencesIds.containsAll( refKey.columnIds ) ) {

                    // CatalogKey combinedKey = getCombinedKey( refKey.id );

                    int i = 0;
                    for ( long referencedColumnId : refKey.columnIds ) {
                        LogicalColumn referencingColumn = getColumn( columnIds.get( i++ ) );
                        LogicalColumn referencedColumn = getColumn( referencedColumnId );
                        if ( referencedColumn.type != referencingColumn.type ) {
                            throw new GenericCatalogException( "The data type of the referenced columns does not match the data type of the referencing column: " + referencingColumn.type.name() + " != " + referencedColumn.type );
                        }
                    }
                    // TODO same keys for key and foreign key
                    if ( getKeyUniqueCount( refKey.id ) > 0 ) {
                        long keyId = getOrAddKey( tableId, columnIds, EnforcementTime.ON_COMMIT );
                        CatalogForeignKey key = new CatalogForeignKey(
                                keyId,
                                constraintName,
                                tableId,
                                table.namespaceId,
                                refKey.id,
                                refKey.tableId,
                                refKey.schemaId,
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
            throw new GenericCatalogException( "There is no key over the referenced columns." );
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void addUniqueConstraint( long tableId, String constraintName, List<Long> columnIds ) throws GenericCatalogException {
        try {
            long keyId = getOrAddKey( tableId, columnIds, EnforcementTime.ON_QUERY );
            // Check if there is already a unique constraint
            List<CatalogConstraint> catalogConstraints = constraints.values().stream()
                    .filter( c -> c.keyId == keyId && c.type == ConstraintType.UNIQUE )
                    .collect( Collectors.toList() );
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
     * {@inheritDoc}
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
     * {@inheritDoc}
     */
    @Override
    public CatalogIndex getIndex( long tableId, String indexName ) throws UnknownIndexException {
        try {
            return indexes.values().stream()
                    .filter( i -> i.key.tableId == tableId && i.name.equals( indexName ) )
                    .findFirst()
                    .orElseThrow( NullPointerException::new );
        } catch ( NullPointerException e ) {
            throw new UnknownIndexException( tableId, indexName );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkIfExistsIndex( long tableId, String indexName ) {
        try {
            LogicalTable table = getTable( tableId );
            getIndex( table.id, indexName );
            return true;
        } catch ( UnknownIndexException e ) {
            return false;
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogIndex getIndex( long indexId ) {
        try {
            return Objects.requireNonNull( indexes.get( indexId ) );
        } catch ( NullPointerException e ) {
            throw new UnknownIndexIdRuntimeException( indexId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogIndex> getIndexes() {
        return new ArrayList<>( indexes.values() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addIndex( long tableId, List<Long> columnIds, boolean unique, String method, String methodDisplayName, int location, IndexType type, String indexName ) throws GenericCatalogException {
        long keyId = getOrAddKey( tableId, columnIds, EnforcementTime.ON_QUERY );
        if ( unique ) {
            // TODO: Check if the current values are unique
        }
        long id = indexIdBuilder.getAndIncrement();
        synchronized ( this ) {
            indexes.put( id, new CatalogIndex(
                    id,
                    indexName,
                    unique,
                    method,
                    methodDisplayName,
                    type,
                    location,
                    keyId,
                    Objects.requireNonNull( keys.get( keyId ) ),
                    null ) );
        }
        listeners.firePropertyChange( "index", null, keyId );
        return id;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setIndexPhysicalName( long indexId, String physicalName ) {
        try {
            CatalogIndex oldEntry = Objects.requireNonNull( indexes.get( indexId ) );
            CatalogIndex newEntry = new CatalogIndex(
                    oldEntry.id,
                    oldEntry.name,
                    oldEntry.unique,
                    oldEntry.method,
                    oldEntry.methodDisplayName,
                    oldEntry.type,
                    oldEntry.location,
                    oldEntry.keyId,
                    oldEntry.key,
                    physicalName );
            synchronized ( this ) {
                indexes.replace( indexId, newEntry );
            }
            listeners.firePropertyChange( "index", oldEntry, newEntry );
        } catch ( NullPointerException e ) {
            throw new UnknownIndexIdRuntimeException( indexId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteIndex( long indexId ) {
        CatalogIndex index = getIndex( indexId );
        if ( index.unique ) {
            if ( getKeyUniqueCount( index.keyId ) == 1 && isForeignKey( index.keyId ) ) {
                // This unique index is the only constraint for the uniqueness of this key.
                //throw new GenericCatalogException( "This key is referenced by at least one foreign key which requires this key to be unique. To delete this index, first add a unique constraint." );
            }
        }
        synchronized ( this ) {
            indexes.remove( indexId );
        }
        listeners.firePropertyChange( "index", index.key, null );
        deleteKeyIfNoLongerUsed( index.keyId );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deletePrimaryKey( long tableId ) throws GenericCatalogException {
        LogicalTable table = getTable( tableId );

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
    }


    /**
     * {@inheritDoc}
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
     * {@inheritDoc}
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
     * {@inheritDoc}
     */
    @Override
    public CatalogUser getUser( String name ) throws UnknownUserException {
        try {
            return Objects.requireNonNull( userNames.get( name ) );
        } catch ( NullPointerException e ) {
            throw new UnknownUserException( name );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogUser getUser( long id ) {
        try {
            return Objects.requireNonNull( users.get( id ) );
        } catch ( NullPointerException e ) {
            throw new UnknownUserIdRuntimeException( id );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogAdapter> getAdapters() {
        return new ArrayList<>( adapters.values() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogAdapter getAdapter( String uniqueName ) throws UnknownAdapterException {
        uniqueName = uniqueName.toLowerCase();
        try {
            return Objects.requireNonNull( adapterNames.get( uniqueName ) );
        } catch ( NullPointerException e ) {
            throw new UnknownAdapterException( uniqueName );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogAdapter getAdapter( long id ) {
        try {
            return Objects.requireNonNull( adapters.get( id ) );
        } catch ( NullPointerException e ) {
            throw new UnknownAdapterIdRuntimeException( id );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkIfExistsAdapter( long id ) {
        return adapters.containsKey( id );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addAdapter( String uniqueName, String adapterName, AdapterType type, Map<String, String> settings ) {
        uniqueName = uniqueName.toLowerCase();

        int id = adapterIdBuilder.getAndIncrement();
        Map<String, String> temp = new HashMap<>( settings );
        CatalogAdapter adapter = new CatalogAdapter( id, uniqueName, adapterName, type, temp );
        synchronized ( this ) {
            adapters.put( id, adapter );
            adapterNames.put( uniqueName, adapter );
        }
        try {
            commit();
        } catch ( NoTablePrimaryKeyException e ) {
            throw new RuntimeException( "An error occurred while creating the adapter." );
        }
        listeners.firePropertyChange( "adapter", null, adapter );
        return id;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateAdapterSettings( long adapterId, Map<String, String> newSettings ) {
        CatalogAdapter old = getAdapter( adapterId );
        Map<String, String> temp = new HashMap<>();
        newSettings.forEach( temp::put );
        CatalogAdapter adapter = new CatalogAdapter( old.id, old.uniqueName, old.adapterName, old.type, temp );
        synchronized ( this ) {
            adapters.put( adapter.id, adapter );
            adapterNames.put( adapter.uniqueName, adapter );
        }
        listeners.firePropertyChange( "adapter", old, adapter );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAdapter( long id ) {
        try {
            CatalogAdapter adapter = Objects.requireNonNull( adapters.get( id ) );
            synchronized ( this ) {
                adapters.remove( id );
                adapterNames.remove( adapter.uniqueName );
            }
            try {
                commit();
            } catch ( NoTablePrimaryKeyException e ) {
                throw new RuntimeException( "An error occurred while deleting the adapter." );
            }
            try {
                commit();
            } catch ( NoTablePrimaryKeyException e ) {
                throw new RuntimeException( "Could not delete adapter" );
            }
            listeners.firePropertyChange( "adapter", adapter, null );
        } catch ( NullPointerException e ) {
            throw new UnknownAdapterIdRuntimeException( id );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogQueryInterface> getQueryInterfaces() {
        return new ArrayList<>( queryInterfaces.values() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogQueryInterface getQueryInterface( String uniqueName ) throws UnknownQueryInterfaceException {
        uniqueName = uniqueName.toLowerCase();
        try {
            return Objects.requireNonNull( queryInterfaceNames.get( uniqueName ) );
        } catch ( NullPointerException e ) {
            throw new UnknownQueryInterfaceException( uniqueName );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogQueryInterface getQueryInterface( long id ) {
        try {
            return Objects.requireNonNull( queryInterfaces.get( id ) );
        } catch ( NullPointerException e ) {
            throw new UnknownQueryInterfaceRuntimeException( id );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addQueryInterface( String uniqueName, String clazz, Map<String, String> settings ) {
        uniqueName = uniqueName.toLowerCase();

        int id = queryInterfaceIdBuilder.getAndIncrement();
        Map<String, String> temp = new HashMap<>( settings );
        CatalogQueryInterface queryInterface = new CatalogQueryInterface( id, uniqueName, clazz, temp );
        synchronized ( this ) {
            queryInterfaces.put( id, queryInterface );
            queryInterfaceNames.put( uniqueName, queryInterface );
        }
        try {
            commit();
        } catch ( NoTablePrimaryKeyException e ) {
            throw new RuntimeException( "An error occurred while creating the query interface." );
        }
        listeners.firePropertyChange( "queryInterface", null, queryInterface );
        return id;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteQueryInterface( long id ) {
        try {
            CatalogQueryInterface queryInterface = Objects.requireNonNull( queryInterfaces.get( id ) );
            synchronized ( this ) {
                queryInterfaces.remove( id );
                queryInterfaceNames.remove( queryInterface.name );
            }
            try {
                commit();
            } catch ( NoTablePrimaryKeyException e ) {
                throw new RuntimeException( "An error occurred while deleting the query interface." );
            }
            listeners.firePropertyChange( "queryInterface", queryInterface, null );
        } catch ( NullPointerException e ) {
            throw new UnknownQueryInterfaceRuntimeException( id );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addPartitionGroup( long tableId, String partitionGroupName, long schemaId, PartitionType partitionType, long numberOfInternalPartitions, List<String> effectivePartitionGroupQualifier, boolean isUnbound ) throws GenericCatalogException {
        try {
            long id = partitionGroupIdBuilder.getAndIncrement();
            if ( log.isDebugEnabled() ) {
                log.debug( "Creating partitionGroup of type '{}' with id '{}'", partitionType, id );
            }
            LogicalNamespace schema = Objects.requireNonNull( schemas.get( schemaId ) );

            List<Long> partitionIds = new ArrayList<>();
            for ( int i = 0; i < numberOfInternalPartitions; i++ ) {
                long partId = addPartition( tableId, schemaId, id, effectivePartitionGroupQualifier, isUnbound );
                partitionIds.add( partId );
            }

            CatalogPartitionGroup partitionGroup = new CatalogPartitionGroup(
                    id,
                    partitionGroupName,
                    tableId,
                    schemaId,
                    0,
                    null,
                    ImmutableList.copyOf( partitionIds ),
                    isUnbound );

            synchronized ( this ) {
                partitionGroups.put( id, partitionGroup );
            }
            //listeners.firePropertyChange( "partitionGroups", null, partitionGroup );
            return id;
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deletePartitionGroup( long tableId, long schemaId, long partitionGroupId ) throws UnknownPartitionGroupIdRuntimeException {
        if ( log.isDebugEnabled() ) {
            log.debug( "Deleting partitionGroup with id '{}' on table with id '{}'", partitionGroupId, tableId );
        }
        // Check whether there this partition id exists
        CatalogPartitionGroup partitionGroup = getPartitionGroup( partitionGroupId );
        synchronized ( this ) {
            for ( long partitionId : partitionGroup.partitionIds ) {
                deletePartition( tableId, schemaId, partitionId );
            }
            partitionGroups.remove( partitionGroupId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updatePartitionGroup( long partitionGroupId, List<Long> partitionIds ) throws UnknownPartitionGroupIdRuntimeException {

        // Check whether there this partition id exists
        CatalogPartitionGroup partitionGroup = getPartitionGroup( partitionGroupId );

        CatalogPartitionGroup updatedCatalogPartitionGroup = new CatalogPartitionGroup(
                partitionGroup.id,
                partitionGroup.partitionGroupName,
                partitionGroup.tableId,
                partitionGroup.schemaId,
                partitionGroup.partitionKey,
                partitionGroup.partitionQualifiers,
                ImmutableList.copyOf( partitionIds ),
                partitionGroup.isUnbound );

        synchronized ( this ) {
            partitionGroups.replace( partitionGroupId, updatedCatalogPartitionGroup );
            listeners.firePropertyChange( "partitionGroup", partitionGroup, updatedCatalogPartitionGroup );
        }

    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void addPartitionToGroup( long partitionGroupId, Long partitionId ) {
        // Check whether there this partition id exists
        getPartition( partitionId );

        CatalogPartitionGroup partitionGroup = getPartitionGroup( partitionGroupId );
        List<Long> newPartitionIds = new ArrayList<>( partitionGroup.partitionIds );

        if ( !newPartitionIds.contains( partitionId ) ) {
            newPartitionIds.add( partitionId );
            updatePartitionGroup( partitionGroupId, newPartitionIds );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void removePartitionFromGroup( long partitionGroupId, Long partitionId ) {
        // Check whether there this partition id exists
        CatalogPartitionGroup partitionGroup = getPartitionGroup( partitionGroupId );
        List<Long> newPartitionIds = new ArrayList<>( partitionGroup.partitionIds );

        if ( newPartitionIds.contains( partitionId ) ) {
            newPartitionIds.remove( partitionId );
            updatePartitionGroup( partitionGroupId, newPartitionIds );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updatePartition( long partitionId, Long partitionGroupId ) {
        // Check whether there this partition id exists
        CatalogPartitionGroup partitionGroup = getPartitionGroup( partitionGroupId );
        List<Long> newPartitionIds = new ArrayList<>( partitionGroup.partitionIds );

        CatalogPartition oldPartition = getPartition( partitionId );

        if ( !newPartitionIds.contains( partitionId ) ) {
            newPartitionIds.add( partitionId );

            addPartitionToGroup( partitionGroupId, partitionId );
            removePartitionFromGroup( oldPartition.partitionGroupId, partitionId );

            CatalogPartition updatedPartition = new CatalogPartition(
                    oldPartition.id,
                    oldPartition.tableId,
                    oldPartition.schemaId,
                    oldPartition.partitionQualifiers,
                    oldPartition.isUnbound,
                    partitionGroupId
            );

            synchronized ( this ) {
                partitions.put( updatedPartition.id, updatedPartition );
            }
            listeners.firePropertyChange( "partition", oldPartition, updatedPartition );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogPartitionGroup getPartitionGroup( long partitionGroupId ) throws UnknownPartitionGroupIdRuntimeException {
        try {
            return Objects.requireNonNull( partitionGroups.get( partitionGroupId ) );
        } catch ( NullPointerException e ) {
            throw new UnknownPartitionGroupIdRuntimeException( partitionGroupId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addPartition( long tableId, long schemaId, long partitionGroupId, List<String> effectivePartitionQualifier, boolean isUnbound ) throws GenericCatalogException {
        try {
            long id = partitionIdBuilder.getAndIncrement();
            if ( log.isDebugEnabled() ) {
                log.debug( "Creating partition with id '{}'", id );
            }
            LogicalNamespace schema = Objects.requireNonNull( schemas.get( schemaId ) );

            CatalogPartition partition = new CatalogPartition(
                    id,
                    tableId,
                    schemaId,
                    effectivePartitionQualifier,
                    isUnbound,
                    partitionGroupId );

            synchronized ( this ) {
                partitions.put( id, partition );
            }
            listeners.firePropertyChange( "partition", null, partition );
            return id;
        } catch ( NullPointerException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deletePartition( long tableId, long schemaId, long partitionId ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "Deleting partition with id '{}' on table with id '{}'", partitionId, tableId );
        }
        // Check whether there this partition id exists
        getPartition( partitionId );
        synchronized ( this ) {
            for ( CatalogPartitionPlacement partitionPlacement : getPartitionPlacements( partitionId ) ) {
                deletePartitionPlacement( partitionPlacement.adapterId, partitionId );
            }
            partitions.remove( partitionId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogPartition getPartition( long partitionId ) {
        try {
            return Objects.requireNonNull( partitions.get( partitionId ) );
        } catch ( NullPointerException e ) {
            throw new UnknownPartitionGroupIdRuntimeException( partitionId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogPartition> getPartitionsByTable( long tableId ) {
        return partitions.values()
                .stream()
                .filter( p -> p.tableId == tableId )
                .collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void partitionTable( long tableId, PartitionType partitionType, long partitionColumnId, int numPartitionGroups, List<Long> partitionGroupIds, PartitionProperty partitionProperty ) {
        LogicalTable old = Objects.requireNonNull( tables.get( tableId ) );

        LogicalTable table = new LogicalTable(
                old.id,
                old.name,
                old.fieldIds,
                old.namespaceId,
                old.ownerId,
                old.entityType,
                old.primaryKey,
                old.dataPlacements,
                old.modifiable,
                partitionProperty,
                old.connectedViews );

        synchronized ( this ) {
            tables.replace( tableId, table );
            tableNames.replace( new Object[]{ table.namespaceId, old.name }, table );

            if ( table.partitionProperty.reliesOnPeriodicChecks ) {
                addTableToPeriodicProcessing( tableId );
            }
        }

        listeners.firePropertyChange( "table", old, table );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void mergeTable( long tableId ) {
        LogicalTable old = Objects.requireNonNull( tables.get( tableId ) );

        if ( old.partitionProperty.reliesOnPeriodicChecks ) {
            removeTableFromPeriodicProcessing( tableId );
        }

        //Technically every Table is partitioned. But tables classified as UNPARTITIONED only consist of one PartitionGroup and one large partition
        List<Long> partitionGroupIds = new ArrayList<>();
        try {
            partitionGroupIds.add( addPartitionGroup( tableId, "full", old.namespaceId, PartitionType.NONE, 1, new ArrayList<>(), true ) );
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        }

        // Get All(only one) PartitionGroups and then get all partitionIds  for each PG and add them to completeList of partitionIds
        CatalogPartitionGroup defaultUnpartitionedGroup = getPartitionGroup( partitionGroupIds.get( 0 ) );
        PartitionProperty partitionProperty = PartitionProperty.builder()
                .partitionType( PartitionType.NONE )
                .isPartitioned( false )
                .partitionGroupIds( ImmutableList.copyOf( partitionGroupIds ) )
                .partitionIds( ImmutableList.copyOf( defaultUnpartitionedGroup.partitionIds ) )
                .reliesOnPeriodicChecks( false )
                .build();

        LogicalTable table = new LogicalTable(
                old.id,
                old.name,
                old.fieldIds,
                old.namespaceId,
                old.ownerId,
                old.entityType,
                old.primaryKey,
                old.dataPlacements,
                old.modifiable,
                partitionProperty,
                old.connectedViews );

        synchronized ( this ) {
            tables.replace( tableId, table );
            tableNames.replace( new Object[]{ table.namespaceId, old.name }, table );
        }
        listeners.firePropertyChange( "table", old, table );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTablePartitionProperties( long tableId, PartitionProperty partitionProperty ) {
        LogicalTable old = Objects.requireNonNull( tables.get( tableId ) );

        LogicalTable table = new LogicalTable(
                old.id,
                old.name,
                old.fieldIds,
                old.namespaceId,
                old.ownerId,
                old.entityType,
                old.primaryKey,
                old.dataPlacements,
                old.modifiable,
                partitionProperty,
                old.connectedViews );

        synchronized ( this ) {
            tables.replace( tableId, table );
            tableNames.replace( new Object[]{ table.namespaceId, old.name }, table );
        }

        listeners.firePropertyChange( "table", old, table );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogPartitionGroup> getPartitionGroups( long tableId ) {
        try {
            LogicalTable table = Objects.requireNonNull( tables.get( tableId ) );
            List<CatalogPartitionGroup> partitionGroups = new ArrayList<>();
            if ( table.partitionProperty.partitionGroupIds == null ) {
                return new ArrayList<>();
            }
            for ( long partId : table.partitionProperty.partitionGroupIds ) {
                partitionGroups.add( getPartitionGroup( partId ) );
            }
            return partitionGroups;
        } catch ( UnknownPartitionGroupIdRuntimeException e ) {
            return new ArrayList<>();
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogPartitionGroup> getPartitionGroups( Pattern schemaNamePattern, Pattern tableNamePattern ) {
        List<LogicalTable> catalogEntities = getTables( schemaNamePattern, tableNamePattern );
        Stream<CatalogPartitionGroup> partitionGroupStream = Stream.of();
        for ( LogicalTable catalogTable : catalogEntities ) {
            partitionGroupStream = Stream.concat( partitionGroupStream, getPartitionGroups( catalogTable.id ).stream() );
        }
        return partitionGroupStream.collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogPartition> getPartitions( long partitionGroupId ) {
        try {
            CatalogPartitionGroup partitionGroup = Objects.requireNonNull( partitionGroups.get( partitionGroupId ) );
            List<CatalogPartition> partitions = new ArrayList<>();
            if ( partitionGroup.partitionIds == null ) {
                return new ArrayList<>();
            }
            for ( long partId : partitionGroup.partitionIds ) {
                partitions.add( getPartition( partId ) );
            }
            return partitions;
        } catch ( UnknownPartitionGroupIdRuntimeException e ) {
            return new ArrayList<>();
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogPartition> getPartitions( Pattern schemaNamePattern, Pattern tableNamePattern ) {
        List<CatalogPartitionGroup> catalogPartitionGroups = getPartitionGroups( schemaNamePattern, tableNamePattern );
        Stream<CatalogPartition> partitionStream = Stream.of();
        for ( CatalogPartitionGroup catalogPartitionGroup : catalogPartitionGroups ) {
            partitionStream = Stream.concat( partitionStream, getPartitions( catalogPartitionGroup.id ).stream() );
        }
        return partitionStream.collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getPartitionGroupNames( long tableId ) {
        List<String> partitionGroupNames = new ArrayList<>();
        for ( CatalogPartitionGroup catalogPartitionGroup : getPartitionGroups( tableId ) ) {
            partitionGroupNames.add( catalogPartitionGroup.partitionGroupName );
        }
        return partitionGroupNames;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsByPartitionGroup( long tableId, long partitionGroupId, long columnId ) {
        List<CatalogColumnPlacement> catalogColumnPlacements = new ArrayList<>();
        for ( CatalogColumnPlacement ccp : getColumnPlacement( columnId ) ) {
            if ( getPartitionGroupsOnDataPlacement( ccp.adapterId, tableId ).contains( partitionGroupId ) ) {
                catalogColumnPlacements.add( ccp );
            }
        }

        return catalogColumnPlacements;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogAdapter> getAdaptersByPartitionGroup( long tableId, long partitionGroupId ) {
        Set<CatalogAdapter> catalogAdapters = new HashSet<>();

        for ( CatalogDataPlacement dataPlacement : getDataPlacements( tableId ) ) {
            for ( long partitionId : dataPlacement.getAllPartitionIds() ) {
                long partitionGroup = getPartitionGroupByPartition( partitionId );
                if ( partitionGroup == partitionGroupId ) {
                    catalogAdapters.add( getAdapter( dataPlacement.adapterId ) );
                }
            }
        }

        return new ArrayList<>( catalogAdapters );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getPartitionGroupsOnDataPlacement( int adapterId, long tableId ) {
        Set<Long> partitionGroups = new HashSet<>();
        CatalogDataPlacement dataPlacement = getDataPlacement( adapterId, tableId );

        dataPlacement.getAllPartitionIds().forEach(
                partitionId -> partitionGroups.add( getPartitionGroupByPartition( partitionId )
                )
        );

        return new ArrayList<>( partitionGroups );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getPartitionsOnDataPlacement( int adapterId, long tableId ) {
        return getDataPlacement( adapterId, tableId ).getAllPartitionIds();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<Long> getPartitionGroupsIndexOnDataPlacement( int adapterId, long tableId ) {
        List<Long> partitionGroups = getPartitionGroupsOnDataPlacement( adapterId, tableId );
        if ( partitionGroups == null ) {
            return new ArrayList<>();
        }

        List<Long> partitionGroupIndexList = new ArrayList<>();
        LogicalTable catalogTable = getTable( tableId );
        for ( int index = 0; index < catalogTable.partitionProperty.partitionGroupIds.size(); index++ ) {
            if ( partitionGroups.contains( catalogTable.partitionProperty.partitionGroupIds.get( index ) ) ) {
                partitionGroupIndexList.add( (long) index );
            }
        }
        return partitionGroupIndexList;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogDataPlacement getDataPlacement( int adapterId, long tableId ) {
        return dataPlacements.get( new Object[]{ adapterId, tableId } );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogDataPlacement> getDataPlacements( long tableId ) {
        List<CatalogDataPlacement> catalogDataPlacements = new ArrayList<>();

        getTable( tableId ).dataPlacements.forEach( adapterId -> catalogDataPlacements.add( getDataPlacement( adapterId, tableId ) ) );

        return catalogDataPlacements;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogDataPlacement> getAllFullDataPlacements( long tableId ) {
        List<CatalogDataPlacement> dataPlacements = new ArrayList<>();
        List<CatalogDataPlacement> allDataPlacements = getDataPlacements( tableId );

        for ( CatalogDataPlacement dataPlacement : allDataPlacements ) {
            if ( dataPlacement.hasFullPlacement() ) {
                dataPlacements.add( dataPlacement );
            }
        }
        return dataPlacements;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogDataPlacement> getAllColumnFullDataPlacements( long tableId ) {
        List<CatalogDataPlacement> dataPlacements = new ArrayList<>();
        List<CatalogDataPlacement> allDataPlacements = getDataPlacements( tableId );

        for ( CatalogDataPlacement dataPlacement : allDataPlacements ) {
            if ( dataPlacement.hasColumnFullPlacement() ) {
                dataPlacements.add( dataPlacement );
            }
        }
        return dataPlacements;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogDataPlacement> getAllPartitionFullDataPlacements( long tableId ) {
        List<CatalogDataPlacement> dataPlacements = new ArrayList<>();
        List<CatalogDataPlacement> allDataPlacements = getDataPlacements( tableId );

        for ( CatalogDataPlacement dataPlacement : allDataPlacements ) {
            if ( dataPlacement.hasPartitionFullPlacement() ) {
                dataPlacements.add( dataPlacement );
            }
        }
        return dataPlacements;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogDataPlacement> getDataPlacementsByRole( long tableId, DataPlacementRole role ) {
        List<CatalogDataPlacement> catalogDataPlacements = new ArrayList<>();
        for ( CatalogDataPlacement dataPlacement : getDataPlacements( tableId ) ) {
            if ( dataPlacement.dataPlacementRole.equals( role ) ) {
                catalogDataPlacements.add( dataPlacement );
            }
        }
        return catalogDataPlacements;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByRole( long tableId, DataPlacementRole role ) {
        List<CatalogPartitionPlacement> partitionPlacements = new ArrayList<>();
        for ( CatalogDataPlacement dataPlacement : getDataPlacementsByRole( tableId, role ) ) {
            if ( dataPlacement.partitionPlacementsOnAdapterByRole.containsKey( role ) ) {
                dataPlacement.partitionPlacementsOnAdapterByRole.get( role )
                        .forEach(
                                partitionId -> partitionPlacements.add( getPartitionPlacement( dataPlacement.adapterId, partitionId ) )
                        );
            }
        }
        return partitionPlacements;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByIdAndRole( long tableId, long partitionId, DataPlacementRole role ) {
        List<CatalogPartitionPlacement> partitionPlacements = new ArrayList<>();
        for ( CatalogPartitionPlacement partitionPlacement : getPartitionPlacements( partitionId ) ) {
            if ( partitionPlacement.role.equals( role ) ) {
                partitionPlacements.add( partitionPlacement );
            }
        }
        return partitionPlacements;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validateDataPlacementsConstraints( long tableId, long adapterId, List<Long> columnIdsToBeRemoved, List<Long> partitionsIdsToBeRemoved ) {
        if ( (columnIdsToBeRemoved.isEmpty() && partitionsIdsToBeRemoved.isEmpty()) || isTableFlaggedForDeletion( tableId ) ) {
            log.warn( "Invoked validation with two empty lists of columns and partitions to be revoked. Is therefore always true..." );
            return true;
        }

        // TODO @HENNLO Focus on PartitionPlacements that are labeled as UPTODATE nodes. The outdated nodes do not
        //  necessarily need placement constraints

        LogicalTable table = getTable( tableId );
        List<CatalogDataPlacement> dataPlacements = getDataPlacements( tableId );

        // Checks for every column on every DataPlacement if each column is placed with all partitions
        for ( long columnId : table.fieldIds ) {
            List<Long> partitionsToBeCheckedForColumn = table.partitionProperty.partitionIds.stream().collect( Collectors.toList() );
            // Check for every column if it has every partition
            for ( CatalogDataPlacement dataPlacement : dataPlacements ) {
                // Can instantly return because we still have a full placement somewhere
                if ( dataPlacement.hasFullPlacement() && dataPlacement.adapterId != adapterId ) {
                    return true;
                }

                List<Long> effectiveColumnsOnStore = dataPlacement.columnPlacementsOnAdapter.stream().collect( Collectors.toList() );
                List<Long> effectivePartitionsOnStore = dataPlacement.getAllPartitionIds();

                // Remove columns and partitions from store to not evaluate them
                if ( dataPlacement.adapterId == adapterId ) {

                    // Skips columns that shall be removed
                    if ( columnIdsToBeRemoved.contains( columnId ) ) {
                        continue;
                    }

                    // Only process those parts that shall be present after change
                    effectiveColumnsOnStore.removeAll( columnIdsToBeRemoved );
                    effectivePartitionsOnStore.removeAll( partitionsIdsToBeRemoved );
                }

                if ( effectiveColumnsOnStore.contains( columnId ) ) {
                    partitionsToBeCheckedForColumn.removeAll( effectivePartitionsOnStore );
                } else {
                    continue;
                }

                // Found all partitions for column, continue with next column
                if ( partitionsToBeCheckedForColumn.isEmpty() ) {
                    break;
                }
            }

            if ( !partitionsToBeCheckedForColumn.isEmpty() ) {
                return false;
            }
        }

        return true;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void flagTableForDeletion( long tableId, boolean flag ) {
        if ( flag && !tablesFlaggedForDeletion.contains( tableId ) ) {
            tablesFlaggedForDeletion.add( tableId );
        } else if ( !flag && tablesFlaggedForDeletion.contains( tableId ) ) {
            tablesFlaggedForDeletion.remove( tableId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTableFlaggedForDeletion( long tableId ) {
        return tablesFlaggedForDeletion.contains( tableId );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void addPartitionPlacement( long namespaceId, int adapterId, long tableId, long partitionId, PlacementType placementType, String physicalSchemaName, String physicalTableName, DataPlacementRole role ) {
        if ( !checkIfExistsPartitionPlacement( adapterId, partitionId ) ) {
            CatalogAdapter store = Objects.requireNonNull( adapters.get( adapterId ) );
            CatalogPartitionPlacement partitionPlacement = new CatalogPartitionPlacement(
                    namespaceId,
                    tableId,
                    adapterId,
                    store.uniqueName,
                    placementType,
                    physicalSchemaName,
                    physicalTableName,
                    partitionId,
                    role );

            synchronized ( this ) {
                partitionPlacements.put( new Object[]{ adapterId, partitionId }, partitionPlacement );

                // Adds this PartitionPlacement to existing DataPlacement container
                addPartitionsToDataPlacement( adapterId, tableId, List.of( partitionId ) );

                listeners.firePropertyChange( "partitionPlacement", null, partitionPlacements );
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogDataPlacement addDataPlacementIfNotExists( int adapterId, long tableId ) {
        CatalogDataPlacement dataPlacement;
        if ( (dataPlacement = getDataPlacement( adapterId, tableId )) == null ) {
            if ( log.isDebugEnabled() ) {
                log.debug( "No DataPlacement exists on adapter '{}' for entity '{}'. Creating a new one.", getAdapter( adapterId ), getTable( tableId ) );
            }
            addDataPlacement( adapterId, tableId );
            dataPlacement = getDataPlacement( adapterId, tableId );
        }

        return dataPlacement;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDataPlacementsOnTable( long tableId, List<Integer> newDataPlacements ) {
        LogicalTable old = Objects.requireNonNull( tables.get( tableId ) );

        LogicalTable newTable = old.withDataPlacements( ImmutableList.copyOf( newDataPlacements ) );

        synchronized ( this ) {
            tables.replace( tableId, newTable );
            tableNames.replace( new Object[]{ newTable.namespaceId, newTable.name }, newTable );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void addDataPlacement( int adapterId, long tableId ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "Creating DataPlacement on adapter '{}' for entity '{}'", getAdapter( adapterId ), getTable( tableId ) );
        }

        if ( !dataPlacements.containsKey( new Object[]{ adapterId, tableId } ) ) {
            CatalogDataPlacement dataPlacement = new CatalogDataPlacement(
                    tableId,
                    adapterId,
                    PlacementType.AUTOMATIC,
                    DataPlacementRole.UPTODATE,
                    ImmutableList.of(),
                    ImmutableList.of() );

            synchronized ( this ) {
                dataPlacements.put( new Object[]{ adapterId, tableId }, dataPlacement );
                addSingleDataPlacementToTable( adapterId, tableId );
            }
            listeners.firePropertyChange( "dataPlacement", null, dataPlacement );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void modifyDataPlacement( int adapterId, long tableId, CatalogDataPlacement catalogDataPlacement ) {

        try {
            CatalogDataPlacement oldDataPlacement = getDataPlacement( adapterId, tableId );
            synchronized ( this ) {
                dataPlacements.replace( new Object[]{ adapterId, tableId }, catalogDataPlacement );
            }
            listeners.firePropertyChange( "dataPlacement", oldDataPlacement, catalogDataPlacement );
        } catch ( NullPointerException e ) {
            e.printStackTrace();
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public long addGraphPlacement( int adapterId, long graphId ) {
        long id = partitionIdBuilder.getAndIncrement();
        CatalogGraphPlacement placement = new CatalogGraphPlacement( adapterId, graphId, null, id );
        LogicalGraph old = graphs.get( graphId );
        if ( old == null ) {
            throw new UnknownGraphException( graphId );
        }

        LogicalGraph graph = old.addPlacement( adapterId );

        synchronized ( this ) {
            graphPlacements.put( new Object[]{ graph.id, adapterId }, placement );
            graphs.replace( graph.id, graph );
            graphNames.replace( new Object[]{ graph.name }, graph );
        }
        listeners.firePropertyChange( "graphPlacement", null, placement );
        return id;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteGraphPlacement( int adapterId, long graphId ) {
        if ( !graphPlacements.containsKey( new Object[]{ graphId, adapterId } ) ) {
            throw new UnknownGraphPlacementsException( graphId, adapterId );
        }
        CatalogGraphPlacement placement = Objects.requireNonNull( graphPlacements.get( new Object[]{ graphId, adapterId } ) );

        deleteGraphPlacementLogistics( placement.graphId, adapterId );

        LogicalGraph old = Objects.requireNonNull( graphs.get( placement.graphId ) );

        LogicalGraph graph = old.removePlacement( adapterId );

        synchronized ( this ) {
            graphPlacements.remove( new Object[]{ graphId, adapterId } );
            graphs.replace( graphId, graph );
            graphNames.replace( new Object[]{ Catalog.defaultDatabaseId, graph.name }, graph );
        }
        listeners.firePropertyChange( "graphPlacements", null, null );
    }


    private void deleteGraphPlacementLogistics( long graphId, int adapterId ) {
        if ( !graphMappings.containsKey( graphId ) ) {
            throw new UnknownGraphException( graphId );
        }
        CatalogGraphMapping mapping = Objects.requireNonNull( graphMappings.get( graphId ) );
        if ( !graphPlacements.containsKey( new Object[]{ graphId, adapterId } ) ) {
            throw new UnknownGraphPlacementsException( graphId, adapterId );
        }
        CatalogGraphPlacement placement = Objects.requireNonNull( graphPlacements.get( new Object[]{ graphId, adapterId } ) );

        removeSingleDataPlacementFromTable( placement.adapterId, mapping.nodesId );
        removeSingleDataPlacementFromTable( placement.adapterId, mapping.nodesPropertyId );
        removeSingleDataPlacementFromTable( placement.adapterId, mapping.edgesId );
        removeSingleDataPlacementFromTable( placement.adapterId, mapping.edgesPropertyId );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogGraphPlacement getGraphPlacement( long graphId, int adapterId ) {
        if ( !graphPlacements.containsKey( new Object[]{ graphId, adapterId } ) ) {
            throw new UnknownGraphPlacementsException( graphId, adapterId );
        }

        return graphPlacements.get( new Object[]{ graphId, adapterId } );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDataPlacement( int adapterId, long tableId ) {
        CatalogDataPlacement dataPlacement = getDataPlacement( adapterId, tableId );

        if ( log.isDebugEnabled() ) {
            log.debug( "Removing DataPlacement on adapter '{}' for entity '{}'", getAdapter( adapterId ), getTable( tableId ) );
        }

        // Make sure that all columnPlacements and partitionPlacements are correctly dropped.
        // Although, they should've been dropped earlier.

        // Recursively removing columns that exist on this placement
        for ( Long columnId : dataPlacement.columnPlacementsOnAdapter ) {
            try {
                deleteColumnPlacement( adapterId, columnId, false );
            } catch ( UnknownColumnIdRuntimeException e ) {
                log.debug( "Column has been removed before the placement" );
            }
        }

        // Recursively removing partitions that exist on this placement
        for ( Long partitionId : dataPlacement.getAllPartitionIds() ) {
            try {
                deletePartitionPlacement( adapterId, partitionId );
            } catch ( UnknownColumnIdRuntimeException e ) {
                log.debug( "Partition has been removed before the placement" );
            }
        }

        synchronized ( this ) {
            dataPlacements.remove( new Object[]{ adapterId, tableId } );
            removeSingleDataPlacementFromTable( adapterId, tableId );
        }
        listeners.firePropertyChange( "dataPlacement", dataPlacement, null );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void addSingleDataPlacementToTable( Integer adapterId, long tableId ) {
        LogicalTable old = getTable( tableId );
        List<Integer> updatedPlacements = new ArrayList<>( old.dataPlacements );

        if ( !updatedPlacements.contains( adapterId ) ) {
            updatedPlacements.add( adapterId );
            updateDataPlacementsOnTable( tableId, updatedPlacements );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void removeSingleDataPlacementFromTable( Integer adapterId, long tableId ) {
        LogicalTable old = getTable( tableId );
        List<Integer> updatedPlacements = new ArrayList<>( old.dataPlacements );

        if ( updatedPlacements.contains( adapterId ) ) {
            updatedPlacements.remove( adapterId );
            updateDataPlacementsOnTable( tableId, updatedPlacements );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void addColumnsToDataPlacement( int adapterId, long tableId, List<Long> columnIds ) {
        CatalogDataPlacement oldDataPlacement = addDataPlacementIfNotExists( adapterId, tableId );

        Set<Long> columnPlacementsOnAdapter = new HashSet<>( oldDataPlacement.columnPlacementsOnAdapter );

        // Merges new columnIds to list of already existing placements
        columnPlacementsOnAdapter.addAll( columnIds );

        CatalogDataPlacement newDataPlacement = new CatalogDataPlacement(
                oldDataPlacement.tableId,
                oldDataPlacement.adapterId,
                oldDataPlacement.placementType,
                oldDataPlacement.dataPlacementRole,
                ImmutableList.copyOf( new ArrayList<>( columnPlacementsOnAdapter ) ),
                ImmutableList.copyOf( oldDataPlacement.getAllPartitionIds() )
        );

        modifyDataPlacement( adapterId, tableId, newDataPlacement );

        if ( log.isDebugEnabled() ) {
            log.debug( "Added columns: {} of table {}, to placement on adapter {}.", columnIds, tableId, adapterId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void removeColumnsFromDataPlacement( int adapterId, long tableId, List<Long> columnIds ) {
        CatalogDataPlacement oldDataPlacement = getDataPlacement( adapterId, tableId );

        Set<Long> columnPlacementsOnAdapter = new HashSet<>( oldDataPlacement.columnPlacementsOnAdapter );
        columnPlacementsOnAdapter.removeAll( columnIds );

        CatalogDataPlacement newDataPlacement = new CatalogDataPlacement(
                oldDataPlacement.tableId,
                oldDataPlacement.adapterId,
                oldDataPlacement.placementType,
                oldDataPlacement.dataPlacementRole,
                ImmutableList.copyOf( new ArrayList<>( columnPlacementsOnAdapter ) ),
                ImmutableList.copyOf( oldDataPlacement.getAllPartitionIds() )
        );

        modifyDataPlacement( adapterId, tableId, newDataPlacement );

        if ( log.isDebugEnabled() ) {
            log.debug( "Removed columns: {} from table {}, to placement on adapter {}.", columnIds, tableId, adapterId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void addPartitionsToDataPlacement( int adapterId, long tableId, List<Long> partitionIds ) {
        CatalogDataPlacement oldDataPlacement = addDataPlacementIfNotExists( adapterId, tableId );

        Set<Long> partitionPlacementsOnAdapter = new HashSet<>( oldDataPlacement.getAllPartitionIds() );
        partitionPlacementsOnAdapter.addAll( partitionIds );

        CatalogDataPlacement newDataPlacement = new CatalogDataPlacement(
                oldDataPlacement.tableId,
                oldDataPlacement.adapterId,
                oldDataPlacement.placementType,
                oldDataPlacement.dataPlacementRole,
                oldDataPlacement.columnPlacementsOnAdapter,
                ImmutableList.copyOf( new ArrayList<>( partitionPlacementsOnAdapter ) ) );

        modifyDataPlacement( adapterId, tableId, newDataPlacement );

        if ( log.isDebugEnabled() ) {
            log.debug( "Added partitions: {} of table {}, to placement on adapter {}.", partitionIds, tableId, adapterId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void removePartitionsFromDataPlacement( int adapterId, long tableId, List<Long> partitionIds ) {
        CatalogDataPlacement oldDataPlacement = getDataPlacement( adapterId, tableId );

        Set<Long> partitionPlacementsOnAdapter = new HashSet<>( oldDataPlacement.getAllPartitionIds() );
        partitionIds.forEach( partitionPlacementsOnAdapter::remove );

        CatalogDataPlacement newDataPlacement = new CatalogDataPlacement(
                oldDataPlacement.tableId,
                oldDataPlacement.adapterId,
                oldDataPlacement.placementType,
                oldDataPlacement.dataPlacementRole,
                oldDataPlacement.columnPlacementsOnAdapter,
                ImmutableList.copyOf( new ArrayList<>( partitionPlacementsOnAdapter ) ) );

        modifyDataPlacement( adapterId, tableId, newDataPlacement );

        if ( log.isDebugEnabled() ) {
            log.debug( "Removed partitions: {} from table {}, to placement on adapter {}.", partitionIds, tableId, adapterId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDataPlacement( int adapterId, long tableId, List<Long> columnIds, List<Long> partitionIds ) {
        CatalogDataPlacement oldDataPlacement = getDataPlacement( adapterId, tableId );

        CatalogDataPlacement newDataPlacement = new CatalogDataPlacement(
                oldDataPlacement.tableId,
                oldDataPlacement.adapterId,
                oldDataPlacement.placementType,
                oldDataPlacement.dataPlacementRole,
                ImmutableList.copyOf( columnIds ),
                ImmutableList.copyOf( partitionIds ) );

        modifyDataPlacement( adapterId, tableId, newDataPlacement );

        if ( log.isDebugEnabled() ) {
            log.debug( "Added columns {} & partitions: {} of table {}, to placement on adapter {}.", columnIds, partitionIds, tableId, adapterId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void deletePartitionPlacement( int adapterId, long partitionId ) {
        if ( checkIfExistsPartitionPlacement( adapterId, partitionId ) ) {
            synchronized ( this ) {
                partitionPlacements.remove( new Object[]{ adapterId, partitionId } );
                removePartitionsFromDataPlacement( adapterId, getTableFromPartition( partitionId ).id, Arrays.asList( partitionId ) );
            }
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public CatalogPartitionPlacement getPartitionPlacement( int adapterId, long partitionId ) {
        try {
            return Objects.requireNonNull( partitionPlacements.get( new Object[]{ adapterId, partitionId } ) );
        } catch ( NullPointerException e ) {
            getAdapter( adapterId );
            getPartition( partitionId );
            throw new UnknownPartitionPlacementException( adapterId, partitionId );
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByAdapter( int adapterId ) {
        return new ArrayList<>( partitionPlacements.prefixSubMap( new Object[]{ adapterId } ).values() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByTableOnAdapter( int adapterId, long tableId ) {
        return getPartitionPlacementsByAdapter( adapterId )
                .stream()
                .filter( p -> p.tableId == tableId )
                .collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogPartitionPlacement> getAllPartitionPlacementsByTable( long tableId ) {
        return partitionPlacements.values()
                .stream()
                .filter( p -> p.tableId == tableId )
                .collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacements( long partitionId ) {
        return partitionPlacements.values()
                .stream()
                .filter( p -> p.partitionId == partitionId )
                .collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<LogicalTable> getTablesForPeriodicProcessing() {
        List<LogicalTable> procTables = new ArrayList<>();
        for ( Iterator<Long> iterator = frequencyDependentTables.iterator(); iterator.hasNext(); ) {
            long tableId = -1;
            try {
                tableId = iterator.next();
                procTables.add( getTable( tableId ) );
            } catch ( UnknownTableIdRuntimeException e ) {
                iterator.remove();
            }
        }

        return procTables;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void addTableToPeriodicProcessing( long tableId ) {
        int beforeSize = frequencyDependentTables.size();
        getTable( tableId );
        if ( !frequencyDependentTables.contains( tableId ) ) {
            frequencyDependentTables.add( tableId );
        }
        // Initially starts the periodic job if this was the first table to enable periodic processing
        if ( beforeSize == 0 && frequencyDependentTables.size() == 1 ) {
            // Start Job for periodic processing
            FrequencyMap.INSTANCE.initialize();
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void removeTableFromPeriodicProcessing( long tableId ) {
        getTable( tableId );
        if ( !frequencyDependentTables.contains( tableId ) ) {
            frequencyDependentTables.remove( tableId );
        }

        // Terminates the periodic job if this was the last table with periodic processing
        if ( frequencyDependentTables.size() == 0 ) {
            // Terminate Job for periodic processing
            FrequencyMap.INSTANCE.terminate();
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkIfExistsPartitionPlacement( int adapterId, long partitionId ) {
        CatalogPartitionPlacement placement = partitionPlacements.get( new Object[]{ adapterId, partitionId } );
        return placement != null;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogKey> getTableKeys( long tableId ) {
        return keys.values().stream().filter( k -> k.tableId == tableId ).collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogIndex> getIndexes( CatalogKey key ) {
        return indexes.values().stream().filter( i -> i.keyId == key.id ).collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogIndex> getForeignKeys( CatalogKey key ) {
        return indexes.values().stream().filter( i -> i.keyId == key.id ).collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogConstraint> getConstraints( CatalogKey key ) {
        return constraints.values().stream().filter( c -> c.keyId == key.id ).collect( Collectors.toList() );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isIndex( long keyId ) {
        return indexes.values().stream().anyMatch( i -> i.keyId == keyId );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isConstraint( long keyId ) {
        return constraints.values().stream().anyMatch( c -> c.keyId == keyId );
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isForeignKey( long keyId ) {
        return foreignKeys.values().stream().anyMatch( f -> f.referencedKeyId == keyId );
    }


    /**
     * Check if the specified key is used as primary key, index or constraint. If so, this is a NoOp. If it is not used, the key is deleted.
     */
    private void deleteKeyIfNoLongerUsed( Long keyId ) {
        if ( keyId == null ) {
            return;
        }
        CatalogKey key = getKey( keyId );
        LogicalTable table = getTable( key.tableId );
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
    }


    /**
     * Returns the id of they defined by the specified column ids. If this key does not yet exist, create it.
     *
     * @param tableId on which the key is defined
     * @param columnIds all involved columns
     * @param enforcementTime at which point during execution the key should be enforced
     * @return the id of the key
     * @throws GenericCatalogException if the key does not exist
     */
    private long getOrAddKey( long tableId, List<Long> columnIds, EnforcementTime enforcementTime ) throws GenericCatalogException {
        Long keyId = keyColumns.get( columnIds.stream().mapToLong( Long::longValue ).toArray() );
        if ( keyId != null ) {
            return keyId;
        }
        return addKey( tableId, columnIds, enforcementTime );
    }


    private long addKey( long tableId, List<Long> columnIds, EnforcementTime enforcementTime ) throws GenericCatalogException {
        try {
            LogicalTable table = Objects.requireNonNull( tables.get( tableId ) );
            long id = keyIdBuilder.getAndIncrement();
            CatalogKey key = new CatalogKey( id, table.id, table.namespaceId, columnIds, enforcementTime );
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


    /**
     * {@inheritDoc}
     */
    @Override
    public List<CatalogKey> getKeys() {
        return new ArrayList<>( keys.values() );
    }


    /**
     * Get a key by its id
     *
     * @return The key
     */
    private CatalogKey getKey( long keyId ) {
        try {
            return Objects.requireNonNull( keys.get( keyId ) );
        } catch ( NullPointerException e ) {
            throw new UnknownKeyIdRuntimeException( keyId );
        }
    }


    static class CatalogValidator {

        public void validate() throws GenericCatalogException {

        }


        public void startCheck() {
            columns.forEach( ( key, column ) -> {
                assert (schemas.containsKey( column.schemaId ));
                assert (Objects.requireNonNull( schemaChildren.get( column.schemaId ) ).contains( column.tableId ));

                assert (tables.containsKey( column.tableId ));
                assert (Objects.requireNonNull( tableChildren.get( column.tableId ) ).contains( column.id ));

                assert (columnNames.containsKey( new Object[]{ column.schemaId, column.tableId, column.name } ));
            } );

            columnPlacements.forEach( ( key, placement ) -> {
                assert (columns.containsKey( placement.columnId ));
                assert (adapters.containsKey( placement.adapterId ));
            } );
        }

    }

}

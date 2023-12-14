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

package org.polypheny.db.backup;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.backup.datagatherer.GatherEntries;
import org.polypheny.db.backup.datagatherer.GatherSchema;
import org.polypheny.db.backup.datainserter.InsertEntries;
import org.polypheny.db.backup.datainserter.InsertSchema;
import org.polypheny.db.backup.dependencies.DependencyManager;
import org.polypheny.db.backup.dependencies.EntityReferencer;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.information.*;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Triple;


@Slf4j
/**
 * The BackupManager manages the backup process. It manages the data gathering, inserting and saving.
 */
public class BackupManager {


    @Getter
    private static BackupManager INSTANCE = null;
    private InformationPage informationPage;
    private InformationGroup informationGroupOverview;
    @Getter
    private BackupInformationObject backupInformationObject;
    public static TransactionManager transactionManager = null;
    public static int batchSize = 2;  //#rows (100 for the beginning)
    public static int threadNumber = 8; //#cores (#cpu's) for now
    //private final Logger logger;


    /**
     * Constructor for the BackupManager
     * @param transactionManager the transactionManager is required to execute queries
     */
    public BackupManager( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;

        informationPage = new InformationPage( "Backup Tasks" );
        informationPage.fullWidth();
        informationGroupOverview = new InformationGroup( informationPage, "Overview" );

        // datagatherer.GatherEntries gatherEntries = new datagatherer.GatherEntries();
        //GatherEntries gatherEntries = new GatherEntries();

        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );
        im.addGroup( informationGroupOverview );

        // start backup button
        InformationText startBackup = new InformationText( informationGroupOverview, "Create the Backup." );
        startBackup.setOrder( 1 );
        im.registerInformation( startBackup );

        InformationAction startBackupAction = new InformationAction( informationGroupOverview, "Start", parameters -> {
            //IndexManager.getInstance().resetCounters();
            startDataGathering();
            System.out.println( "gather" );
            return "Successfully started backup";
        } );
        startBackupAction.setOrder( 2 );
        im.registerInformation( startBackupAction );

        // insert backup-data button
        InformationText insertBackupData = new InformationText( informationGroupOverview, "Insert the Backup Data." );
        insertBackupData.setOrder( 3 );
        im.registerInformation( insertBackupData );

        InformationAction insertBackupDataAction = new InformationAction( informationGroupOverview, "Insert", parameters -> {
            //IndexManager.getInstance().resetCounters();
            startInserting();
            System.out.println( "hii" );
            return "Successfully inserted backup data";
        } );
        insertBackupDataAction.setOrder( 4 );
        im.registerInformation( insertBackupDataAction );

    }


    /**
     * Sets and returns the BackupManager instance
     * @param backupManager The backupManager to be set
     * @return the instance of the backupManager
     */
    public static BackupManager setAndGetInstance( BackupManager backupManager ) {
        if ( INSTANCE != null ) {
            throw new GenericRuntimeException( "Setting the BackupInterface, when already set is not permitted." );
        }
        INSTANCE = backupManager;
        return INSTANCE;
    }


    /**
     * Starts the data gathering process. It is responsible for starting and managing the schema and the entry data gathering.
     * It starts the wrapping process of the schema data.
     * It also starts the process of saving the data to a file (resp. it is started in the GatherEntries class)
     */
    public void startDataGathering() {
        this.backupInformationObject = new BackupInformationObject();
        GatherSchema gatherSchema = new GatherSchema();

        //gatherEntries.start();
        this.backupInformationObject = gatherSchema.start( backupInformationObject );
        wrapEntities();

        // how/where do i safe the data
        //gatherEntries.start();


        List<Triple<Long, String, String>> tablesForDataCollection = tableDataToBeGathered();
        List<Triple<Long, String, String>> collectionsForDataCollection = collectionDataToBeGathered();
        List<Long> graphNamespaceIds = collectGraphNamespaceIds();
        GatherEntries gatherEntries = new GatherEntries(transactionManager, tablesForDataCollection, collectionsForDataCollection, graphNamespaceIds);
        gatherEntries.start();
        log.info( "finished all datagathering" );


    }


    /**
     * Wraps the entities with the BackupEntityWrapper. Each entity is wrapped indivually (but the wrapping itself is not happening here), and are brought back into the respecitve (map or list) form for the BackupInformationObject
     * Part of the wrapped information is the entity itself, if the entity should be inserted, the name used in the insertion and the dependencies between the entites.
     * Entities that are wrapped are: namespaces, tables, collections (to be done: views, indexes (document and relational), materialized views)
     */
    private void wrapEntities() {
        // 1. check for dependencies
        // 2. wrap namespaces, tables, views, etc

        ImmutableMap<Long, List<LogicalForeignKey>> foreignKeysPerTable = backupInformationObject.getForeignKeysPerTable();
        Map<Long, List<Long>> namespaceDependencies = new HashMap<>();  // key: namespaceId, value: referencedKeySchemaId
        Map<Long, List<Long>> tableDependencies = new HashMap<>();  // key: tableId, value: referencedKeyTableId
        Map<Long, List<Pair<Long, Long>>> namespaceTableDependendencies = new HashMap<>();  // key: namespaceId, value: <namespaceId, referencedKeyTableId>
        Map<Long, List<Long>> viewDependencies = new HashMap<>();
        //TODO(FF): are there dependencies for collections? (views/indexes from collections?)

        //go through all foreign keys, and check if the namespaceId equals the referencedKeySchemaId, and if not, add it to the namespaceDependencies map, with the namespaceId as key and the referencedKeySchemaId as value
        for ( Map.Entry<Long, List<LogicalForeignKey>> entry : foreignKeysPerTable.entrySet() ) {
            for ( LogicalForeignKey logicalForeignKey : entry.getValue() ) {
                if ( logicalForeignKey.namespaceId != logicalForeignKey.referencedKeySchemaId ) {

                    // Check for namespace dependencies
                    if ( namespaceDependencies.containsKey( logicalForeignKey.namespaceId ) ) {
                        List<Long> temp = namespaceDependencies.get( logicalForeignKey.namespaceId );
                        //only add it if it isn't already in the list??
                        temp.add( logicalForeignKey.referencedKeySchemaId );
                        namespaceDependencies.put( logicalForeignKey.namespaceId, temp );
                    } else {
                        List<Long> temp = new ArrayList<>();
                        temp.add( logicalForeignKey.referencedKeySchemaId );
                        namespaceDependencies.put( logicalForeignKey.namespaceId, temp );
                    }

                    // Check for table dependencies
                    if ( tableDependencies.containsKey( logicalForeignKey.tableId ) ) {
                        List<Long> temp = tableDependencies.get( logicalForeignKey.tableId );
                        temp.add( logicalForeignKey.referencedKeyTableId );
                        tableDependencies.put( logicalForeignKey.tableId, temp );

                        List<Pair<Long, Long>> temp2 = namespaceTableDependendencies.get( logicalForeignKey.namespaceId );
                        temp2.add( new Pair<>( logicalForeignKey.referencedKeySchemaId, logicalForeignKey.referencedKeyTableId ) );
                        namespaceTableDependendencies.put( logicalForeignKey.namespaceId, temp2 );
                    } else {
                        List<Long> temp = new ArrayList<>();
                        temp.add( logicalForeignKey.referencedKeyTableId );
                        tableDependencies.put( logicalForeignKey.tableId, temp );

                        List<Pair<Long, Long>> temp2 = new ArrayList<>();
                        temp2.add( new Pair<>( logicalForeignKey.referencedKeySchemaId, logicalForeignKey.referencedKeyTableId ) );
                        namespaceTableDependendencies.put( logicalForeignKey.namespaceId, temp2 );
                    }


                }
            }
        }

        // wrap all namespaces with BackupEntityWrapper
        ImmutableMap<Long, BackupEntityWrapper<LogicalNamespace>> wrappedNamespaces = backupInformationObject.wrapNamespaces( backupInformationObject.getNamespaces(), namespaceDependencies, tableDependencies, namespaceTableDependendencies);
        backupInformationObject.setWrappedNamespaces( wrappedNamespaces );

        // wrap all tables with BackupEntityWrapper
        ImmutableMap<Long, List<BackupEntityWrapper<LogicalEntity>>> wrappedTables = backupInformationObject.wrapLogicalEntities( backupInformationObject.getTables(), tableDependencies, namespaceTableDependendencies, true);
        backupInformationObject.setWrappedTables( wrappedTables );

        // wrap all collections with BackupEntityWrapper
        ImmutableMap<Long, List<BackupEntityWrapper<LogicalEntity>>> wrappedCollections = backupInformationObject.wrapLogicalEntities( backupInformationObject.getCollections(), null, namespaceTableDependendencies, true);
        backupInformationObject.setWrappedCollections( wrappedCollections );

        /*
        ArrayList<LogicalTable> lol = new ArrayList<>();
        lol.add( (LogicalTable) backupInformationObject.getTables().get( 0 ));

        Map<Long, List<LogicalEntity>> lol2 = backupInformationObject.getTables();
        Map<Integer, List<LogicalEntity>> lol3 =  backupInformationObject.test2(lol2);


        //ImmutableMap<Integer, LogicalTable> ha = backupInformationObject.test( lol );

         */

        // testing
        DependencyManager dependencyManager = new DependencyManager();
        EntityReferencer entityReferencer = null;
        List<EntityReferencer> allTableReferencers = backupInformationObject.getAllTableReferencers();
        Map<Long, List<Long>> test = new HashMap<>();
        if (entityReferencer != null) {
            for ( EntityReferencer tableReferencer : allTableReferencers ) {
                List<Long> lol = dependencyManager.getReferencedEntities(entityReferencer, allTableReferencers );
                test.put( tableReferencer.getEntityId(), lol );
            }
        }



    }


    /**
     * Starts the inserting process. It is responsible for starting and managing the schema and the entry data inserting.
     */
    private void startInserting() {
        InsertSchema insertSchema = new InsertSchema( transactionManager );

        if ( backupInformationObject != null ) {
            insertSchema.start( backupInformationObject );
        } else {
            log.info( "backupInformationObject is null" );
        }


        InsertEntries insertEntries = new InsertEntries(transactionManager);
        insertEntries.start();
        log.info( "inserting done" );
    }


    /**
     * Returns a list of all table names where the entry-data should be collected for the backup (right now, all of them, except sources)
     * @return List of triples with the format: <namespaceId (long), namespaceName (string), tablename (string)>
     */
    private List<Triple<Long, String, String>> tableDataToBeGathered() {
        List<Triple<Long, String, String>> tableDataToBeGathered = new ArrayList<>();
        List<LogicalNamespace> relationalNamespaces = backupInformationObject.getRelNamespaces();

        if (!relationalNamespaces.isEmpty()) {
            for ( LogicalNamespace relationalNamespace : relationalNamespaces ) {
                List<LogicalEntity> tables = backupInformationObject.getTables().get( relationalNamespace.id );
                if(!tables.isEmpty() ) {
                    for ( LogicalEntity table : tables ) {
                        if (!(table.entityType.equals( EntityType.SOURCE ))) {
                            Triple triple = new Triple( relationalNamespace.id, relationalNamespace.name, table.name );
                            tableDataToBeGathered.add( triple );
                        }
                    }
                }
            }
        }

        return tableDataToBeGathered;
    }


    /**
     * Returns a list of triples with all collection names and their corresponding namespaceId where the entry-data should be collected for the backup (right now all of them)
     * @return List of triples with the format: <namespaceId (long), namespaceName (string), collectionName (string)>
     */
    private List<Triple<Long, String, String>> collectionDataToBeGathered() {
        List<Triple<Long, String, String>> collectionDataToBeGathered = new ArrayList<>();

        for ( Map.Entry<Long, List<LogicalEntity>> entry : backupInformationObject.getCollections().entrySet() ) {
            for ( LogicalEntity collection : entry.getValue() ) {
                String nsName = getNamespaceName( entry.getKey() );
                collectionDataToBeGathered.add( new Triple<>( entry.getKey(), nsName, collection.name ) );
            }
        }
        return collectionDataToBeGathered;
    }


    /**
     * Gets a list of all graph namespaceIds, which should be collected in the backup (right now all of them)
     * @return List of all graph namespaceIds
     */
    private List<Long> collectGraphNamespaceIds() {
        List<Long> graphNamespaceIds = new ArrayList<>();
        for ( Map.Entry<Long, LogicalEntity> entry : backupInformationObject.getGraphs().entrySet() ) {
            graphNamespaceIds.add( entry.getKey() );
        }
        return graphNamespaceIds;
    }


    /**
     * Gets the namespaceName for a given namespaceId
     * @param nsId id of the namespace you want the name for
     * @return the name of the namespace
     */
    private String getNamespaceName (Long nsId) {
        String namespaceName = backupInformationObject.getNamespaces().stream().filter( namespace -> namespace.id == nsId ).findFirst().get().name;
        return namespaceName;
    }


    /**
     * Gets the number of columns for a given table (via the table name)
     * @param nsId The id of the namespace where the table is located in
     * @param tableName The name of the table you want to know the number of columns from
     * @return The number of columns for the table
     */
    public int getNumberColumns( Long nsId, String tableName ) {
        //get the tableId for the given tableName
        Long tableId = backupInformationObject.getTables().get( nsId ).stream().filter( table -> table.name.equals( tableName ) ).findFirst().get().id;

        // go through all columns in the backupinformationobject and see how many are associated with a table
        int nbrCols = backupInformationObject.getColumns().get( tableId ).size();
        log.info( String.format( "nbr cols for table %s: %s", tableName, nbrCols ));
        return nbrCols;
    }

}

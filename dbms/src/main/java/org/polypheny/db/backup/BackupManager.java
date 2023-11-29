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
import org.polypheny.db.backup.datainserter.InsertSchema;
import org.polypheny.db.backup.dependencies.DependencyAssembler;
import org.polypheny.db.backup.dependencies.DependencyManager;
import org.polypheny.db.backup.dependencies.EntityReferencer;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.information.*;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;


@Slf4j
public class BackupManager {


    private static BackupManager INSTANCE = null;
    private InformationPage informationPage;
    private InformationGroup informationGroupOverview;
    @Getter
    private BackupInformationObject backupInformationObject;
    public static TransactionManager transactionManager = null;
    //private final Logger logger;


    public BackupManager( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;

        informationPage = new InformationPage( "Backup Tasks" );
        informationPage.fullWidth();
        informationGroupOverview = new InformationGroup( informationPage, "Overview" );

        // datagatherer.GatherEntries gatherEntries = new datagatherer.GatherEntries();
        GatherEntries gatherEntries = new GatherEntries();

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


    public static BackupManager setAndGetInstance( BackupManager backupManager ) {
        if ( INSTANCE != null ) {
            throw new GenericRuntimeException( "Setting the BackupInterface, when already set is not permitted." );
        }
        INSTANCE = backupManager;
        return INSTANCE;
    }


    public void startDataGathering() {
        this.backupInformationObject = new BackupInformationObject();
        //GatherEntries gatherEntries = new GatherEntries();
        GatherSchema gatherSchema = new GatherSchema();

        //gatherEntries.start();
        this.backupInformationObject = gatherSchema.start( backupInformationObject );
        wrapEntities();
    }


    private void wrapEntities() {
        // 1. check for dependencies
        // 2. wrap namespaces, tables, views, etc

        ImmutableMap<Long, List<LogicalForeignKey>> foreignKeysPerTable = backupInformationObject.getForeignKeysPerTable();
        Map<Long, List<Long>> namespaceDependencies = new HashMap<>();  // key: namespaceId, value: referencedKeySchemaId
        Map<Long, List<Long>> tableDependencies = new HashMap<>();  // key: tableId, value: referencedKeyTableId
        Map<Long, List<Pair<Long, Long>>> namespaceTableDependendencies = new HashMap<>();  // key: namespaceId, value: <namespaceId, referencedKeyTableId>
        Map<Long, List<Long>> viewDependencies = new HashMap<>();

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
                //FIXME(FF): nullpointerexception
                List<Long> lol = dependencyManager.getReferencedEntities(entityReferencer, allTableReferencers );
                test.put( tableReferencer.getEntityId(), lol );
            }
        }



    }


    private void startInserting() {
        InsertSchema insertSchema = new InsertSchema( transactionManager );

        insertSchema.start( backupInformationObject );
        log.info( "inserting done" );
    }

}

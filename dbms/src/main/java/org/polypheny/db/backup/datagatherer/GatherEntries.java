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

package org.polypheny.db.backup.datagatherer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.backup.BackupManager;
import org.polypheny.db.backup.datasaver.manifest.BackupManifestGenerator;
import org.polypheny.db.backup.datasaver.manifest.EntityInfo;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.Triple;

/**
 * This class is responsible for gathering the entries from the database that should be saved in the backup. One thread is created for each entity, and each entity gets one file.
 */
@Slf4j
public class GatherEntries {
    private final TransactionManager transactionManager;
    private final List<Triple<Long, String, String>> tablesToBeCollected;
    private final List<Triple<Long, String, String>> collectionsToBeCollected;
    private final List<Long> graphNamespaceIds;
    //private final int = hal.getProcessor().getPhysicalProcessorCount();

    @Getter
    private File backupFolder = null;
    @Getter
    private File dataFolder = null;
    PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();


    /**
     * Gathers entries from the database that should be saved in the backup, creates a thread for each entity (uses threadpool)
     * @param transactionManager TransactionManager to use
     * @param tablesToBeCollected List of tables that should be collected
     * @param collectionsForDataCollection List of collections that should be collected
     * @param graphNamespaceIds List of graph namespaces that should be collected
     */
    public GatherEntries( TransactionManager transactionManager, List<Triple<Long, String, String>> tablesToBeCollected, List<Triple<Long, String, String>> collectionsForDataCollection, List<Long> graphNamespaceIds ) {
        this.transactionManager = transactionManager;
        this.tablesToBeCollected = tablesToBeCollected;
        this.collectionsToBeCollected = collectionsForDataCollection;
        this.graphNamespaceIds = graphNamespaceIds;
    }

    // Move data around as little as possible -> use shortest possible path
    // Stream and flush data

    /**
     * Starts the gathering of the entries from the database that should be saved in the backup. The entries are gathered with select statemens and saved in files.
     */
    public void start() {
        ExecutorService executorService = null;
        List<EntityInfo> entityInfoList = new ArrayList<>();
        try {
            executorService = Executors.newFixedThreadPool( BackupManager.threadNumber );
            //initFileTest();
            //PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();
            backupFolder = homeDirManager.registerNewFolder( "backup" );
            dataFolder = homeDirManager.registerNewFolder( backupFolder, "data" );
            String dataFolderPath = "backup/data";

            if ( !tablesToBeCollected.isEmpty() ) {
                //go through each pair in tablesToBeCollectedList
                for ( Triple<Long, String, String> table : tablesToBeCollected ) {
                    List<String> filePaths = new ArrayList<>();
                    //int nbrCols = BackupManager.getNumberColumns(table.getLeft(), table.getRight());
                    int nbrCols = BackupManager.getINSTANCE().getNumberColumns( table.getLeft(), table.getRight() );
                    //TODO(FF): exclude default columns? no, how do you differentiate for each line if it is not a default value
                    String query = String.format( "SELECT * FROM %s.%s", table.getMiddle(), table.getRight() );
                    //executeQuery2( query, DataModel.RELATIONAL, Catalog.defaultNamespaceId );

                    String fileName = String.format( "tab_%s_%s.txt", table.getMiddle(), table.getRight() );
                    File tableData = homeDirManager.registerNewFile( getDataFolder(), fileName );
                    filePaths.add( String.format( "%s/%s", dataFolderPath, fileName ) );
                    EntityInfo entityInfo = new EntityInfo( filePaths, table.getRight(), table.getMiddle(), table.getLeft(), DataModel.RELATIONAL, nbrCols );
                    entityInfoList.add( entityInfo );
                    executorService.submit( new GatherEntriesTask( transactionManager, query, DataModel.RELATIONAL, Catalog.defaultNamespaceId, tableData ) );
                }
            /*
            for ( String nsTableName : tablesToBeCollected ) {
                String query = "SELECT * FROM " + nsTableName;
                executeQuery( query, DataModel.RELATIONAL, Catalog.defaultNamespaceId );
            } */
            }

            if ( !collectionsToBeCollected.isEmpty() ) {
                for ( Triple<Long, String, String> collection : collectionsToBeCollected ) {
                    List<String> filePaths = new ArrayList<>();
                    String query = String.format( "db.%s.find()", collection.getRight() );
                    //executeQuery2( query, DataModel.DOCUMENT, collection.getKey() );

                    String fileName = String.format( "col_%s.txt", collection.getRight() );
                    File collectionData = homeDirManager.registerNewFile( getDataFolder(), fileName );
                    filePaths.add( String.format( "%s/%s", dataFolderPath, fileName ) );
                    EntityInfo entityInfo = new EntityInfo( filePaths, collection.getRight(), collection.getMiddle(), collection.getLeft(), DataModel.DOCUMENT );
                    entityInfoList.add( entityInfo );
                    executorService.submit( new GatherEntriesTask( transactionManager, query, DataModel.DOCUMENT, collection.getLeft(), collectionData ) );
                }
            }

            if ( !graphNamespaceIds.isEmpty() ) {
                for ( Long graphNamespaceId : graphNamespaceIds ) {
                    List<String> filePaths = new ArrayList<>();
                    String query = "MATCH (*) RETURN n";
                    //executeQuery2( query, DataModel.GRAPH, graphNamespaceId );
                    String nsName = Catalog.snapshot().getNamespace( graphNamespaceId ).orElseThrow().name;

                    String fileName = String.format( "graph_%s.txt", graphNamespaceId.toString() );
                    File graphData = homeDirManager.registerNewFile( getDataFolder(), fileName );
                    filePaths.add( String.format( "%s/%s", dataFolderPath, fileName ) );
                    EntityInfo entityInfo = new EntityInfo( filePaths, nsName, nsName, graphNamespaceId, DataModel.GRAPH );
                    entityInfoList.add( entityInfo );
                    executorService.submit( new GatherEntriesTask( transactionManager, query, DataModel.GRAPH, graphNamespaceId, graphData ) );
                }
            }

            log.info( "collected entry data" );
            //initializeFileLocation();
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.MINUTES);
            log.info( "executor service was shut down" );

        } catch ( Exception e ) {
            throw new GenericRuntimeException( "An error occured during threadpooling to collect the data: " + e.getMessage() );
        }
        /*
        finally {
            if ( Objects.nonNull( executorService ) && !executorService.isTerminated() ) {
                log.error( "cancelling all non-finished tasks" );
            }
            if ( Objects.nonNull( executorService ) ) {
                //executorService.shutdownNow();
                log.info( "shutdown finished" );
            }
        }

         */

        try {
            Calendar calendar = Calendar.getInstance();
            Date currentDate = calendar.getTime();
            //TODO(FF): calculate checksum
            File manifestFile = homeDirManager.registerNewFile( getBackupFolder(), "manifest.txt" );
            BackupManifestGenerator.generateManifest( entityInfoList, "", manifestFile, currentDate );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Could not create manifest for backup" + e.getMessage() );
        }

    }
}

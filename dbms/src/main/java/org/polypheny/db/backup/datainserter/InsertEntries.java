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

package org.polypheny.db.backup.datainserter;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.backup.BackupManager;
import org.polypheny.db.backup.datasaver.manifest.BackupManifest;
import org.polypheny.db.backup.datasaver.manifest.EntityInfo;
import org.polypheny.db.backup.datasaver.manifest.ManifestReader;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.PolyphenyHomeDirManager;

@Slf4j
public class InsertEntries {
    Optional<File> backupFile = null;
    TransactionManager transactionManager = null;


    public InsertEntries(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }


    public void start() {
        ExecutorService executorService = null;
        try {
            executorService = Executors.newFixedThreadPool( BackupManager.threadNumber );
            PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();

            this.backupFile = homeDirManager.getHomeFile( "backup" );
            Optional<File> manifestFile = homeDirManager.getHomeFile( "backup/manifest.txt" );
            BackupManifest manifest = new ManifestReader().readManifest( manifestFile.get().getPath() );

            File[] files = backupFile.get().listFiles();

            for ( EntityInfo entityInfo : manifest.getEntityInfos() ) {
                for ( String path : entityInfo.getFilePaths()) {
                    //TODO(FF): check if file is there from path, if not, skip it and move to next file...
                    //File filee = new File( path.toString() );
                    //Path filePath = filee.toPath();
                    //File file = filePath.toFile();
                    File file = homeDirManager.getHomeFile( path ).get();
                    log.info( path );
                    if ( file.isFile() && file.exists() ) {
                        long nsId = Catalog.snapshot().getNamespace( entityInfo.getNamespaceName() ).orElseThrow().id;
                        log.info( "insertEntries - file exists: " + file.getPath() );
                        //TransactionManager transactionManager, File dataFile, DataModel dataModel, Long namespaceId, String namespaceName, String tableName, int nbrCols
                        executorService.submit( new InsertEntriesTask( transactionManager, file, entityInfo.getDataModel(), nsId, entityInfo.getNamespaceName(), entityInfo.getEntityName(), entityInfo.getNbrCols() ) );
                    } else {
                        log.warn( "Insert Entries for Backup: " + path + " does not exist, but is listed in manifest" );
                    }

                }
            }

                /*

                if ( backupFile != null ) {
                for ( File file : files ) {
                    if ( file.isDirectory() ) {
                        log.info( "insertEntries: " + file.getPath() );
                        File[] subFiles = file.listFiles();
                        for ( File subFile : subFiles ) {
                            if ( subFile.isFile() ) {
                                executorService.submit( new InsertEntriesTask( subFile, DataModel.RELATIONAL, "reli", "album" ) );
                            }
                        }
                    }

                File dataFolder = homeDirManager.getFileIfExists( "backup/data" );
                File[] dataFiles = dataFolder.listFiles();
                for ( File dataFile : dataFiles ) { //i can go through file... (or check if file is file, bcs if it is folder, subfiles are listed
                    executorService.submit( new InsertEntriesTask( dataFile ) );
                }

                 */
                /*
                if ( file.isFile() ) {
                    executorService.submit( new InsertEntriesTask( file ) );
                }
                }
            }
                 */

            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.MINUTES);
            log.info( "executor service was shut down" );

        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error with threadpool, datagathering: " + e.getMessage() );
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


    }

}

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
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.backup.BackupManager;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.util.PolyphenyHomeDirManager;

@Slf4j
public class InsertEntries {
    File backupFile = null;


    public InsertEntries() {
    }


    public void start() {
        ExecutorService executorService = null;
        try {
            executorService = Executors.newFixedThreadPool( BackupManager.threadNumber );
            //initFileTest();
            PolyphenyHomeDirManager fileSystemManager = PolyphenyHomeDirManager.getInstance();
            // getfiles (or operate over path?)
            this.backupFile = fileSystemManager.getFileIfExists( "backup" );
            log.info( "insertEntries: " + backupFile.getPath() );
            File[] files = backupFile.listFiles();
            File[] lol = fileSystemManager.getFileIfExists( backupFile.getPath() ).listFiles();    //FIXME(FF): is null... try filesystemManager.dirs

            if ( backupFile != null ) {
                //TODO(FF): get correct file to read from...
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
                /*
                File dataFolder = fileSystemManager.getFileIfExists( "backup/data" );
                File[] dataFiles = dataFolder.listFiles();
                for ( File dataFile : dataFiles ) { //i can go through file... (or check if file is file, bcs if it is folder, subfiles are listed
                    executorService.submit( new InsertEntriesTask( dataFile ) );
                }

                 */
                /*
                if ( file.isFile() ) {
                    executorService.submit( new InsertEntriesTask( file ) );
                }

                 */
                }
            }
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        } finally {
            if ( Objects.nonNull( executorService ) && !executorService.isTerminated() ) {
                log.error( "cancelling all non-finished tasks" );
            }
            if ( Objects.nonNull( executorService ) ) {
                //executorService.shutdownNow();
                log.info( "shutdown finished" );
            }
        }


    }

}

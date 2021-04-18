/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.monitoring.subscriber;


import java.io.File;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.polypheny.db.monitoring.MonitorEvent;
import org.polypheny.db.monitoring.storage.BackendConnector;
import org.polypheny.db.util.FileSystemManager;


@Slf4j
public class InternalSubscriber extends AbstractSubscriber{


    private static final String subscriberName = "_SYS_INTERNAL";
    private static final String FILE_PATH = "internalSubscriberBackendDb";
    private static DB internalSubscriberBackendDb;

    public InternalSubscriber(){
        this.isPersistent = true;
        this.initializeSubscriber();
    }

    public InternalSubscriber( BackendConnector backendConnector ){
        this.isPersistent = true;
        this.backendConnector = backendConnector;
        this.initializeSubscriber();
    }


    @Override
    protected void initializeSubscriber() {
        setSubscriberName( this.subscriberName );
    }


    @Override
    public void handleEvent( MonitorEvent event ) {
        log.info( "Internal received event which originated at: " + new Timestamp( event.getRecordedTimestamp()) );
    }

    protected void initPersistentDB() {


        if ( internalSubscriberBackendDb != null ) {
            internalSubscriberBackendDb.close();
        }
        synchronized ( this ) {

            File folder = FileSystemManager.getInstance().registerNewFolder( "monitoring" );

            internalSubscriberBackendDb = DBMaker.fileDB( new File( folder, this.FILE_PATH ) )
                    .closeOnJvmShutdown()
                    .transactionEnable()
                    .fileMmapEnableIfSupported()
                    .fileMmapPreclearDisable()
                    .make();

            internalSubscriberBackendDb.getStore().fileLoad();

            /* ToDO: Extend to dummy frontend
            tableEvents = simpleBackendDb.treeMap( "tableEvents", Serializer.STRING, Serializer.STRING ).createOrOpen();
            tableColumnEvents = simpleBackendDb.treeMap( "tableColumnEvents", Serializer.STRING, Serializer.STRING ).createOrOpen();
            tableValueEvents = simpleBackendDb.treeMap( "tableValueEvents", Serializer.STRING, Serializer.LONG ).createOrOpen();
            events = simpleBackendDb.treeMap( "events", Serializer.LONG, Serializer.JAVA ).createOrOpen();
            */

        }

    }
}

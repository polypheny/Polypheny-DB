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

package org.polypheny.db.mqtt;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.EntityAlreadyExistsException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.PolyphenyHomeDirManager;

@Slf4j
public class StreamCapture {

    TransactionManager transactionManager;
    PolyphenyHomeDirManager homeDirManager;
    String namespace;


    StreamCapture( final TransactionManager transactionManager, String namespace ) {
        this.transactionManager = transactionManager;
        this.namespace = namespace;
    }


    public void saveMsgInDocument( String topic, String message ) {
        //String path = registerTopicFolder(topic);
        this.createCollection( topic );
        //TODO: create Document in collection with message
    }


    void createCollection( String topic ) {
        Catalog catalog = Catalog.getInstance();

        long schemaId;
        //TODO: evtl. defaultDatabaseID anpassen + getSchema methodenaufruf
        schemaId = catalog.getSchema( Catalog.defaultDatabaseId ).id;

        Catalog.PlacementType placementType = Catalog.PlacementType.AUTOMATIC;
        Transaction transaction = null;
        Statement statement = null;

        try {
            transaction = this.transactionManager.startTransaction( Catalog.defaultUserId, Catalog.defaultDatabaseId, false, "MQTT Stream" );
            statement = transaction.createStatement();
        } catch ( Exception e ) {
            log.error( "An error occurred: {}", e.getMessage() );
        }

        try {
            List<DataStore> dataStores = new ArrayList<>();
            DdlManager.getInstance().createCollection(
                    schemaId,
                    this.namespace,
                    true,   //only creates collection if it does not already exist.
                    dataStores.size() == 0 ? null : dataStores,
                    placementType,
                    statement );
            log.info( "Created Collection with name: {}", topic );
        } catch ( EntityAlreadyExistsException e ) {
            throw new RuntimeException( "The generation of the collection was not possible, due to: " + e.getMessage() );
        }


    }


    private static String registerTopicFolder( String topic ) {
        PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();

        String path = File.separator + "mqttStreamPlugin" + File.separator + topic.replace( "/", File.separator );
        ;

        File file = null;
        if ( !homeDirManager.checkIfExists( path ) ) {
            file = homeDirManager.registerNewFolder( path );
            log.info( "New Directory created!" );
        } else {
            //TODO: rmv log
            log.info( "Directory already exists" );
        }

        return path;

    }


    private void getTransaction() {
        /**
         try {
         return transactionManager.startTransaction( userId, databaseId, false, "Stream Processing", Transaction.MultimediaFlavor.FILE );
         } catch (UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
         throw new RuntimeException( "Error while starting transaction", e );
         }
         **/
    }

}

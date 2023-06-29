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
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.exceptions.EntityAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.NamespaceAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
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
        long schemaId = createCollection( topic );
        log.info( "schemaId = {}", schemaId );
        log.info( "testing method createCollection" );
        Catalog catalog = Catalog.getInstance();
        CatalogSchema schema = null;
        schema = catalog.getSchema( schemaId );

        log.info( "Namespace name: {}, Namespace Type: {}, Database name: {}", schema.getName(), schema.getNamespaceType(), schema.getDatabaseName() );
        //TODO: create Document in collection with message
    }
    long createCollection( String topic ) {
        return createCollection( topic, Catalog.defaultDatabaseId, Catalog.defaultUserId );
    }
    long createCollection( String topic, long databaseId ) {
        return createCollection( topic, databaseId, Catalog.defaultUserId );
    }
    long createCollection( String topic, int userId ) {
        return createCollection( topic, Catalog.defaultDatabaseId, userId );
    }

    long createCollection( String topic, long databaseId, int userId ) {

        Catalog catalog = Catalog.getInstance();

        // In case there is a namespace with the same name existing and is of Type Document, then that id of the existing schema will be used to create a collection.
        long schemaId = schemaId = catalog.addNamespace( this.namespace, databaseId, userId, NamespaceType.DOCUMENT );

        // check for namespace and NamespaceType:
        try {
            CatalogSchema schema = catalog.getSchema( databaseId, this.namespace );

            if ( catalog.checkIfExistsSchema( databaseId, this.namespace ) && schema.namespaceType == NamespaceType.DOCUMENT ) {
                schemaId =  schema.id;
            }
        } catch ( UnknownSchemaException e ) {
            log.error( "The catalog seems to be corrupt, as it was impossible to retrieve an existing namespace." );
        }

        //TODO: abfragen ob es collection schon gibt
        //TODO: Namespace abfragen/erstellen und collection abfragen/erstellen in versch methoden machen

        Catalog.PlacementType placementType = Catalog.PlacementType.AUTOMATIC;
        Transaction transaction = null;
        Statement statement = null;

        try {
            transaction = this.transactionManager.startTransaction( Catalog.defaultUserId, databaseId, false, "MQTT Stream" );
            statement = transaction.createStatement();
        } catch ( Exception e ) {
            log.error( "An error occurred: {}", e.getMessage() );
        }

        try {
            List<DataStore> dataStores = new ArrayList<>();
            DdlManager.getInstance().createCollection(
                    schemaId,
                    topic,
                    true,   //only creates collection if it does not already exist.
                    dataStores.size() == 0 ? null : dataStores,
                    placementType,
                    statement );
            log.info( "Created Collection with name: {}", topic );
        } catch ( EntityAlreadyExistsException e ) {
            log.error( "The generation of the collection was not possible due to: " + e.getMessage() );
        }
        return schemaId;

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


}

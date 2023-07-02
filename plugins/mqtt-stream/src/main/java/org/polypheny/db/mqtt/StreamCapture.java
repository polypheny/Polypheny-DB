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
    PolyStream stream;


    StreamCapture( final TransactionManager transactionManager, PolyStream stream) {
        this.transactionManager = transactionManager;
        this.stream = stream;

    }


    public void handleContent( ) {
        //String path = registerTopicFolder(topic);
        long storeId = getCollection( );
        if ( storeId != 0 ) {
            stream.setStoreID( storeId );
            boolean saved = saveContent();
            log.info( "testing method createCollection" );
            Catalog catalog = Catalog.getInstance();
            CatalogSchema schema = null;
            schema = catalog.getSchema( stream.getNamespaceID() );

            log.info( "Namespace name: {}, Namespace Type: {}, Database name: {}", schema.getName(), schema.getNamespaceType(), schema.getDatabaseName() );

        }
    }

    /**
     *
     * @return the id of the collection that was already existing with the topic as name or that was newly created
     */
    long getCollection( ) {
        // TODO: schemaID ist NamespaceId ???
        Catalog catalog = Catalog.getInstance();

        // In case there is a namespace with the same name existing and is of Type Document, then that id of the existing schema will be used to create a collection.
        long schemaId = 0;

        // check for existing namespace with DOCUMENT NamespaceType:
        if ( catalog.checkIfExistsSchema( stream.databaseId, stream.getNamespace() ) ) {
            CatalogSchema schema = null;
            try {
                schema = catalog.getSchema( stream.databaseId, stream.getNamespace() );
            } catch ( UnknownSchemaException e ) {
                log.error( "The catalog seems to be corrupt, as it was impossible to retrieve an existing namespace." );
                // TODO: what to do here? Maybe get schema again?
                //Not this as Namespace already exists...OR create Namespace of Datatype Document ???
                // schemaId = catalog.addNamespace( this.namespace, databaseId, userId, NamespaceType.DOCUMENT );
                return 0;
            }

            assert schema != null;
            if (schema.namespaceType == NamespaceType.DOCUMENT ) {
                //check for collection with same name //TODO: maybe change the collection name, currently collection name is the topic
                List<CatalogCollection> collectionList =  catalog.getCollections( schema.id, null );
                for ( CatalogCollection collection : collectionList ) {
                    if( collection.name.equals( stream.topic ) ) {
                        //TODO: vll. Placement abfragen?
                            //collection.addPlacement( adapterId );
                            //collection.removePlacement( adapterID );
                        return collection.id;
                        // and return
                    }
                }

                //TODO: no Collection with this name: so create Collection here, and return collection id


            } else {
                //TODO: Namespacetype is not of type Document -> what to do:
                //maybe create new Namespace with type Document:
                return createNewCollection();
            }
        }
        if ( addNewNamespace() ) {
            return createNewCollection();
        }
        return 0;

    }

    private boolean addNewNamespace( ) {
        boolean methodFinished = false;
        Catalog catalog = Catalog.getInstance();
        stream.setNamespaceID( catalog.addNamespace( stream.getNamespace(), stream.databaseId, stream.userId, NamespaceType.DOCUMENT ) );
        methodFinished = true;
        return methodFinished;
    }


    private long createNewCollection() {
        // TODO: new collection
        boolean methodFinished = false;
        Catalog catalog = Catalog.getInstance();

        //Catalog.PlacementType placementType = Catalog.PlacementType.AUTOMATIC;
        Transaction transaction = null;
        Statement statement = null;

        try {
            transaction = this.transactionManager.startTransaction( Catalog.defaultUserId, stream.databaseId, false, "MQTT Stream" );
            statement = transaction.createStatement();
        } catch ( Exception e ) {
            log.error( "An error occurred: {}", e.getMessage() );
            return 0;
        }

        try {
            List<DataStore> dataStores = new ArrayList<>();
            //TODO: bei Placement adapter angeben!
            DdlManager.getInstance().createCollection(
                    this.stream.getNamespaceID(),
                    this.stream.topic,
                    true,   //only creates collection if it does not already exist.
                    dataStores.size() == 0 ? null : dataStores,
                    PlacementType.MANUAL,
                    statement );
            log.info( "Created Collection with name: {}", this.stream.topic );

        } catch ( EntityAlreadyExistsException e ) {
            log.error( "The generation of the collection was not possible due to: " + e.getMessage() );
            return 0;
        }

        List<CatalogCollection> collectionList =  catalog.getCollections( this.stream.getNamespaceID(), null );
        for ( int i = 0; i < collectionList.size(); i++ ) {
            if( collectionList.get( i ).name.equals( this.stream.topic ) ) {
                //TODO: vll. Placement einfügen?
                int adapterId = AdapterManager.getInstance().getStore( this.stream.getUniqueNameInterface() ).getAdapterId();
                collectionList.set( i, collectionList.get( i ).addPlacement( adapterId ) );

                return collectionList.get( i ).id;
            }
        }
        return 0;
    }

    boolean saveContent( ) {
        //TODO: save Message here -> Polyalgebra
        return true;
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

    public static void main ( String[] args ) {

    }


}

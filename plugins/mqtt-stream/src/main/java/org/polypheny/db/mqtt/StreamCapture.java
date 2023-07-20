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

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.exceptions.EntityAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaIdRuntimeException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.mqtt.MqttStreamPlugin.MqttStreamServer;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.PolyphenyHomeDirManager;

@Slf4j
public class StreamCapture {

    Transaction transaction;
    PolyphenyHomeDirManager homeDirManager;
    ReceivedMqttMessage receivedMqttMessage;


    StreamCapture( final Transaction transaction ) {
        this.transaction = transaction;
        //this.receivedMqttMessage = receivedMqttMessage;

    }


    public void handleContent( ReceivedMqttMessage receivedMqttMessage ) {
        this.receivedMqttMessage = receivedMqttMessage;
        if ( receivedMqttMessage.getStoreId() == 0 ) {
            //TODO: get store id of already existing or create new one.

            //TODO: maybe do this with interface
            if ( receivedMqttMessage.getNamespaceType() == NamespaceType.DOCUMENT ) {
                long newstoreId = getCollectionId();
                if ( newstoreId != 0 ) {
                    receivedMqttMessage.setStoreId( newstoreId );
                }
            }
        }
        boolean saved = saveContent();
        //TODO: gescheite Tests
//            Catalog catalog = Catalog.getInstance();
//            CatalogSchema schema = null;
//            schema = catalog.getSchema( stream.getNamespaceID() );
    }


    /**
     * @return the id of the collection that was either already existing with the topic as name or that was newly created
     */
    long getCollectionId() {
        Catalog catalog = Catalog.getInstance();
        //check for collection with same name
        List<CatalogCollection> collectionList = catalog.getCollections( this.receivedMqttMessage.getNamespaceId(), null );
        for ( CatalogCollection collection : collectionList ) {
            if ( collection.name.equals( this.receivedMqttMessage.getTopic() ) ) {
                int queryInterfaceId = QueryInterfaceManager.getInstance().getQueryInterface( this.receivedMqttMessage.getUniqueNameOfInterface() ).getQueryInterfaceId();
                if ( !collection.placements.contains( queryInterfaceId ) ) {
                    log.info( "found matching collection!" );
                    //TODO: Nur AdapterID können als Placements hinzugefügt werden. nicht QueryInterfaceIds -> Marco fragen.
                    //catalog.addCollectionPlacement( queryInterfaceId, collection.id, PlacementType.MANUAL );
                    return collection.id;
                } else {
                    log.info( "found matching collection!" );
                    return collection.id;
                }
            }
        }

        return 0;
    }


    boolean saveContent() {
        if ( this.receivedMqttMessage.getNamespaceType() == NamespaceType.DOCUMENT ) {
            insertDocument();
        }
        return true;
    }


    // added by Datomo
    public void insertDocument() {
        String collectionName = this.receivedMqttMessage.getNamespaceName() + "." + this.receivedMqttMessage.getTopic();
        Statement statement = transaction.createStatement();

        // Builder which allows to construct the algebra tree which is equivalent to query and is executed
        AlgBuilder builder = AlgBuilder.createDocumentBuilder( statement );

        // we insert document { age: 28, name: "David" } into the collection users
        BsonDocument document = new BsonDocument();
        //TODO: change to id:
        document.put( "topic", new BsonString( this.receivedMqttMessage.getTopic() ) );
        document.put( "content", new BsonString( this.receivedMqttMessage.getMessage() ) );

        AlgNode algNode = builder.docInsert( statement, collectionName, document ).build();

        // we can then wrap the tree in an AlgRoot and execute it
        AlgRoot root = AlgRoot.of( algNode, Kind.INSERT );
        // for inserts and all DML queries only a number is returned
        List<List<Object>> res = executeAndTransformPolyAlg( root, statement, statement.getPrepareContext() );
        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            throw new RuntimeException( e );
        }
        log.info( "inserted message as document" );
    }


    public List<String> getRecentMessages( String namespaceName, String topic ) {
        List<String> listOfDocuments = scanCollection( namespaceName, topic );
        List<String> listOfMessage = new ArrayList<>();
        //TODO: rmv debugging log: log.info( "listOfDocuments:{}", listOfDocuments );
        //only show last 20 Messages:
        int indexLastMessage = 0;
        if ( listOfDocuments.size() > 20 ) {
            indexLastMessage = listOfDocuments.size() - 20;
        }
        for ( int i = indexLastMessage; i < listOfDocuments.size(); i++ ) {
            String message = listOfDocuments.get( i ).split( "," )[1].trim();
            message = message.substring( message.indexOf( ':' ) ).trim();
            message = message.substring( message.indexOf( '"' ) + 1, message.lastIndexOf( '"' ) );
            listOfMessage.add( message );
        }
        log.info( "extracted messages: {}", listOfMessage );
        return listOfMessage;
    }


    // added by Datomo
    public List<String> scanCollection( String namespaceName, String topic ) {
        String collectionName = namespaceName + "." + topic;
        Statement statement = transaction.createStatement();

        // Builder which allows to construct the algebra tree which is equivalent to query and is executed
        AlgBuilder builder = AlgBuilder.create( statement );

        AlgNode algNode = builder.docScan( statement, collectionName ).build();

        // we can then wrap the tree in an AlgRoot and execute it
        AlgRoot root = AlgRoot.of( algNode, Kind.SELECT );
        List<List<Object>> res = executeAndTransformPolyAlg( root, statement, statement.getPrepareContext() );
        List<String> result = new ArrayList<>();
        for ( List<Object> objectsList : res ) {
            result.add( objectsList.get( 0 ).toString() );
        }
        return result;
    }


    List<List<Object>> executeAndTransformPolyAlg( AlgRoot algRoot, Statement statement, final Context ctx ) {

        try {
            // Prepare
            PolyImplementation result = statement.getQueryProcessor().prepareQuery( algRoot, false );
            log.debug( "AlgRoot was prepared." );

            List<List<Object>> rows = result.getRows( statement, -1 );
            statement.getTransaction().commit();
            return rows;
        } catch ( Throwable e ) {
            log.error( "Error during execution of stream capture query", e );
            try {
                statement.getTransaction().rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Could not rollback", e );
            }
            return null;
        }
    }

}

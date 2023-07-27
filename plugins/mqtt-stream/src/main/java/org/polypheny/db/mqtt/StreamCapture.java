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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
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
    }


    public void handleContent( ReceivedMqttMessage receivedMqttMessage ) {
        this.receivedMqttMessage = receivedMqttMessage;
        insertDocument( this.receivedMqttMessage.getCollectionName() );
    }


    /**
     * @return the id of the collection that was either already existing with the topic as name or that was newly created
     */
/*    long getCollectionId() {
        Catalog catalog = Catalog.getInstance();
        //check for collection with same name
        List<CatalogCollection> collectionList = catalog.getCollections( this.receivedMqttMessage.getNamespaceId(), null );
        for ( CatalogCollection collection : collectionList ) {
            if ( collection.name.equals( this.receivedMqttMessage.getCollectionName() ) ) {
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
    */


    // added by Datomo
    public void insertDocument( String collectionName) {
        String sqlCollectionName = this.receivedMqttMessage.getNamespaceName() + "." + collectionName;
        Statement statement = transaction.createStatement();

        // Builder which allows to construct the algebra tree which is equivalent to query and is executed
        AlgBuilder builder = AlgBuilder.createDocumentBuilder( statement );

        // we insert document { age: 28, name: "David" } into the collection users
        BsonDocument document = new BsonDocument();
        document.put( "topic", new BsonString( this.receivedMqttMessage.getTopic() ) );
        document.put( "content", new BsonString( this.receivedMqttMessage.getMessage() ) );

        AlgNode algNode = builder.docInsert( statement, sqlCollectionName, document ).build();

        // we can then wrap the tree in an AlgRoot and execute it
        AlgRoot root = AlgRoot.of( algNode, Kind.INSERT );
        // for inserts and all DML queries only a number is returned
        List<List<Object>> res = executeAndTransformPolyAlg( root, statement, statement.getPrepareContext() );
        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            throw new RuntimeException( e );
        }
    }


    public List<MqttMessage> getMessages( String namespaceName, String collectionName ) {
        List<String> listOfDocuments = scanCollection( namespaceName, collectionName );
        List<MqttMessage> listOfMessage = new ArrayList<>();
        for ( String document : listOfDocuments ) {
            String[] documentAsList = document.split( "," );
            String topic = documentAsList[0].substring( documentAsList[0].indexOf( ':' ) );
            topic = topic.substring( topic.indexOf( '"' ) + 1, topic.lastIndexOf( '"' ) );

            String message = documentAsList[1].trim();
            message = message.substring( message.indexOf( ':' ) ).trim();
            message = message.substring( message.indexOf( '"' ) + 1, message.lastIndexOf( '"' ) );
            listOfMessage.add( new MqttMessage( message, topic ) );
        }
        return listOfMessage;
    }


    // added by Datomo
    public List<String> scanCollection( String namespaceName, String collectionName ) {
        String sqlCollectionName = namespaceName + "." + collectionName;
        Statement statement = transaction.createStatement();

        // Builder which allows to construct the algebra tree which is equivalent to query and is executed
        AlgBuilder builder = AlgBuilder.create( statement );

        AlgNode algNode = builder.docScan( statement, sqlCollectionName ).build();

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

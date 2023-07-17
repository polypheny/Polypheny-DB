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
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.pojo.annotations.BsonId;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.exceptions.EntityAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.Prepare.PreparingTable;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PolyphenyHomeDirManager;

@Slf4j
public class StreamCapture {

    TransactionManager transactionManager;
    PolyphenyHomeDirManager homeDirManager;
    PolyStream stream;


    StreamCapture( final TransactionManager transactionManager, PolyStream stream ) {
        this.transactionManager = transactionManager;
        this.stream = stream;

    }


    public void handleContent() {

        if ( stream.getStoreID() == 0 ) {
            //TODO: get store id of already existing or create new one.

            //TODO: maybe do this with interface
            if ( stream.getNamespaceType() == NamespaceType.DOCUMENT ) {
                long newstoreId = getCollectionID(  );
                if ( newstoreId != 0 ) {
                    stream.setStoreID( newstoreId );
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
    long getCollectionID() {
        Catalog catalog = Catalog.getInstance();
        //check for collection with same name //TODO: maybe change the collection name, currently collection name is the topic
        List<CatalogCollection> collectionList = catalog.getCollections( stream.getNamespaceID(), null );
        for ( CatalogCollection collection : collectionList ) {
            if ( collection.name.equals( this.stream.topic ) ) {
                int queryInterfaceId = QueryInterfaceManager.getInstance().getQueryInterface( this.stream.getUniqueNameOfInterface() ).getQueryInterfaceId();
                if ( !collection.placements.contains( queryInterfaceId ) ) {
                    return collection.addPlacement( queryInterfaceId ).id;
                } else {
                    return collection.id;
                }
            }
        }
        return createNewCollection();

    }

    private long createNewCollection() {
        Catalog catalog = Catalog.getInstance();

        //Catalog.PlacementType placementType = Catalog.PlacementType.AUTOMATIC;
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();

        try {
            List<DataStore> dataStores = new ArrayList<>();
            DdlManager.getInstance().createCollection(
                    this.stream.getNamespaceID(),
                    this.stream.topic,
                    true,   //only creates collection if it does not already exist.
                    dataStores.size() == 0 ? null : dataStores,
                    PlacementType.MANUAL,
                    statement );
            log.info( "Created Collection with name: {}", this.stream.topic );
            transaction.commit();
        } catch ( EntityAlreadyExistsException e ) {
            log.error( "The generation of the collection was not possible because there is a collection already existing with this name." );
            return 0;
        } catch ( TransactionException e ) {
            log.error( "The commit after creating a new Collection could not be completed!" );
            return 0;
        }
        //add placement
        List<CatalogCollection> collectionList = catalog.getCollections( this.stream.getNamespaceID(), null );
        for ( int i = 0; i < collectionList.size(); i++ ) {
            if ( collectionList.get( i ).name.equals( this.stream.topic ) ) {
                int queryInterfaceId = QueryInterfaceManager.getInstance().getQueryInterface( this.stream.getUniqueNameOfInterface() ).getQueryInterfaceId();
                collectionList.set( i, collectionList.get( i ).addPlacement( queryInterfaceId ) );

                return collectionList.get( i ).id;
            }
        }
        return 0;
    }


    // added by Datomo
    public void insertDocument() {
        String collectionName = "wohnzimmer." + this.stream.topic;
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();

        // Builder which allows to construct the algebra tree which is equivalent to query and is executed
        AlgBuilder builder = AlgBuilder.createDocumentBuilder( statement );

        // we insert document { age: 28, name: "David" } into the collection users
        BsonDocument document = new BsonDocument();
        //TODO: change to id:
        document.put( "topic", new BsonString( this.stream.topic ) );
        document.put( "content", new BsonString( this.stream.getContent() ) );

        AlgNode algNode = builder.docInsert( statement, collectionName, document ).build();

        //final AlgDataType rowType = algNode.getExpectedInputRowType(0);
        List<Integer> columnNumber = new ArrayList<>();
        columnNumber.add( 0, 1 );
        columnNumber.add( 1, 2 );

        List<String> columnNames = new ArrayList<>();
        columnNames.add( 0, "id" );
        columnNames.add( 1, "content" );

        final List<Pair<Integer, String>> fields = Pair.zip( columnNumber, columnNames );
        final AlgCollation collation =
                algNode instanceof Sort
                        ? ((Sort) algNode).collation
                        : AlgCollations.EMPTY;
        AlgRoot root = new AlgRoot( algNode, algNode.getRowType(), Kind.INSERT, fields, collation );

        // we can then wrap the tree in an AlgRoot and execute it
        // AlgRoot root = AlgRoot.of( algNode, algNode.getRowType(), Kind.INSERT );
        // for inserts and all DML queries only a number is returned
        String res = executeAndTransformPolyAlg( root, statement, statement.getPrepareContext() );


    }


    // added by Datomo
    public void scanDocument() {
        String collectionName = "users";
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();

        // Builder which allows to construct the algebra tree which is equivalent to query and is executed
        AlgBuilder builder = AlgBuilder.create( statement );

        AlgNode algNode = builder.docScan( statement, collectionName ).build();

        // we can then wrap the tree in an AlgRoot and execute it
        AlgRoot root = AlgRoot.of( algNode, Kind.SELECT );
        String res = executeAndTransformPolyAlg( root, statement, statement.getPrepareContext() );

    }


    boolean saveContent() {
        if ( this.stream.getNamespaceType() == NamespaceType.DOCUMENT ) {
            insertDocument();
        }
        return true;
    }


    private Transaction getTransaction() {
        try {
            return transactionManager.startTransaction( this.stream.getUserId(), this.stream.getDatabaseId(), false, "MQTT Stream" );
        } catch ( UnknownUserException | UnknownDatabaseException | UnknownSchemaException | GenericCatalogException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }


    String executeAndTransformPolyAlg( AlgRoot algRoot, Statement statement, final Context ctx ) {
        //TODO: implement

        try {
            // Prepare
            PolyImplementation result = statement.getQueryProcessor().prepareQuery( algRoot, false );
            log.debug( "AlgRoot was prepared." );

            // todo transform into desired output format
            List<List<Object>> rows = result.getRows( statement, -1 );

            statement.getTransaction().commit();
            return rows.toString();
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

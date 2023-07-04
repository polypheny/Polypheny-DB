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
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.exceptions.EntityAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.PolyphenyHomeDirManager;

@Slf4j
public class StreamCapture {

    TransactionManager transactionManager;
    PolyphenyHomeDirManager homeDirManager;
    PolyStream stream;


    StreamCapture( final TransactionManager transactionManager, PolyStream stream ) {
        this.transactionManager = transactionManager;
        this.stream = stream;
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        this.stream.setUserId( statement.getPrepareContext().getCurrentUserId() );
        this.stream.setDatabaseId( statement.getPrepareContext().getDatabaseId() );
    }


    public void handleContent() {
        //String path = registerTopicFolder(topic);
        long storeId = getCollection();
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
     * @return the id of the collection that was already existing with the topic as name or that was newly created
     */
    long getCollection() {
        // TODO: schemaID ist NamespaceId ???
        Catalog catalog = Catalog.getInstance();

        // In case there is a namespace with the same name existing and is of Type Document, then that id of the existing schema will be used to create a collection.
        long schemaId = 0;

        // check for existing namespace with DOCUMENT NamespaceType:
        if ( catalog.checkIfExistsSchema( stream.getDatabaseId(), stream.getNamespace() ) ) {
            CatalogSchema schema = null;
            try {
                schema = catalog.getSchema( stream.getDatabaseId(), stream.getNamespace() );
            } catch ( UnknownSchemaException e ) {
                log.error( "The catalog seems to be corrupt, as it was impossible to retrieve an existing namespace." );
                // TODO: what to do here? Maybe get schema again?
                //Not this as Namespace already exists...OR create Namespace of Datatype Document ???
                // schemaId = catalog.addNamespace( this.namespace, databaseId, userId, NamespaceType.DOCUMENT );
                return 0;
            }
            //todo: rmv
            log.info( "E Namespace with same name" );

            assert schema != null;
            if ( schema.namespaceType == NamespaceType.DOCUMENT ) {
                //todo: rmv
                log.info( "E Namespace with type DOCUMENT" );
                //check for collection with same name //TODO: maybe change the collection name, currently collection name is the topic
                List<CatalogCollection> collectionList = catalog.getCollections( schema.id, null );
                for ( CatalogCollection collection : collectionList ) {
                    if ( collection.name.equals( stream.topic ) ) {
                        //todo: rmv
                        log.info( "E Collection with topic as name" );
                        int adapterId = AdapterManager.getInstance().getStore( this.stream.getUniqueNameOfInterface() ).getAdapterId();
                        if ( !collection.placements.contains( adapterId ) ) {
                            return collection.addPlacement( adapterId ).id;
                        } else {
                            return collection.id;
                        }
                    }
                }
                //todo: rmv
                log.info( "No Collection with topic as name" );
                return createNewCollection();

            } else {
                //todo: rmv
                log.info( "Namespace not of type DOCUMENT" );
                //TODO: Namespacetype is not of type Document -> IS this what you do:
                if ( addNewNamespace() ) {
                    return createNewCollection();
                }
                return 0;
            }
        } else if ( addNewNamespace() ) {
            //todo: rmv
            log.info( "No Namespace with choosen name" );
            log.info( "Created new Namespace" );
            return createNewCollection();
        }
        return 0;
    }


    private boolean addNewNamespace() {
        boolean methodFinished = false;
        Catalog catalog = Catalog.getInstance();
        stream.setNamespaceID( catalog.addNamespace( stream.getNamespace(), stream.getDatabaseId(), stream.getUserId(), NamespaceType.DOCUMENT ) );
        methodFinished = true;
        return methodFinished;
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

        } catch ( EntityAlreadyExistsException e ) {
            log.error( "The generation of the collection was not possible due to: " + e.getMessage() );
            return 0;
        }
        //add placement
        List<CatalogCollection> collectionList = catalog.getCollections( this.stream.getNamespaceID(), null );
        for ( int i = 0; i < collectionList.size(); i++ ) {
            if ( collectionList.get( i ).name.equals( this.stream.topic ) ) {
                int adapterId = AdapterManager.getInstance().getStore( this.stream.getUniqueNameOfInterface() ).getAdapterId();
                collectionList.set( i, collectionList.get( i ).addPlacement( adapterId ) );

                return collectionList.get( i ).id;
            }
        }
        return 0;
    }


    boolean saveContent() {
/**
 //TODO: save Message here -> Polyalgebra
 Transaction transaction = getTransaction();
 Statement statement = transaction.createStatement();
 AlgBuilder algBuilder = AlgBuilder.create( statement );
 JavaTypeFactory typeFactory = transaction.getTypeFactory();
 RexBuilder rexBuilder = new RexBuilder( typeFactory );

 PolyphenyDbCatalogReader catalogReader = statement.getTransaction().getCatalogReader();
 List<String> names = new ArrayList<>();
 //TODO: change naming maybe
 names.add( this.stream.topic );
 AlgOptTable table = catalogReader.getCollection( names );

 // Values
 AlgDataType tableRowType = table.getRowType(  );
 List<AlgDataTypeField> tableRows = tableRowType.getFieldList();

 AlgOptPlanner planner = statement.getQueryProcessor().getPlanner();
 AlgOptCluster cluster = AlgOptCluster.create( planner, rexBuilder );

 List<String> valueColumnNames = this.valuesColumnNames( insertValueRequest.values );
 List<RexNode> rexValues = this.valuesNode( statement, algBuilder, rexBuilder, insertValueRequest, tableRows, inputStreams ).get( 0 );
 algBuilder.push( LogicalValues.createOneRow( cluster ) );
 algBuilder.project( rexValues, valueColumnNames );

 // Table Modify
 AlgNode algNode = algBuilder.build();
 Modify modify = new LogicalModify(
 cluster,
 algNode.getTraitSet(),
 table,
 catalogReader,
 algNode,
 LogicalModify.Operation.INSERT,
 null,
 null,
 false
 );

 // Wrap {@link AlgNode} into a RelRoot
 final AlgDataType rowType = modify.getRowType();
 final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
 final AlgCollation collation =
 algNode instanceof Sort
 ? ((Sort) algNode).collation
 : AlgCollations.EMPTY;
 AlgRoot root = new AlgRoot( modify, rowType, Kind.INSERT, fields, collation );
 log.debug( "AlgRoot was built." );

 Context ctx = statement.getPrepareContext();
 log.info( executeAndTransformPolyAlg( root, statement, ctx ) );
 **/
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

            final Iterable<Object> iterable = result.enumerable( statement.getDataContext() );
            Iterator<Object> iterator = iterable.iterator();
            while ( iterator.hasNext() ) {
                iterator.next();
            }

            statement.getTransaction().commit();
        } catch ( Throwable e ) {
            log.error( "Error during execution of REST query", e );
            try {
                statement.getTransaction().rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Could not rollback", e );
            }
            return null;
        }
        //Pair<String, Integer> result = restResult.getResult( ctx );

        //return result.left;
        return null;
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


    public static void main( String[] args ) {

    }


}

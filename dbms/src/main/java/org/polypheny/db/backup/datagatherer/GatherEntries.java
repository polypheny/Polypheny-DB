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

package org.polypheny.db.backup.datagatherer;

import java.io.File;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.backup.BackupManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PolyphenyHomeDirManager;

@Slf4j
public class GatherEntries {
    private final TransactionManager transactionManager;
    private final List<Pair<String, String>> tablesToBeCollected;
    private final List<Pair<Long, String>> collectionsToBeCollected;
    private final List<Long> graphNamespaceIds;



    public GatherEntries( TransactionManager transactionManager, List<Pair<String, String>> tablesToBeCollected, List<Pair<Long, String>> collectionsForDataCollection, List<Long> graphNamespaceIds ) {
        this.transactionManager = transactionManager;
        this.tablesToBeCollected = tablesToBeCollected;
        this.collectionsToBeCollected = collectionsForDataCollection;
        this.graphNamespaceIds = graphNamespaceIds;
    }

    // Move data around as little as possible -> use shortest possible path
    // Stream and flush data

    // Structure for saving: 1 schemafile, 1 storagefile, 1 to many datadata file(s)

    public void start() {
        if (!tablesToBeCollected.isEmpty()){
            //go through each pair in tablesToBeCollectedList
            for ( Pair<String, String> table : tablesToBeCollected) {
                //TODO(FF): exclude default columns? no, how do you differentiate for each line if it is not a default value
                String query = String.format( "SELECT * FROM %s.%s" , table.getKey(), table.getValue() );
                executeQuery( query, DataModel.RELATIONAL, Catalog.defaultNamespaceId );
            }
            /*
            for ( String nsTableName : tablesToBeCollected ) {
                String query = "SELECT * FROM " + nsTableName;
                executeQuery( query, DataModel.RELATIONAL, Catalog.defaultNamespaceId );
            } */
        }

        if (!collectionsToBeCollected.isEmpty()){
            for ( Pair<Long, String> collection : collectionsToBeCollected ) {
                String query = String.format( "db.%s.find()", collection.getValue() );
                executeQuery( query, DataModel.DOCUMENT, collection.getKey() );
            }
        }

        if (!graphNamespaceIds.isEmpty()){
            for ( Long graphNamespaceId : graphNamespaceIds ) {
                //String query = "MATCH (n) RETURN n";
                String query = "MATCH (*) RETURN n"; //todo: result is polygraph
                executeQuery( query, DataModel.GRAPH, graphNamespaceId );
            }
        }

        log.info( "collected entry data" );
        initializeFileLocation();
        log.info( "folder was created" );

    }

    // Gather entries with select statements

    private void executeQuery( String query, DataModel dataModel, long namespaceId ) {

        log.debug( "gather entries" );
        Transaction transaction;
        Statement statement = null;
        PolyImplementation result;

        /*
        printed out results of three namespaces with little test entries
        09:49:55.957 INFO [JettyServerThreadPool-32]: [[1, Best Album Ever!, 10], [2, Pretty Decent Album..., 15], [3, Your Ears will Bleed!, 13]]
        09:50:10.578 INFO [JettyServerThreadPool-32]: [[{email:jane@example.com,name:Jane Doe,_id:6570348c4023777b64ff8be8}], [{email:jim@example.com,name:Jim Doe,_id:6570348c4023777b64ff8be9}]]
        09:50:12.880 INFO [JettyServerThreadPool-32]: [[PolyNode{id=5823e305-17bb-4fb7-bd17-2108a91acb70, properties=PolyMap(map={age=45, name=Ann, depno=13}), labels=PolyList(value=[Person])}], [PolyNode{id=e8772eff-10ab-4436-a693-9e3f2f0af6d2, properties=PolyMap(map={age=30, name=John, depno=13}), labels=PolyList(value=[Person2])}]]

         */

        switch ( dataModel ) {
            case RELATIONAL:
                try {
                    // get a transaction and a statement
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Gatherer" );
                    statement = transaction.createStatement();
                    //TODO(FF): be aware for writing into file with batches that you dont overwrite the entries already in the file (evtl you need to read the whole file again
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).batch( BackupManager.batchSize ).namespaceId( namespaceId ).build(), statement ).get( 0 );
                    ExecutedContext executedQuery1 = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( Catalog.defaultNamespaceId ).build(), statement ).get( 0 );
                    // in case of results
                    ResultIterator iter = executedQuery.getIterator();
                    while ( iter.hasMoreRows() ) {
                        // liste mit tuples
                        List<List<PolyValue>> resultsPerTable = iter.getNextBatch();
                        log.info( resultsPerTable.toString() );
                        //FIXME(FF): if this is array: [[1, PolyList(value=[PolyList(value=[PolyList(value=[PolyBigDecimal(value=111), PolyBigDecimal(value=112)]), PolyList(value=[PolyBigDecimal(value=121), PolyBigDecimal(value=122)])]), PolyList(value=[PolyList(value=[PolyBigDecimal(value=211), PolyBigDecimal(value=212)]), PolyList(value=[PolyBigDecimal(value=221), PolyBigDecimal(value=222)])])])]]
                        //value is shown correctly for tojson

                        for ( List<PolyValue> row : resultsPerTable ) {
                            for ( PolyValue polyValue : row ) {
                                String test = polyValue.serialize();
                                String jsonString = polyValue.toTypedJson();    //larger, testing easier, replace later
                                PolyValue deserialized = PolyValue.deserialize( test );
                                PolyValue deserialized2 = PolyValue.fromTypedJson( jsonString, PolyValue.class );    // gives nullpointerexception
                                int jhg=87;
                            }
                        }

                    }

                } catch ( Exception e ) {
                    throw new RuntimeException( "Error while starting transaction", e );
                }
                break;

            case DOCUMENT:
                try {
                    // get a transaction and a statement
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Gatherer" );
                    statement = transaction.createStatement();
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "mql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );

                    ResultIterator iter = executedQuery.getIterator();
                    while ( iter.hasMoreRows() ) {
                        // liste mit tuples
                        List<List<PolyValue>> resultsPerCollection = iter.getNextBatch();
                        log.info( resultsPerCollection.toString() );
                    }
                } catch ( Exception e ) {
                    throw new RuntimeException( "Error while starting transaction", e );
                }
                break;

            case GRAPH:
                try {
                    // get a transaction and a statement
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Gatherer" );
                    statement = transaction.createStatement();
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "cypher" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );

                    ResultIterator iter = executedQuery.getIterator();
                    while ( iter.hasMoreRows() ) {
                        // liste mit tuples
                        List<List<PolyValue>> graphPerNamespace = iter.getNextBatch();
                        log.info( graphPerNamespace.toString() );
                    }
                } catch ( Exception e ) {
                    throw new RuntimeException( "Error while starting transaction", e );
                }
                break;

            default:
                throw new RuntimeException( "Backup - GatherEntries: DataModel not supported" );
        }


        /*
        try {

            // get a transaction and a statement
            transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Inserter" );
            statement = transaction.createStatement();
            ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( Catalog.defaultNamespaceId ).build(), statement ).get( 0 );
            // in case of results
            int batchSize = 100;
            //ResultIterator iter = executedQuery.getIterator(statement, batchSize);
            ResultIterator iter = executedQuery.getIterator();
            while ( iter.hasMoreRows() ) {
                // liste mit tuples
                iter.getNextBatch();
            }

        } catch ( Exception e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }

         */
    }


    private static void initializeFileLocation() {
        PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();
        File applicationConfDir = null;  //todo: wär eig class field (private static)
        // String currentConfigurationDirectoryName = DEFAULT_CONFIGURATION_DIRECTORY_NAME;    //static string in class field
        String currentConfigurationDirectoryName = "backup";
        String currentConfigurationFileName = "backup.bu";
        File applicationConfFile = homeDirManager.registerNewFolder( currentConfigurationDirectoryName );   //there is complicated thing in ConfigManager>loadConfigFile()
        // Create config directory and file if they do not already exist
        //PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();
        if ( applicationConfDir == null ) {
            applicationConfDir = homeDirManager.registerNewFolder( currentConfigurationDirectoryName );
        } else {
            applicationConfDir = homeDirManager.registerNewFolder( applicationConfDir.getParentFile(), currentConfigurationDirectoryName );
        }
        applicationConfFile = homeDirManager.registerNewFile( applicationConfDir, currentConfigurationFileName );
    }

}

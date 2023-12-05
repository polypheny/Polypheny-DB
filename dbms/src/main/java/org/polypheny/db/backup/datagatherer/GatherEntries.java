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

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
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

@Slf4j
public class GatherEntries {
    private final TransactionManager transactionManager;
    private final List<String> tablesToBeCollected;
    private final List<Pair<Long, String>> collectionsToBeCollected;
    private final List<Long> graphNamespaceIds;


    public GatherEntries( TransactionManager transactionManager, List<String> tablesToBeCollected, List<Pair<Long, String>> collectionsForDataCollection, List<Long> graphNamespaceIds ) {
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
            for ( String nsTableName : tablesToBeCollected ) {
                //TODO(FF): exclude default columns? no, how do you differentiate for each line if it is not a default value
                String query = "SELECT * FROM " + nsTableName;
                executeQuery( query, DataModel.RELATIONAL, Catalog.defaultNamespaceId );
            }
        }

        if (!collectionsToBeCollected.isEmpty()){
            for ( Pair<Long, String> collection : collectionsToBeCollected ) {
                String query = String.format( "db.%s.find()", collection.getValue() );
                executeQuery( query, DataModel.DOCUMENT, collection.getKey() );
            }
        }

        if (!graphNamespaceIds.isEmpty()){
            for ( Long graphNamespaceId : graphNamespaceIds ) {
                String query = "MATCH (n) RETURN n";
                executeQuery( query, DataModel.GRAPH, graphNamespaceId );
            }
        }

    }

    // Gather entries with select statements

    private void executeQuery( String query, DataModel dataModel, long namespaceId ) {

        log.debug( "gather entries" );
        Transaction transaction;
        Statement statement = null;
        PolyImplementation result;

        switch ( dataModel ) {
            case RELATIONAL:
                try {
                    // get a transaction and a statement
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Gatherer" );
                    statement = transaction.createStatement();
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );
                    //TODO(FF): is the list here when there are subqueries? or what was the list for again?
                    List<ExecutedContext> executedQuery1 = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( Catalog.defaultNamespaceId ).build(), statement );
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
                                String jsonString = polyValue.toJson();
                                PolyValue deserialized = PolyValue.deserialize( test );
                                //PolyValue deserialized2 = PolyValue.deserialize( jsonString );    // gives nullpointerexception
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

            //TODO(FF): fix rest of data collection (just copied, nothing done yet)
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

}

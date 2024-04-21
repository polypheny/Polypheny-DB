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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.backup.BackupManager;
import org.polypheny.db.backup.datasaver.BackupFileWriter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyGraph;

/**
 * This class is a task that is executed by a thread. It gathers the entries from the database from one entity and writes them to a file.
 */
@Slf4j
public class GatherEntriesTask implements Runnable {

    private TransactionManager transactionManager;
    private String query;
    private DataModel dataModel;
    private long namespaceId;
    private File dataFile;


    /**
     * Creates a new GatherEntriesTask, which gathers the entries from the database from one entity and writes them to a file (one file per entity)
     * @param transactionManager TransactionManager to use
     * @param query gather query to execute
     * @param dataModel DataModel of the entity where the entry data belongs to
     * @param namespaceId Id of the namespace of the entity
     * @param dataFile File to write the entries to
     */
    public GatherEntriesTask( TransactionManager transactionManager, String query, DataModel dataModel, long namespaceId, File dataFile ) {
        this.transactionManager = transactionManager;   //TODO(FF): is transactionmanager thread safe to pass it like this??
        this.query = query;
        this.dataModel = dataModel;
        this.namespaceId = namespaceId;
        this.dataFile = dataFile;
    }


    /**
     * Runs the task, gathers the entries from the database from one entity and writes them to a file
     */
    @Override
    public void run() {
        log.info( "thread for gather entries entered with query" + query );
        Transaction transaction;
        Statement statement = null;
        PolyImplementation result;

        switch ( dataModel ) {
            case RELATIONAL:
                try (
                        //DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataFile), 32768));
                        PrintWriter pOut = new PrintWriter( new DataOutputStream( new BufferedOutputStream( new FileOutputStream( dataFile ), 32768 ) ) );
                        //BufferedWriter bOut = new BufferedWriter( new OutputStreamWriter( new BufferedOutputStream( new FileOutputStream( dataFile ), 32768 ) ) );
                        //DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(dataFile)));
                        //String result = in.readUTF();
                        //in.close();

                ) {
                    BackupFileWriter out = new BackupFileWriter( dataFile );

                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Gatherer" );
                    statement = transaction.createStatement();
                    log.warn( "Batch size gather: " + BackupManager.batchSize );
                    //TODO(FF): be aware for writing into file with batches that you dont overwrite the entries already in the file (evtl you need to read the whole file again
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).batch( BackupManager.batchSize ).namespaceId( namespaceId ).transactions( new ArrayList<>( List.of( transaction ) ) ).statement( statement ).build() ).get( 0 );
                    //ExecutedContext executedQuery1 = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( Catalog.defaultNamespaceId ).build(), statement ).get( 0 );
                    // in case of results
                    ResultIterator iter = executedQuery.getIterator();

                    //batch repeats here
                    while ( iter.hasMoreRows() ) {
                        // liste mit tuples
                        List<List<PolyValue>> resultsPerTable = iter.getNextBatch();
                        log.info( resultsPerTable.toString() );
                        //FIXME(FF): if this is array: [[1, PolyList(value=[PolyList(value=[PolyList(value=[PolyBigDecimal(value=111), PolyBigDecimal(value=112)]), PolyList(value=[PolyBigDecimal(value=121), PolyBigDecimal(value=122)])]), PolyList(value=[PolyList(value=[PolyBigDecimal(value=211), PolyBigDecimal(value=212)]), PolyList(value=[PolyBigDecimal(value=221), PolyBigDecimal(value=222)])])])]]
                        //value is shown correctly for tojson

                        for ( List<PolyValue> row : resultsPerTable ) {
                            for ( PolyValue polyValue : row ) {
                                String byteString = polyValue.serialize();
                                byte[] byteBytes = polyValue.serialize().getBytes( StandardCharsets.UTF_8 );
                                String jsonString = polyValue.toTypedJson();

                                //out.write( byteBytes );
                                //out.write( byteString.getBytes( StandardCharsets.UTF_8 ) );
                                //out.writeChars( jsonString );
                                //pOut.println( jsonString );
                                out.write( jsonString );
                                //out.write( byteString );
                                out.newLine();

                                //larger, testing easier, replace later
                                PolyValue deserialized = PolyValue.deserialize( byteString );
                                PolyValue deserialized2 = PolyValue.fromTypedJson( jsonString, PolyValue.class );
                            }
                        }

                    }
                    out.flush();
                    out.close();
                    transaction.commit();

                } catch ( Exception e ) {
                    throw new GenericRuntimeException( "Error while collecting entries: " + e.getMessage() );
                } catch ( TransactionException e ) {
                    throw new GenericRuntimeException( "Error while collecting entries: " + e.getMessage() );
                }

                break;

            case DOCUMENT:
                try (
                        //DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataFile), 32768));
                        PrintWriter pOut = new PrintWriter( new DataOutputStream( new BufferedOutputStream( new FileOutputStream( dataFile ), 32768 ) ) );
                        //BufferedWriter bOut = new BufferedWriter( new OutputStreamWriter( new BufferedOutputStream( new FileOutputStream( dataFile ), 32768 ) ) );
                ) {
                    BackupFileWriter out = new BackupFileWriter( dataFile );
                    // get a transaction and a statement
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Gatherer" );
                    statement = transaction.createStatement();
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE()
                            .anyQuery(
                                    QueryContext.builder()
                                            .language( QueryLanguage.from( "mql" ) )
                                            .query( query ).origin( "Backup Manager" )
                                            .transactionManager( transactionManager )
                                            .batch( BackupManager.batchSize )
                                            .namespaceId( namespaceId )
                                            .statement( statement )
                                            .build()
                                            .addTransaction( transaction ) ).get( 0 );
                    //ExecutedContext executedQuery1 = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "mql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );

                    ResultIterator iter = executedQuery.getIterator();
                    while ( iter.hasMoreRows() ) {
                        // list with tuples
                        List<List<PolyValue>> resultsPerCollection = iter.getNextBatch();
                        for ( List<PolyValue> entry : resultsPerCollection ) {
                            for ( PolyValue polyValue : entry ) {
                                String byteString = polyValue.serialize();
                                byte[] byteBytes = polyValue.serialize().getBytes( StandardCharsets.UTF_8 );
                                String jsonString = polyValue.toTypedJson();

                                //out.write( byteBytes );
                                //out.write( byteString.getBytes( StandardCharsets.UTF_8 ) );
                                //pOut.println( jsonString);
                                out.write( jsonString );
                                //bOut.write( byteString );
                                out.newLine();
                                //out.writeChars( jsonString );
                            }
                        }

                        //out.writeChars( resultsPerCollection.toString() );
                        log.info( resultsPerCollection.toString() );
                    }
                    out.flush();
                    out.close();
                    log.info( "end of thread reached: case document" );
                    transaction.commit();
                } catch ( Exception e ) {
                    throw new GenericRuntimeException( "Error while collecting entries: " + e.getMessage() );
                } catch ( TransactionException e ) {
                    throw new GenericRuntimeException( "Error while collecting entries: " + e.getMessage() );
                }
                break;

            case GRAPH:
                try (
                        DataOutputStream ouuut = new DataOutputStream( new BufferedOutputStream( new FileOutputStream( dataFile ), 32768 ) );
                        //BufferedWriter bOut = new BufferedWriter( new OutputStreamWriter( new BufferedOutputStream( new FileOutputStream( dataFile ), 32768 ) ) );
                ) {
                    BackupFileWriter out = new BackupFileWriter( dataFile );
                    // get a transaction and a statement
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Gatherer" );
                    statement = transaction.createStatement();
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE()
                            .anyQuery(
                                    QueryContext.builder()
                                            .language( QueryLanguage.from( "cypher" ) )
                                            .query( query )
                                            .origin( "Backup Manager" )
                                            .transactionManager( transactionManager )
                                            .batch( BackupManager.batchSize )
                                            .namespaceId( namespaceId )
                                            .statement( statement )
                                            .build()
                                            .addTransaction( transaction ) ).get( 0 );
                    //ExecutedContext executedQuery1 = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "cypher" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );

                    ResultIterator iter = executedQuery.getIterator();
                    while ( iter.hasMoreRows() ) {
                        // liste mit tuples
                        List<List<PolyValue>> graphPerNamespace = iter.getNextBatch();
                        for ( List<PolyValue> entry : graphPerNamespace ) {
                            for ( PolyValue polyValue : entry ) {

                                String byteString = polyValue.serialize();
                                byte[] byteBytes = polyValue.serialize().getBytes( StandardCharsets.UTF_8 );
                                String jsonString = polyValue.toTypedJson();

                                //out.write( byteBytes );
                                //out.write( byteString.getBytes( StandardCharsets.UTF_8 ) );
                                //pOut.println( jsonString);
                                out.write( jsonString );
                                //bOut.write( byteString );
                                out.newLine();
                                //out.writeChars( jsonString );
                            }
                        }
                        log.info( graphPerNamespace.toString() );
                    }
                    out.flush();
                    out.close();
                    log.info( "end of thread reached: case graph" );
                    transaction.commit();
                } catch ( Exception e ) {
                    throw new GenericRuntimeException( "Error while collecting entries: " + e.getMessage() );
                } catch ( TransactionException e ) {
                    throw new GenericRuntimeException( "Error while collecting entries: " + e.getMessage() );
                }
                break;

            default:
                throw new GenericRuntimeException( "Backup - GatherEntries: DataModel not supported" );
        }
        log.info( "end of thread reached - completely done" );
        statement.close();

    }


}

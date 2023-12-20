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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
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
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.relational.PolyMap;

@Slf4j
public class GatherEntriesTask implements Runnable {
    private TransactionManager transactionManager;
    private String query;
    private DataModel dataModel;
    private long namespaceId;
    private File dataFile;

    public GatherEntriesTask( TransactionManager transactionManager, String query, DataModel dataModel, long namespaceId, File dataFile ) {
        this.transactionManager = transactionManager;   //TODO(FF): is transactionmanager thread safe to pass it like this??
        this.query = query;
        this.dataModel = dataModel;
        this.namespaceId = namespaceId;
        this.dataFile = dataFile;
    }


    @Override
    public void run() {
        log.info( "thread for gather entries entered with query" + query );
        Transaction transaction;
        Statement statement = null;
        PolyImplementation result;


        switch ( dataModel ) {
            case RELATIONAL:
                //fileChannel (is blocking... does it matter?) or
                // DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(dataFile)));

                /*
                //fileChannel way (randomaccessfile, nio)
                try(
                        //DataOutputStream out = new DataOutputStream( new BufferedOutputStream( new FileOutputStream( dataFile ) ) );  //channel doesn't work with this
                        RandomAccessFile writer = new RandomAccessFile( dataFile, "rw" );
                        FileChannel channel = writer.getChannel();

                        //method2
                        FileOutputStream fos = new FileOutputStream( dataFile );
                        FileChannel channel1 = fos.getChannel();

                    ) {

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
                                String byteString = polyValue.serialize();
                                //byte[] byteString2 = polyValue.serialize().getBytes(StandardCharsets.UTF_8);
                                String jsonString = polyValue.toTypedJson();

                                ByteBuffer buff = ByteBuffer.wrap(byteString.getBytes( StandardCharsets.UTF_8));
                                channel.write( buff );


                                //larger, testing easier, replace later
                                PolyValue deserialized = PolyValue.deserialize( byteString );
                                PolyValue deserialized2 = PolyValue.fromTypedJson( jsonString, PolyValue.class );
                                int jhg=87;
                            }
                        }

                        // flush only batchwise? is this even possible? does it make sense?

                    }

                } catch(Exception e){
                    throw new GenericRuntimeException( "Error while starting transaction", e );
                }

                 */


                /*
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
                    throw new GenericRuntimeException( "Error while starting transaction", e );
                }

                 */

                // bufferedOutputStream, io way
                try(
                        //DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataFile), 32768));
                        PrintWriter pOut = new PrintWriter( new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataFile), 32768)));

                        //BufferedWriter bOut = new BufferedWriter( new OutputStreamWriter( new BufferedOutputStream( new FileOutputStream( dataFile ), 32768 ) ) );



                        //DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(dataFile)));

                        //String result = in.readUTF();
                        //in.close();

                ) {
                    BackupFileWriter out = new BackupFileWriter( dataFile );

                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Gatherer" );
                    statement = transaction.createStatement();
                    //TODO(FF): be aware for writing into file with batches that you dont overwrite the entries already in the file (evtl you need to read the whole file again
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).batch( BackupManager.batchSize ).namespaceId( namespaceId ).build(), statement ).get( 0 );
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
                                byte[] byteBytes = polyValue.serialize().getBytes(StandardCharsets.UTF_8);
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
                                int jhg=87;
                            }
                        }
                        // flush only batchwise? is this even possible? does it make sense?

                    }
                    out.flush();
                    out.close();
                    transaction.commit();

                } catch(Exception e){
                    throw new GenericRuntimeException( "Error while collecting entries: " + e.getMessage() );
                } catch ( TransactionException e ) {
                    throw new GenericRuntimeException( "Error while collecting entries: " + e.getMessage() );
                }

                break;

            case DOCUMENT:
                try(
                        //DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataFile), 32768));
                        PrintWriter pOut = new PrintWriter( new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataFile), 32768)));
                        //BufferedWriter bOut = new BufferedWriter( new OutputStreamWriter( new BufferedOutputStream( new FileOutputStream( dataFile ), 32768 ) ) );
                )
                {
                    BackupFileWriter out = new BackupFileWriter( dataFile );
                    // get a transaction and a statement
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Gatherer" );
                    statement = transaction.createStatement();
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "mql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).batch( BackupManager.batchSize ).namespaceId( namespaceId ).build(), statement ).get( 0 );
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
                                out.write( jsonString);
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
                try(
                        DataOutputStream ouuut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataFile), 32768));
                        //BufferedWriter bOut = new BufferedWriter( new OutputStreamWriter( new BufferedOutputStream( new FileOutputStream( dataFile ), 32768 ) ) );
                    )
                {
                    BackupFileWriter out = new BackupFileWriter( dataFile );
                    // get a transaction and a statement
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Gatherer" );
                    statement = transaction.createStatement();
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "cypher" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).batch( BackupManager.batchSize ).namespaceId( namespaceId ).build(), statement ).get( 0 );
                    //ExecutedContext executedQuery1 = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "cypher" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );

                    ResultIterator iter = executedQuery.getIterator();
                    while ( iter.hasMoreRows() ) {
                        // liste mit tuples
                        List<List<PolyValue>> graphPerNamespace = iter.getNextBatch();
                        for ( List<PolyValue> entry : graphPerNamespace ) {
                            for ( PolyValue polyValue : entry ) {
                                PolyGraph polyGraph = polyValue.asGraph();

                                /*
                                PolyMap<PolyString, PolyNode> nodes = polyGraph.getNodes();
                                PolyMap<PolyString, PolyEdge> edges = polyGraph.getEdges();
                                String edgejson = edges.get( 0 ).toTypedJson();
                                String nodejson = nodes.get( 0 ).toTypedJson();
                                String edgeByte = edges.get( 0 ).serialize();
                                String nodeByte = nodes.get( 0 ).serialize();

                                PolyNode node = PolyNode.fromTypedJson( nodejson, PolyNode.class );
                                PolyEdge edge = PolyNode.fromTypedJson( edgejson, PolyEdge.class );
                                PolyValue node1 = PolyNode.deserialize( nodeByte );
                                PolyValue edge1 = PolyEdge.deserialize( edgeByte );
                                 */


                                String typedJson = polyValue.asGraph().toTypedJson();
                                //String bytestr = polyValue.asGraph().serialize();   //not implemented exception

                                //PolyValue dd = PolyGraph.deserialize( bytestr );  //get exception
                                PolyValue aa = PolyGraph.fromTypedJson( typedJson, PolyGraph.class );   //is null
                                PolyValue aaa = PolyGraph.fromTypedJson( typedJson, PolyValue.class );  //au null

                                String jsonString = polyValue.toTypedJson();
                                PolyValue test = PolyValue.fromTypedJson( jsonString, PolyValue.class );

                                String byteString = polyValue.serialize();  //not implemented exception
                                PolyValue haha = PolyValue.deserialize( byteString );
                                byte[] byteBytes = polyValue.serialize().getBytes( StandardCharsets.UTF_8 );


                                //out.write( byteBytes );
                                //out.write( byteString.getBytes( StandardCharsets.UTF_8 ) );
                                //pOut.println( jsonString);
                                out.write( jsonString);
                                //out.write( byteString );
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

    private void createFile(String path) {

    }

}

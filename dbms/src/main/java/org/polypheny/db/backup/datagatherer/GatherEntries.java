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
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.backup.BackupManager;
import org.polypheny.db.backup.datasaver.manifest.BackupManifestGenerator;
import org.polypheny.db.backup.datasaver.manifest.EntityInfo;
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
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.Triple;

@Slf4j
public class GatherEntries {
    private final TransactionManager transactionManager;
    private final List<Triple<Long, String, String>> tablesToBeCollected;
    private final List<Triple<Long, String, String>> collectionsToBeCollected;
    private final List<Long> graphNamespaceIds;
    //private final int = hal.getProcessor().getPhysicalProcessorCount();

    @Getter
    private File backupFolder = null;
    @Getter
    private File dataFolder = null;
    PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();



    public GatherEntries( TransactionManager transactionManager, List<Triple<Long, String, String>> tablesToBeCollected, List<Triple<Long, String, String>> collectionsForDataCollection, List<Long> graphNamespaceIds ) {
        this.transactionManager = transactionManager;
        this.tablesToBeCollected = tablesToBeCollected;
        this.collectionsToBeCollected = collectionsForDataCollection;
        this.graphNamespaceIds = graphNamespaceIds;
    }

    // Move data around as little as possible -> use shortest possible path
    // Stream and flush data

    // Structure for saving: 1 schemafile, 1 storagefile, 1 to many datadata file(s)

    public void start() {
        ExecutorService executorService = null;
        List<EntityInfo> entityInfoList = new ArrayList<>();
        try {
            executorService = Executors.newFixedThreadPool( BackupManager.threadNumber );
            //initFileTest();
            //PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();
            backupFolder = homeDirManager.registerNewFolder( "backup" );
            dataFolder = homeDirManager.registerNewFolder( backupFolder, "data" );
            String dataFolderPath = "backup/data";

            if ( !tablesToBeCollected.isEmpty() ) {
                //go through each pair in tablesToBeCollectedList
                for ( Triple<Long, String, String> table : tablesToBeCollected ) {
                    List<String> filePaths = new ArrayList<>();
                    //int nbrCols = BackupManager.getNumberColumns(table.getLeft(), table.getRight());
                    int nbrCols = BackupManager.getINSTANCE().getNumberColumns( table.getLeft(), table.getRight() );
                    //TODO(FF): exclude default columns? no, how do you differentiate for each line if it is not a default value
                    String query = String.format( "SELECT * FROM %s.%s", table.getMiddle(), table.getRight() );
                    //executeQuery2( query, DataModel.RELATIONAL, Catalog.defaultNamespaceId );

                    String fileName = String.format( "tab_%s_%s.txt", table.getMiddle(), table.getRight() );
                    File tableData = homeDirManager.registerNewFile( getDataFolder(), fileName );
                    filePaths.add( String.format( "%s/%s", dataFolderPath, fileName ) );
                    EntityInfo entityInfo = new EntityInfo( filePaths, table.getRight(), table.getMiddle(), table.getLeft(), DataModel.RELATIONAL, nbrCols );
                    entityInfoList.add( entityInfo );
                    executorService.submit( new GatherEntriesTask( transactionManager, query, DataModel.RELATIONAL, Catalog.defaultNamespaceId, tableData ) );
                }
            /*
            for ( String nsTableName : tablesToBeCollected ) {
                String query = "SELECT * FROM " + nsTableName;
                executeQuery( query, DataModel.RELATIONAL, Catalog.defaultNamespaceId );
            } */
            }

            if ( !collectionsToBeCollected.isEmpty() ) {
                for ( Triple<Long, String, String> collection : collectionsToBeCollected ) {
                    List<String> filePaths = new ArrayList<>();
                    String query = String.format( "db.%s.find()", collection.getRight() );
                    //executeQuery2( query, DataModel.DOCUMENT, collection.getKey() );

                    String fileName = String.format( "col_%s.txt", collection.getRight() );
                    File collectionData = homeDirManager.registerNewFile( getDataFolder(), fileName );
                    filePaths.add( String.format( "%s/%s", dataFolderPath, fileName ) );
                    EntityInfo entityInfo = new EntityInfo( filePaths, collection.getRight(), collection.getMiddle(), collection.getLeft(), DataModel.DOCUMENT );
                    entityInfoList.add( entityInfo );
                    executorService.submit( new GatherEntriesTask( transactionManager, query, DataModel.DOCUMENT, collection.getLeft(), collectionData ) );
                }
            }

            if ( !graphNamespaceIds.isEmpty() ) {
                for ( Long graphNamespaceId : graphNamespaceIds ) {
                    List<String> filePaths = new ArrayList<>();
                    String query = "MATCH (*) RETURN n";
                    //executeQuery2( query, DataModel.GRAPH, graphNamespaceId );

                    String fileName = String.format( "graph_%s.txt", graphNamespaceId.toString() );
                    File graphData = homeDirManager.registerNewFile( getDataFolder(), fileName );
                    EntityInfo entityInfo = new EntityInfo( filePaths, "graph", "graph", graphNamespaceId, DataModel.GRAPH );
                    entityInfoList.add( entityInfo );
                    executorService.submit( new GatherEntriesTask( transactionManager, query, DataModel.GRAPH, graphNamespaceId, graphData ) );
                }
            }

            log.info( "collected entry data" );
            //initializeFileLocation();
            executorService.shutdown();
            //executorService.awaitTermination(10, TimeUnit.SECONDS);
            log.info( "executor service was shut down" );

        } catch ( Exception e ) {
            throw new GenericRuntimeException( "An error occured during threadpooling to collect the data: " + e.getMessage() );
        }
        /*
        finally {
            if ( Objects.nonNull( executorService ) && !executorService.isTerminated() ) {
                log.error( "cancelling all non-finished tasks" );
            }
            if ( Objects.nonNull( executorService ) ) {
                //executorService.shutdownNow();
                log.info( "shutdown finished" );
            }
        }

         */

        try {
            Calendar calendar = Calendar.getInstance();
            Date currentDate = calendar.getTime();
            //TODO(FF): calculate checksum
            File manifestFile = homeDirManager.registerNewFile( getBackupFolder(), "manifest.txt" );
            BackupManifestGenerator.generateManifest( entityInfoList, "", manifestFile, currentDate );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Could not create manifest for backup" + e.getMessage() );
        }


    }

    // Gather entries with select statements

    private void executeQuery2( String query, DataModel dataModel, long namespaceId ) {

        log.debug( "gather entries" );
        Transaction transaction;
        Statement statement = null;
        PolyImplementation result;

        /*
        printed out results of three namespaces with little test entries
        09:49:55.957 INFO [JettyServerThreadPool-32]: [[1, Best Album Ever!, 10], [2, Pretty Decent Album..., 15], [3, Your Ears will Bleed!, 13]]
        09:50:10.578 INFO [JettyServerThreadPool-32]: [[{email:jane@example.com,name:Jane Doe,_id:6570348c4023777b64ff8be8}], [{email:jim@example.com,name:Jim Doe,_id:6570348c4023777b64ff8be9}]]
        09:50:12.880 INFO [JettyServerThreadPool-32]: [[PolyNode{id=5823e305-17bb-4fb7-bd17-2108a91acb70, properties=PolyMap(map={age=45, name=Ann, depno=13}), labels=PolyList(value=[Person])}], [PolyNode{id=e8772eff-10ab-4436-a693-9e3f2f0af6d2, properties=PolyMap(map={age=30, name=John, depno=13}), labels=PolyList(value=[Person2])}]]


        if the batchsize is 1: this is printed
        17:32:48.142 INFO [JettyServerThreadPool-418]: [[1, Best Album Ever!, 10]]
        17:33:53.853 INFO [JettyServerThreadPool-418]: [[2, Pretty Decent Album..., 15]]
        17:33:53.858 INFO [JettyServerThreadPool-418]: [[3, Your Ears will Bleed!, 13]]
         */

        switch ( dataModel ) {
            case RELATIONAL:
                try {
                    // get a transaction and a statement
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Gatherer" );
                    statement = transaction.createStatement();
                    //TODO(FF): be aware for writing into file with batches that you dont overwrite the entries already in the file (evtl you need to read the whole file again
                    //ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).batch( BackupManager.batchSize ).namespaceId( namespaceId ).build(), statement ).get( 0 );
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );
                    //ExecutedContext executedQuery1 = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( Catalog.defaultNamespaceId ).build(), statement ).get( 0 );
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
                    //iter.close();
                    transaction.commit();

                } catch ( Exception e ) {
                    throw new GenericRuntimeException( "Error while starting transaction: " + e.getMessage() );
                } catch ( TransactionException e ) {
                    throw new GenericRuntimeException( "Error while starting transaction: " + e.getMessage() );
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
                    throw new GenericRuntimeException( "Error while starting transaction: "+ e.getMessage() );
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
                    throw new GenericRuntimeException( "Error while starting transaction: " + e.getMessage() );
                }
                break;

            default:
                throw new RuntimeException( "Backup - GatherEntries: DataModel not supported" );
        }
        statement.close();

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

    private void initFileTest() {
        // this creates (only folders): dataa>cottontaildb-store>store23>dataIntern
        PolyphenyHomeDirManager fileSystemManager = PolyphenyHomeDirManager.getInstance();
        File adapterRoot = fileSystemManager.registerNewFolder( "dataaa/cottontaildb-store" );

        File embeddedDir = fileSystemManager.registerNewFolder( adapterRoot, "store" + 23 );
        File testFolder = fileSystemManager.registerNewFolder( "dataaa/cottontaildb-store/test" );  // works too

        final File dataFolder = fileSystemManager.registerNewFolder( embeddedDir, "dataIntern" );

    }


    private void initializeFileLocation() {
        PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();
        File folder = null;  //todo: wär eig class field (private static)
        // String folderName = DEFAULT_CONFIGURATION_DIRECTORY_NAME;    //static string in class field
        Date date = new java.util.Date();
        String datum = date.toString();
        String folderName = "backup";
        String fileName = "backup.txt";
        File file = homeDirManager.registerNewFolder( folderName );   //there is complicated thing in ConfigManager>loadConfigFile()
        // Create config directory and file if they do not already exist
        //PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();


        if ( folder == null ) {
            folder = homeDirManager.registerNewFolder( folderName );
        } else {
            folder = homeDirManager.registerNewFolder( folder.getParentFile(), folderName );
        }
        file = homeDirManager.registerNewFile( folder, fileName );





        // For a large amount of data, we will require a better raw performance.
        // In this case, buffered methods like BufferedWriter and Files.write() can offer improved efficiency.
        // Use FileChannel to write larger files. It is the preferred way of writing files in Java 8 as well.
        // https://howtodoinjava.com/java/io/java-write-to-file/




        // this is apparently also an option;
        try {
            String str = "lisjlk";
            byte[] strToBytes = str.getBytes();

            Files.write( file.toPath(), strToBytes);

            String read = Files.readAllLines(file.toPath()).get(0);
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }

        // apparently this is slower, no buffered is okee
        try {
            BufferedWriter writer = new BufferedWriter( new FileWriter( file ), 32768 );
            writer.write( "test2" );
            writer.close();
        } catch ( IOException e ) {
            throw new GenericRuntimeException( e );
        }


        //this is apparently faster, same as above (??) but for raw data
        try {
            //bufferedOutputStream??
            FileInputStream fis = new FileInputStream(new File("in.txt"));
            FileOutputStream fos = new FileOutputStream(new File("out.txt"));
            byte[] buffer = new byte[1024];
            int len;
            while((len = fis.read(buffer)) != -1){
                fos.write(buffer, 0, len);
            }
            fos.close();
            fis.close();
        } catch ( IOException e ) {
            throw new GenericRuntimeException( e );
        }

        //apparently this is superfast? https://stackoverflow.com/questions/8109762/dataoutputstream-vs-dataoutputstreamnew-bufferedoutputstream
        try {
            File dataFile = null;
            FileOutputStream fos = new FileOutputStream(dataFile);
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fos));
            DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(dataFile)));

            String str = "hello world";
            byte[] strToBytes = str.getBytes();
            out.write( strToBytes );


            out.writeUTF("test");
            out.close();
            String result = in.readUTF();
            String result2 = in.readAllBytes().toString();
            byte[] byteResult = new byte[100];
            in.readFully( byteResult ); //reads bytes from an input stream and allocates those into the buffer array b.
            in.close();

        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }


        //https://stackoverflow.com/questions/1605332/java-nio-filechannel-versus-fileoutputstream-performance-usefulness
        //randomAccessFile: lets you start writing from specific point in file (after byte offset)... filechannel is from nio, apparently faster with large amount of data (+buffer)
        try {
            RandomAccessFile stream = new RandomAccessFile(fileName, "rw");
            FileChannel channel = stream.getChannel();
            String value = "Hello";
            byte[] strBytes = value.getBytes();
            ByteBuffer buffer = ByteBuffer.allocate(strBytes.length);
            buffer.put(strBytes);
            buffer.flip();
            channel.write(buffer);
            stream.close();
            channel.close();

            // verify
            RandomAccessFile reader = new RandomAccessFile(fileName, "r");
            //assertEquals(value, reader.readLine());
            reader.close();
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }


    }


    private void speedTest() {
        char[] chars = new char[100*1024*1024];
        Arrays.fill(chars, 'A');
        String text = new String(chars);
        long start = System.nanoTime();

        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("/tmp/a.txt"));
            bw.write(text);
            bw.close();
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

        long time = System.nanoTime() - start;
        log.info("Wrote " + chars.length*1000L/time+" MB/s.");
        // stackoverflow dude: Wrote 135 MB/s
    }


    //from: https://www.devinline.com/2013/09/write-to-file-in-java.html
    // With buffered input:- Read input file and write to output file
    /*
    public static void writeBinaryStreamEfficient(File outputFile, File inputFile) {
        int byteCoint;
        Long starttime = System.currentTimeMillis();
        try {
            FileInputStream is = new FileInputStream(inputFile);
            // Buffered input stream and loop over buffered result
            BufferedInputStream bis = new BufferedInputStream(is);

            FileOutputStream os = new FileOutputStream(outputFile);
            BufferedOutputStream bos = new BufferedOutputStream(os);
            while ((byteCoint = bis.read()) != -1) {
                bos.write(byteCoint);
            }

//Closes this file input/output stream and releases any system resources associated with the stream.
            is.close();
            os.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Total time spent in writing "
                + "with buffered input is (in millisec) "
                + (System.currentTimeMillis() - starttime));

    }

     */

}

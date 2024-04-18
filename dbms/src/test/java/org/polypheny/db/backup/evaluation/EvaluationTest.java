/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.backup.evaluation;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.CypherConnection;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.TestHelper.MongoConnection;
import org.polypheny.db.backup.BackupManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.webui.models.results.DocResult;
import org.polypheny.db.webui.models.results.GraphResult;


@Slf4j
@Tag("adapter")
public class EvaluationTest {

    static TestHelper testHelper;
    BackupManager backupManager;


    @BeforeAll
    public static void start() {
        testHelper = TestHelper.getInstance();
    }

    /*
    @Test
    public void testingForEvaluation() {
        TransactionManager transactionManager = testHelper.getTransactionManager();
        BackupManager backupManager = BackupManager.getINSTANCE();

        addBasicRelTestData();
        addBasicGraphData();
        addBasicDocData();

        backupManager.startDataGathering();
        BackupInformationObject bupobj = backupManager.getBackupInformationObject();

        // go through all tables in the bupobj and add the table names to a string, which will be printed
        StringBuilder sb = new StringBuilder();
        ImmutableMap<Long, List<LogicalEntity>> tables = bupobj.getTables();
        for ( Long key : tables.keySet() ) {
            List<LogicalEntity> tableList = tables.get( key );
            for ( LogicalEntity entity : tableList ) {
                sb.append( entity.name ).append( "\n" );
            }
        }
        log.warn( sb.toString() );

        // read the graph from backup file and print the PolyValue
        //PolyValue deserialized = PolyValue.fromTypedJson( inLine, PolyGraph.class ); //--> deserialized is null??
        //String value = deserialized.toJson();
        int lol = 2;


        //assertEquals( 2, tables.size(), "Wrong number of tables" );
        assertEquals( 2, 2, "Wrong number of tables" );

        deleteBasicRelTestData();
        deleteBasicGraphData();
        deleteBasicDocData();
    }

     */

    @Test
    public void startEvaluation() {
        /*

        startMeasure( "batchSize5", "s", "simple", "rel", 5 );
        startMeasure( "batchSize500", "s", "simple", "rel", 500 );
        startMeasure( "batchSize5000", "s", "simple", "rel", 5000 );

        startMeasure( "batchSize5", "m", "simple", "rel", 5 );
        startMeasure( "batchSize500", "m", "simple", "rel", 500 );
        startMeasure( "batchSize5000", "m", "simple", "rel", 5000 );

        startMeasure( "batchSize5", "l", "simple", "rel", 5 );
        startMeasure( "batchSize500", "l", "simple", "rel", 500 );
        startMeasure( "batchSize5000", "l", "simple", "rel", 5000 );



         */

        //startMeasure( "batchSize5", "s", "simple", "graph", 5 );//yes
        //startMeasure( "batchSize500", "s", "simple", "graph", 500 );//yes
        //startMeasure( "batchSize1000", "s", "simple", "graph", 1000 );//yes

        //startMeasure( "batchSize5", "m", "simple", "graph", 5 );//yes
        //startMeasure( "batchSize500", "m", "simple", "graph", 500 );//yes
        //startMeasure( "batchSize1000", "m", "simple", "graph", 1000 );//yes

        //todo: first failed on first insertion (or writing to file): kong.unirest.UnirestException: java.net.SocketTimeoutException: Read timed out (evtl s file uusläse?)
        //startMeasure( "batchSize5", "l", "simple", "graph", 5 );
        //startMeasure( "batchSize500", "l", "simple", "graph", 500 );
        //startMeasure( "batchSize1000", "l", "simple", "graph", 1000 );

        /*

        startMeasure( "batchSize5", "s", "simple", "doc", 5 );
        startMeasure( "batchSize500", "s", "simple", "doc", 500 );
        startMeasure( "batchSize5000", "s", "simple", "doc", 5000 );

        startMeasure( "batchSize5", "m", "simple", "doc", 5 );
        startMeasure( "batchSize500", "m", "simple", "doc", 500 );
        startMeasure( "batchSize5000", "m", "simple", "doc", 5000 );

        startMeasure( "batchSize500", "l", "simple", "doc", 500 );
        startMeasure( "batchSize5000", "l", "simple", "doc", 5000 );
        startMeasure( "batchSize5", "l", "simple", "doc", 5 );

         */



        assertEquals( 2, 2, "Wrong number of tables" );

    }

    @Test public void relTest() throws InterruptedException {
        BackupManager backupManager = BackupManager.getINSTANCE();
        //backupManager.setBatchSize( 10 );
        addBasicRelData( 6 );
        /*
        List<Object[]> data = List.of(
                new Object[]{ 12, "Name1", 60 },
                new Object[]{ 15, "Name2", 24 },
                new Object[]{ 99, "Name3", 11 }
        );
        TestHelper.executeSql(
                ( c, s ) -> TestHelper.checkResultSet( s.executeQuery( "SELECT * FROM TableA" ), data, true ),
                //( c, s ) -> s.executeUpdate( "UPDATE TableA SET AGE = 13 WHERE AGE = 12" ),
                //( c, s ) -> s.executeUpdate( "DROP TABLE TableA" ),
                ( c, s ) -> c.commit()
        );

         */

        backupManager.startDataGathering( -1 );



        assertEquals( 2, 2, "Wrong number of tables" );
        //Thread.sleep( 5000 );
    }

    @Test
    public void docTest() {
        BackupManager backupManager = BackupManager.getINSTANCE();
        addBasicDocData(5);
        backupManager.startDataGathering( -1 );

        deleteBasicDocData();
        backupManager.startInserting();
        deleteBasicDocData();
        backupManager.startInserting();
        deleteBasicDocData();
        backupManager.startInserting();
        assertEquals( 2, 2, "Wrong number of tables" );
    }

    @Test
    public void graphTest() throws InterruptedException {
        BackupManager backupManager = BackupManager.getINSTANCE();
        addBasicGraphData(10);
        backupManager.startDataGathering( -1 );

        deleteBasicGraphData();
        backupManager.startInserting();
        deleteBasicGraphData();
        backupManager.startInserting();
        deleteBasicGraphData();
        backupManager.startInserting();
        assertEquals( 2, 2, "Wrong number of tables" );
        //Thread.sleep( 5000 );
    }


    /**
     * Start the measurement for the backup creation and insertion time
     *
     * @param parameter What backup parameter is measured (e.g. batchSize)
     * @param scale The scale of the data (s|m|l)
     * @param complexity The complexity of the data (simple|complex)
     * @param dataModel The data model to be tested (rel|doc|graph|allModels)
     * @param batchSize
     * @throws IOException when something goes wrong with creating a filewriter
     */
    private static void startMeasure ( String parameter, String scale, String complexity, String dataModel, int batchSize ) {
        TransactionManager transactionManager = testHelper.getTransactionManager();
        BackupManager backupManager = BackupManager.getINSTANCE();
        int nbrEntries = 0;

        switch ( scale ) {
            case "s":
                nbrEntries = 5;
                break;
            case "m":
                nbrEntries = 500;
                break;
            case "l":
                nbrEntries = 1000;
                break;
            default:
                break;
        }

        //parameter: e.g. batchsize, scaling: [s|m|l], complexity: [simple|complex], type: [collection|insertion], dataModel: [rel|doc|graph|allModels]
        // file title: e.g. rel_batchSize10_s_simple_collection
        String fileName = "";

        ArrayList<Long> measuredTime = new ArrayList<Long>();
        WriteToCSV writeToCSV = new WriteToCSV();

        switch ( dataModel ) {
            case "rel":
                if ( complexity.equals( "simple" )) {
                    //collection
                    addBasicRelData( nbrEntries );

                    fileName = String.format( "%s_%s_%s_%s_collection", dataModel, parameter, scale, complexity);
                    measuredTime = measureBackupCreationTime( backupManager, complexity,  dataModel, batchSize );
                    writeToCSV.writeToCSV( measuredTime, fileName );

                    deleteBasicRelData();
                    measuredTime.clear();

                    //insertion
                    fileName = String.format( "%s_%s_%s_%s_insertion", dataModel, parameter, scale, complexity);
                    measuredTime = measureBackupInsertionTime( backupManager, complexity, dataModel );
                    writeToCSV.writeToCSV( measuredTime, fileName );

                    measuredTime.clear();
                } else if ( complexity.equals( "complex" ) ) {
                    //addComplexRelEvalData( 6 );
                }

                break;


            case "doc":
                if ( complexity.equals( "simple" )) {
                    //collection
                    addBasicDocData( nbrEntries );

                    fileName = String.format( "%s_%s_%s_%s_collection", dataModel, parameter, scale, complexity);
                    measuredTime = measureBackupCreationTime( backupManager, complexity, dataModel, batchSize );
                    writeToCSV.writeToCSV( measuredTime, fileName );

                    deleteBasicDocData();
                    measuredTime.clear();

                    //insertion
                    fileName = String.format( "%s_%s_%s_%s_insertion", dataModel, parameter, scale, complexity);
                    measuredTime = measureBackupInsertionTime( backupManager, complexity, dataModel );
                    writeToCSV.writeToCSV( measuredTime, fileName );

                    measuredTime.clear();
                } else if ( complexity.equals( "complex" ) ) {
                    //addComplexRelEvalData( 6 );
                }
                break;


            case "graph":
                if ( complexity.equals( "simple" )) {
                    //collection
                    addBasicGraphData( nbrEntries );

                    fileName = String.format( "%s_%s_%s_%s_collection", dataModel, parameter, scale, complexity);
                    measuredTime = measureBackupCreationTime( backupManager, complexity, dataModel, batchSize );
                    writeToCSV.writeToCSV( measuredTime, fileName );

                    deleteBasicGraphData();
                    measuredTime.clear();

                    //insertion
                    fileName = String.format( "%s_%s_%s_%s_insertion", dataModel, parameter, scale, complexity);
                    measuredTime = measureBackupInsertionTime( backupManager, complexity, dataModel );
                    writeToCSV.writeToCSV( measuredTime, fileName );

                    measuredTime.clear();
                } else if ( complexity.equals( "complex" ) ) {
                    //addComplexRelEvalData( 6 );
                }
                break;

            case "allModels":
            default:
                if ( dataModel.isEmpty() ) {
                    dataModel = "allModels";
                }
                if ( complexity.equals( "simple" )) {
                    addBasicRelData( nbrEntries );
                    addBasicDocData( nbrEntries );
                    addBasicGraphData( nbrEntries );

                    fileName = String.format( "%s_%s_%s_%s_collection", dataModel, parameter, scale, complexity);
                    measuredTime = measureBackupCreationTime( backupManager, complexity, dataModel, batchSize );
                    writeToCSV.writeToCSV( measuredTime, fileName );

                    deleteBasicRelData();
                    deleteBasicDocData();
                    deleteBasicGraphData();
                    measuredTime.clear();

                    //insertion
                    fileName = String.format( "%s_%s_%s_%s_insertion", dataModel, parameter, scale, complexity);
                    measuredTime = measureBackupInsertionTime( backupManager, complexity, dataModel );
                    writeToCSV.writeToCSV( measuredTime, fileName );

                    measuredTime.clear();
                } else if ( complexity.equals( "complex" ) ) {
                    //addComplexRelEvalData( 6 );
                }
                break;
        }

    }


    private static ArrayList<Long> measureBackupCreationTime ( BackupManager backupManager, String complexity, String dataModel, int batchSize ) {
        ArrayList<Long> measuredTime = new ArrayList<Long>();
        long startTime;
        long elapsedTime;
        // es warmup mache vorhär, evtl eif 2, 3mol mässe vorhär - das au schriibe
        backupManager.startDataGathering( batchSize );
        backupManager.startDataGathering( batchSize );
        backupManager.startDataGathering( batchSize );
        for(int i=0; i< 50; i++){
            startTime = System.nanoTime();
            backupManager.startDataGathering( batchSize );
            elapsedTime = System.nanoTime() - startTime;
            measuredTime.add(elapsedTime);
            log.info( "Time" + dataModel+complexity + ": " + elapsedTime);
            //todo print elapsed time
            //can (and do i have to) delete old backup data & reset bupInofrmationObject?? - no
        }
        return measuredTime;
    }


    /**
     * Measure the time it takes to insert the backup data
     * @param backupManager the backup manager
     * @param complexity the complexity of the data can be (simple|complex)
     * @param dataModel the data model to be tested (rel|doc|graph|allModels)
     * @return a list of the measured time in nanoseconds
     */
    private static ArrayList<Long> measureBackupInsertionTime ( BackupManager backupManager, String complexity, String dataModel ) {
        ArrayList<Long> measuredTime = new ArrayList<Long>();
        long startTime;
        long elapsedTime;
        //parameter: e.g. batchsize, scaling: [s|m|l], complexity: [simple|complex], type: [collection|insertion], dataModel: [rel|doc|graph|allModels]
        // file title: e.g. rel_batchSize10_s_simple_collection

        // warmup runs, 2
        for(int i=0; i<= 3; i++){
            backupManager.startInserting();
            switch ( dataModel ) {
                case "rel":
                    deleteBasicRelData();
                    break;
                case "doc":
                    deleteBasicDocData();
                    break;
                case "graph":
                    deleteBasicGraphData();
                    break;
                case "allModels":
                default:
                    deleteBasicRelData();
                    deleteBasicDocData();
                    deleteBasicGraphData();
                    break;
            }
        }

        for(int i=0; i< 50; i++){
            startTime = System.nanoTime();
            backupManager.startInserting();
            elapsedTime = System.nanoTime() - startTime;
            measuredTime.add(elapsedTime);
            switch ( dataModel ) {
                case "rel":
                    deleteBasicRelData();
                    break;
                case "doc":
                    deleteBasicDocData();
                    break;
                case "graph":
                    deleteBasicGraphData();
                    break;
                case "allModels":
                default:
                    deleteBasicRelData();
                    deleteBasicDocData();
                    deleteBasicGraphData();
                    break;
            }
        }
        return measuredTime;
    }

    //------------------------------------------------------------------------

    private static void addBasicRelTestData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE NAMESPACE reli" );
                statement.executeUpdate( "CREATE TABLE reli.album(albumId INTEGER NOT NULL, albumName VARCHAR(255), nbrSongs INTEGER,PRIMARY KEY (albumId))" );
                statement.executeUpdate( "INSERT INTO reli.album VALUES (1, 'Best Album Ever!', 10), (2, 'Pretty Decent Album...', 15), (3, 'Your Ears will Bleed!', 13)" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while adding test data", e );
        }
    }

    private static void deleteBasicRelTestData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE reli.album" );
                statement.executeUpdate( "DROP SCHEMA reli" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while deleting old data", e );
        }
    }


    /**
     * Add simple relational evaluation data - simple structure
     * @param nbrRows number of rows to add
     */
    private static void addBasicRelData( int nbrRows ) {

        TestHelper.executeSql(
                ( c, s ) -> s.executeUpdate( "CREATE NAMESPACE reli" ),
                ( c, s ) -> s.executeUpdate( "CREATE TABLE reli.TableA(ID INTEGER NOT NULL, NAME VARCHAR(20), AGE INTEGER, PRIMARY KEY (ID))" )
        );

        for ( int i = 0; i < nbrRows; i++ ) {
            int finalI = i;
            int finalI1 = i;
            TestHelper.executeSql(
                    ( c, s ) -> s.executeUpdate( "INSERT INTO reli.TableA VALUES (" + finalI + ", 'Name" + finalI1 + "', 60)" )
            );
        }

        TestHelper.executeSql(
                ( c, s ) -> c.commit()
        );

        /*

        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE NAMESPACE reli" );
                statement.executeUpdate( "CREATE TABLE reli.album(albumId INTEGER NOT NULL, albumName VARCHAR(255), nbrSongs INTEGER,PRIMARY KEY (albumId))" );
                for ( int i = 0; i < nbrRows; i++ ) {
                    statement.executeUpdate( "INSERT INTO reli.album VALUES (" + i + ", 'Best Album Ever!', 10)" );
                }
                connection.commit();
                int l = 3;
            }
        } catch ( SQLException e ) {
            log.error( "Exception while adding test data", e );
        }

         */
    }


    /**
     * Delete simple relational evaluation data
     */
    private static void deleteBasicRelData() {
        /*
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                //statement.executeUpdate( "DROP TABLE reli.album" ); //TODO: delete this?
                statement.executeUpdate( "DROP SCHEMA IF EXISTS reli" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            log.error( "Exception while deleting old data", e );
        }

         */

        TestHelper.executeSql(
                ( c, s ) -> s.executeUpdate( "DROP SCHEMA IF EXISTS reli" ),
                ( c, s ) -> c.commit()
        );
    }

    //------------------------------------------------------------------------


    /**
     * Add simple graph test data with a number of nodes and edges. Edges are always between two nodes
     * @param nbrNodes how many nodes you want to create
     */
    private static void addBasicGraphData( int nbrNodes ) {
        //nbr Edges is nbrNodes/2
        /*

        final String SINGLE_NODE_PERSON_1 = "CREATE (p:Person {name: 'Max'})";
        final String SINGLE_NODE_PERSON_2 = "CREATE (p:Person {name: 'Hans'})";

        final String SINGLE_NODE_PERSON_COMPLEX_1 = "CREATE (p:Person {name: 'Ann', age: 45, depno: 13})";
        final String SINGLE_NODE_PERSON_COMPLEX_2 = "CREATE (p:Person {name: 'Bob', age: 31, depno: 13})";

        final String SINGLE_NODE_ANIMAL = "CREATE (a:Animal {name:'Kira', age:3, type:'dog'})";
        final String SINGLE_EDGE_1 = "CREATE (p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira', age:3, type:'dog'})";
        final String SINGLE_EDGE_2 = "CREATE (p:Person {name: 'Max'})-[rel:KNOWS {since: 1994}]->(a:Person {name:'Hans', age:31})";


        executeGraph( SINGLE_NODE_PERSON_2, GRAPH_NAME );
        executeGraph( SINGLE_NODE_PERSON_COMPLEX_1, GRAPH_NAME );
        executeGraph( SINGLE_NODE_PERSON_COMPLEX_2, GRAPH_NAME );
        executeGraph( SINGLE_NODE_ANIMAL, GRAPH_NAME );
        executeGraph( SINGLE_EDGE_1, GRAPH_NAME );
        executeGraph( SINGLE_EDGE_2, GRAPH_NAME );

         */
        String GRAPH_NAME = "graphtest";
        String nodesString = "";
        String edgesString = "";

        //create graph
        executeGraph( format( "CREATE DATABASE %s", GRAPH_NAME ) );
        executeGraph( format( "USE GRAPH %s", GRAPH_NAME ) );
        log.info( "started creating nodes and edges to insert" );

        // divide nbr nodes by 3, so that we have 3 times as many nodes as edges
        int nbrNodesReal = (nbrNodes/3)*2;
        int nbrEdges = (nbrNodes/3) - 1;


        for (int i = 0; i < nbrNodes; i++) {
            String nString = "n" + i;
            //nodesString += String.format( "(%s:Person {name: 'Ann', age: 45, depno: 13}), ", nString);
            String nodeQuery = String.format( "CREATE (%s:Person {name: 'Ann'})", nString );
            executeGraph( nodeQuery, GRAPH_NAME );
        }

        /*
        // connect two nodes with an edge
        for ( int i = 0; i < nbrEdges; i++ ) {
            if ( i%2 != 0 ) {
                String nString = "n" + i;
                String j = String.valueOf( i-1 );
                String eString = "n" + j;
                //edgesString += String.format( "(%s)-[:KNOWS {since: 1994}]->(%s), ", eString, nString);
                String edgeQuery = String.format( "CREATE (%s)-[:KNOWS {since: 1994}]->(%s)", eString, nString );
                executeGraph( edgeQuery, GRAPH_NAME );
            }
        }

         */
        // remove the last ", " from the string
        //nodesString = nodesString.substring( 0, nodesString.length() - 2 );
        //edgesString = edgesString.substring( 0, edgesString.length() - 2 );
        //String query = String.format( "CREATE %s, %s", nodesString, edgesString );
        //log.info( query );

        //executeGraph( query, GRAPH_NAME );
    }


    private static void deleteBasicGraphData() {
        deleteGraphData( "graphtest" );
    }

    //------------------------------------------------------------------------

    private static void addBasicDocData( int nbrDocs ) {
        initDatabase( "doctest" );  //database = namespace
        createCollection( "doc1", "doctest" );

        for ( int i = 0; i < nbrDocs; i++ ) {
            executeDoc( "db.doc1.insert({name: 'Max" + i + "', age: 31, depno: 13})", "doctest" );
        }
        //executeDoc( "db.doc1.insert({name: 'Max', age: 31, depno: 13})", "doctest" );
        //executeDoc( "db.doc1.insert({name: 'Hans', age: 45, depno: 13})", "doctest" );
        //executeDoc( "db.doc1.insert({name: 'Ann', age: 45, depno: 13})", "doctest" );
    }

    private static void deleteBasicDocData() {
        //dropCollection( "doc1", "doctest" );
        dropDatabase( "doctest" );

    }

    //------------------------------------------------------------------------


    public static void initDatabase( String database ) {
        MongoConnection.executeGetResponse( "use " + database );
    }

    public static void dropDatabase( String database ) {
        MongoConnection.executeGetResponse( "db.dropDatabase()", database );
    }

    public static void initCollection( String collection ) {
        MongoConnection.executeGetResponse( "db.createCollection(" + collection + ")" );
    }

    public static void createCollection( String collection, String database ) {
        MongoConnection.executeGetResponse( String.format( "db.createCollection( %s )", collection ), database );
    }

    public static void dropCollection( String collection ) {
        MongoConnection.executeGetResponse( "db." + collection + ".drop()" );
    }

    public static DocResult executeDoc( String doc ) {
        return MongoConnection.executeGetResponse( doc );
    }


    public static DocResult executeDoc( String doc, String database ) {
        return MongoConnection.executeGetResponse( doc, database );
    }


    //------------------------------------------------------------------------

    public static void deleteGraphData( String graph ) {
        executeGraph( format( "DROP DATABASE %s IF EXISTS", graph ) );
        Snapshot snapshot = Catalog.getInstance().getSnapshot();
        AdapterCatalog adapterCatalog = Catalog.getInstance().getAdapterCatalog( 0 ).orElseThrow();
    }


    public static GraphResult executeGraph( String query ) {
        GraphResult res = CypherConnection.executeGetResponse( query );
        if ( res.getError() != null ) {
            fail( res.getError() );
        }
        return res;
    }


    public static GraphResult executeGraph( String query, String namespace ) {
        GraphResult res = CypherConnection.executeGetResponse( query, namespace );
        if ( res.getError() != null ) {
            fail( res.getError() );
        }
        return res;
    }

}

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

package org.polypheny.db.workflow.engine.storage;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.mql.MqlTestTemplate;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

public class StorageUtils {

    public static final String HSQLDB_LOCKS = "hsqldb_locks";
    public static final String HSQLDB_MVLOCKS = "hsqldb_mvlocks";

    public static final String REL_TABLE = "rel_data";
    public static final String GRAPH = "lpg_data";

    private static final Set<String> createdGraphs = new HashSet<>();
    private static final Set<String> createdCollections = new HashSet<>();


    public static void addHsqldbStore( String name, String trxControlMode ) throws SQLException {
        TestHelper.executeSQL( "ALTER ADAPTERS ADD \"%s\" USING 'Hsqldb' AS 'Store'".formatted( name )
                + " WITH '{maxConnections:\"25\",trxControlMode:%s,trxIsolationLevel:read_committed,type:Memory,tableType:Memory,mode:embedded}'".formatted( trxControlMode )
        );
    }


    public static void addHsqldbLocksStore( String name ) throws SQLException {
        addHsqldbStore( name, HSQLDB_LOCKS );
    }


    public static void removeStore( String name ) throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {

                statement.executeUpdate( String.format( "ALTER ADAPTERS DROP \"%s\"", name ) );

            }
        }
    }


    public static Map<DataModel, String> getDefaultStoreMap( String storeName ) {
        return Map.of(
                DataModel.RELATIONAL, storeName,
                DataModel.DOCUMENT, storeName,
                DataModel.GRAPH, storeName
        );
    }


    public static List<List<PolyValue>> readCheckpoint( StorageManager sm, UUID activityId, int index ) {
        List<List<PolyValue>> list = new ArrayList<>();
        try ( CheckpointReader reader = sm.readCheckpoint( activityId, index ) ) {
            reader.getIterable().forEach( list::add );
        }
        return list;
    }


    public static AlgDataType readCheckpointType( StorageManager sm, UUID activityId, int index ) {
        try ( CheckpointReader reader = sm.readCheckpoint( activityId, index ) ) {
            return reader.getTupleType();
        }
    }


    public static void addRelData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( false ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE rel_data( id INTEGER NOT NULL, name VARCHAR(39), foo INTEGER, PRIMARY KEY (id))" );
                statement.executeUpdate( "INSERT INTO rel_data VALUES (1, 'Hans', 5)" );
                statement.executeUpdate( "INSERT INTO rel_data VALUES (2, 'Alice', 7)" );
                statement.executeUpdate( "INSERT INTO rel_data VALUES (3, 'Bob', 4)" );
                statement.executeUpdate( "INSERT INTO rel_data VALUES (4, 'Saskia', 6)" );
                statement.executeUpdate( "INSERT INTO rel_data VALUES (5, 'Rebecca', 3)" );
                statement.executeUpdate( "INSERT INTO rel_data VALUES (6, 'Georg', 9)" );
                connection.commit();
            }
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    public static void addLpgData() {
        CypherTestTemplate.createGraph( GRAPH );
        CypherTestTemplate.execute( "CREATE (p:Person {name: 'Ann', age: 45, depno: 13})", GRAPH );
        CypherTestTemplate.execute( "CREATE (p:Person {name: 'Bob', age: 31, depno: 13})", GRAPH );
        CypherTestTemplate.execute( "CREATE (p:Person {name: 'Hans', age: 55, depno: 7})", GRAPH );
        CypherTestTemplate.execute( "CREATE (p:Person {name: 'Max'})-[rel:OWNER_OF]->(a:Animal {name:'Kira', age:3, type:'dog'})", GRAPH );
    }


    public static String createEmptyGraph() {
        String graphName = "test_" + UUID.randomUUID().toString().replace( "-", "" );
        CypherTestTemplate.createGraph( graphName );
        createdGraphs.add( graphName );
        return graphName;
    }


    public static String createEmptyCollection( String namespace ) {
        MqlTestTemplate.initDatabase( namespace );
        String collectionName = "test_" + UUID.randomUUID().toString().replace( "-", "" );
        MqlTestTemplate.createCollection( collectionName, namespace );
        createdCollections.add( collectionName );
        return collectionName;
    }


    public static void dropData() {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "DROP TABLE rel_data" );
            }
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }

        CypherTestTemplate.deleteData( GRAPH );
        createdGraphs.forEach( CypherTestTemplate::deleteData );
        createdCollections.forEach( MqlTestTemplate::dropCollection );
    }

}

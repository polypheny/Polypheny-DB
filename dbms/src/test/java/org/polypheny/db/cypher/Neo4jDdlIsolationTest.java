/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.cypher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;

@Tag("adapter")
class Neo4jDdlIsolationTest {

    @Test
    void droppingAnotherNamespaceLeavesWritesUncommitted() throws SQLException {
        assumeTrue( "neo4j".equalsIgnoreCase( System.getProperty( "store.default" ) ) );
        TestHelper.getInstance();
        String id = UUID.randomUUID().toString().replace( "-", "" );
        String table = "ddl_writer_" + id;
        String graph = "ddl_graph_" + id;

        try ( JdbcConnection admin = new JdbcConnection( true ); Statement ddl = admin.getConnection().createStatement() ) {
            ddl.executeUpdate( "CREATE TABLE " + table + " (id INT NOT NULL, PRIMARY KEY(id))" );
            try {
                ddl.executeUpdate( "CREATE GRAPH NAMESPACE " + graph );
                try ( JdbcConnection writer = new JdbcConnection( false ); Statement insert = writer.getConnection().createStatement() ) {
                    try {
                        assertEquals( 1, insert.executeUpdate( "INSERT INTO " + table + " VALUES (1)" ) );
                        ddl.executeUpdate( "DROP NAMESPACE " + graph );
                    } finally {
                        writer.getConnection().rollback();
                    }
                }
                try ( ResultSet rows = ddl.executeQuery( "SELECT COUNT(*) FROM " + table ) ) {
                    assertTrue( rows.next() );
                    assertEquals( 0, rows.getInt( 1 ) );
                }
            } finally {
                try {
                    ddl.executeUpdate( "DROP NAMESPACE IF EXISTS " + graph );
                } finally {
                    ddl.executeUpdate( "DROP TABLE " + table );
                }
            }
        }
    }

}

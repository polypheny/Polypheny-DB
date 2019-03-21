/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.test;


import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.adapter.java.ReflectiveSchema;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbConnection;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Before;
import org.junit.Test;


/**
 * Test cases to check that necessary information from underlying exceptions is correctly propagated via {@link SQLException}s.
 */
public class ExceptionMessageTest {

    private Connection conn;


    /**
     * Simple reflective schema that provides valid and invalid entries.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static class TestSchema {

        public Entry[] entries = {
                new Entry( 1, "name1" ),
                new Entry( 2, "name2" )
        };

        public Iterable<Entry> badEntries = () -> {
            throw new IllegalStateException( "Can't iterate over badEntries" );
        };
    }


    /**
     * Entries made available in the reflective TestSchema.
     */
    public static class Entry {

        public int id;
        public String name;


        public Entry( int id, String name ) {
            this.id = id;
            this.name = name;
        }
    }


    @Before
    public void setUp() throws SQLException {
        Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:" );
        PolyphenyDbConnection polyphenyDbConnection = connection.unwrap( PolyphenyDbConnection.class );
        SchemaPlus rootSchema = polyphenyDbConnection.getRootSchema();
        rootSchema.add( "test", new ReflectiveSchema( new TestSchema() ) );
        polyphenyDbConnection.setSchema( "test" );
        this.conn = polyphenyDbConnection;
    }


    private void runQuery( String sql ) throws SQLException {
        Statement stmt = conn.createStatement();
        try {
            stmt.executeQuery( sql );
        } finally {
            try {
                stmt.close();
            } catch ( Exception e ) {
                // We catch a possible exception on close so that we know we're not masking the query exception with the close exception
                fail( "Error on close" );
            }
        }
    }


    @Test
    public void testValidQuery() throws SQLException {
        // Just ensure that we're actually dealing with a valid connection to be sure that the results of the other tests can be trusted
        runQuery( "select * from \"entries\"" );
    }


    @Test
    public void testNonSqlException() throws SQLException {
        try {
            runQuery( "select * from \"badEntries\"" );
            fail( "Query badEntries should result in an exception" );
        } catch ( SQLException e ) {
            assertThat( e.getMessage(), equalTo( "Error while executing SQL \"select * from \"badEntries\"\": Can't iterate over badEntries" ) );
        }
    }


    @Test
    public void testSyntaxError() {
        try {
            runQuery( "invalid sql" );
            fail( "Query should fail" );
        } catch ( SQLException e ) {
            assertThat( e.getMessage(), equalTo( "Error while executing SQL \"invalid sql\": parse failed: Non-query expression encountered in illegal context" ) );
        }
    }


    @Test
    public void testSemanticError() {
        try {
            runQuery( "select \"name\" - \"id\" from \"entries\"" );
            fail( "Query with semantic error should fail" );
        } catch ( SQLException e ) {
            assertThat( e.getMessage(), containsString( "Cannot apply '-' to arguments" ) );
        }
    }


    @Test
    public void testNonexistentTable() {
        try {
            runQuery( "select name from \"nonexistentTable\"" );
            fail( "Query should fail" );
        } catch ( SQLException e ) {
            assertThat( e.getMessage(), containsString( "Object 'nonexistentTable' not found" ) );
        }
    }
}

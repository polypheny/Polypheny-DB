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


import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.adapter.java.ReflectiveSchema;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcSchema;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbEmbeddedConnection;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import com.google.common.collect.Sets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.Test;


/**
 * Test case for joining tables from two different JDBC databases.
 */
public class MultiJdbcSchemaJoinTest {

    @Test
    public void test() throws SQLException, ClassNotFoundException {
        // Create two databases
        // It's two times hsqldb, but imagine they are different rdbms's
        final String db1 = TempDb.INSTANCE.getUrl();
        Connection c1 = DriverManager.getConnection( db1, "", "" );
        Statement stmt1 = c1.createStatement();
        stmt1.execute( "create table table1(id varchar(10) not null primary key, field1 varchar(10))" );
        stmt1.execute( "insert into table1 values('a', 'aaaa')" );
        c1.close();

        final String db2 = TempDb.INSTANCE.getUrl();
        Connection c2 = DriverManager.getConnection( db2, "", "" );
        Statement stmt2 = c2.createStatement();
        stmt2.execute( "create table table2(id varchar(10) not null primary key, field1 varchar(10))" );
        stmt2.execute( "insert into table2 values('a', 'aaaa')" );
        c2.close();

        // Connect via Polypheny-DB to these databases
        Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:" );
        PolyphenyDbEmbeddedConnection polyphenyDbEmbeddedConnection = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
        SchemaPlus rootSchema = polyphenyDbEmbeddedConnection.getRootSchema();
        final DataSource ds1 = JdbcSchema.dataSource( db1, "org.hsqldb.jdbcDriver", "", "" );
        rootSchema.add( "DB1", JdbcSchema.create( rootSchema, "DB1", ds1, null, null ) );
        final DataSource ds2 = JdbcSchema.dataSource( db2, "org.hsqldb.jdbcDriver", "", "" );
        rootSchema.add( "DB2", JdbcSchema.create( rootSchema, "DB2", ds2, null, null ) );

        Statement stmt3 = connection.createStatement();
        ResultSet rs = stmt3.executeQuery( "select table1.id, table1.field1 from db1.table1 join db2.table2 on table1.id = table2.id" );
        assertThat( PolyphenyDbAssert.toString( rs ), equalTo( "ID=a; FIELD1=aaaa\n" ) );
    }


    /**
     * Makes sure that {@link #test} is re-entrant. Effectively a test for {@code TempDb}.
     */
    @Test
    public void test2() throws SQLException, ClassNotFoundException {
        test();
    }


    private Connection setup() throws SQLException {
        // Create a jdbc database & table
        final String db = TempDb.INSTANCE.getUrl();
        Connection c1 = DriverManager.getConnection( db, "", "" );
        Statement stmt1 = c1.createStatement();
        // This is a table we can join with the emps from the hr schema
        stmt1.execute( "create table table1(id integer not null primary key, field1 varchar(10))" );
        stmt1.execute( "insert into table1 values(100, 'foo')" );
        stmt1.execute( "insert into table1 values(200, 'bar')" );
        c1.close();

        // Make a Polypheny-DB schema with both a jdbc schema and a non-jdbc schema
        Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:" );
        PolyphenyDbEmbeddedConnection polyphenyDbEmbeddedConnection = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
        SchemaPlus rootSchema = polyphenyDbEmbeddedConnection.getRootSchema();
        rootSchema.add( "DB",
                JdbcSchema.create( rootSchema, "DB",
                        JdbcSchema.dataSource( db, "org.hsqldb.jdbcDriver", "", "" ),
                        null,
                        null ) );
        rootSchema.add( "hr", new ReflectiveSchema( new JdbcTest.HrSchema() ) );
        return connection;
    }


    @Test
    public void testJdbcWithEnumerableJoin() throws SQLException {
        // This query works correctly
        String query = "select t.id, t.field1 from db.table1 t join \"hr\".\"emps\" e on e.\"empid\" = t.id";
        final Set<Integer> expected = Sets.newHashSet( 100, 200 );
        assertThat( runQuery( setup(), query ), equalTo( expected ) );
    }


    @Test
    public void testEnumerableWithJdbcJoin() throws SQLException {
        //  * compared to testJdbcWithEnumerableJoin, the join order is reversed
        //  * the query fails with a CannotPlanException
        String query = "select t.id, t.field1 from \"hr\".\"emps\" e join db.table1 t on e.\"empid\" = t.id";
        final Set<Integer> expected = Sets.newHashSet( 100, 200 );
        assertThat( runQuery( setup(), query ), equalTo( expected ) );
    }


    @Test
    public void testEnumerableWithJdbcJoinWithWhereClause() throws SQLException {
        // Same query as above but with a where condition added:
        //  * the good: this query does not give a CannotPlanException
        //  * the bad: the result is wrong: there is only one emp called Bill. The query plan shows the join condition is always true, afaics, the join condition is pushed down to the non-jdbc
        //             table. It might have something to do with the cast that is introduced in the join condition.
        String query = "select t.id, t.field1 from \"hr\".\"emps\" e join db.table1 t on e.\"empid\" = t.id where e.\"name\" = 'Bill'";
        final Set<Integer> expected = Sets.newHashSet( 100 );
        assertThat( runQuery( setup(), query ), equalTo( expected ) );
    }


    private Set<Integer> runQuery( Connection connection, String query ) throws SQLException {
        // Print out the plan
        Statement stmt = connection.createStatement();
        try {
            ResultSet rs;
            if ( PolyphenyDbPrepareImpl.DEBUG ) {
                rs = stmt.executeQuery( "explain plan for " + query );
                rs.next();
                System.out.println( rs.getString( 1 ) );
            }

            // Run the actual query
            rs = stmt.executeQuery( query );
            Set<Integer> ids = new HashSet<>();
            while ( rs.next() ) {
                ids.add( rs.getInt( 1 ) );
            }
            return ids;
        } finally {
            stmt.close();
        }
    }


    @Test
    public void testSchemaConsistency() throws Exception {
        // Create a database
        final String db = TempDb.INSTANCE.getUrl();
        Connection c1 = DriverManager.getConnection( db, "", "" );
        Statement stmt1 = c1.createStatement();
        stmt1.execute( "create table table1(id varchar(10) not null primary key, field1 varchar(10))" );

        // Connect via Polypheny-DB to these databases
        Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:" );
        PolyphenyDbEmbeddedConnection polyphenyDbEmbeddedConnection = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
        SchemaPlus rootSchema = polyphenyDbEmbeddedConnection.getRootSchema();
        final DataSource ds = JdbcSchema.dataSource( db, "org.hsqldb.jdbcDriver", "", "" );
        rootSchema.add( "DB", JdbcSchema.create( rootSchema, "DB", ds, null, null ) );

        Statement stmt3 = connection.createStatement();
        ResultSet rs;

        // fails, table does not exist
        try {
            rs = stmt3.executeQuery( "select * from db.table2" );
            fail( "expected error, got " + rs );
        } catch ( SQLException e ) {
            assertThat( e.getCause().getCause().getMessage(), equalTo( "Object 'TABLE2' not found within 'DB'" ) );
        }

        stmt1.execute( "create table table2(id varchar(10) not null primary key, field1 varchar(10))" );
        stmt1.execute( "insert into table2 values('a', 'aaaa')" );

        PreparedStatement stmt2 = connection.prepareStatement( "select * from db.table2" );

        stmt1.execute( "alter table table2 add column field2 varchar(10)" );

        // "field2" not visible to stmt2
        rs = stmt2.executeQuery();
        assertThat( PolyphenyDbAssert.toString( rs ), equalTo( "ID=a; FIELD1=aaaa\n" ) );

        // "field2" visible to a new query
        rs = stmt3.executeQuery( "select * from db.table2" );
        assertThat( PolyphenyDbAssert.toString( rs ), equalTo( "ID=a; FIELD1=aaaa; FIELD2=null\n" ) );
        c1.close();
    }


    /**
     * Pool of temporary databases.
     */
    static class TempDb {

        public static final TempDb INSTANCE = new TempDb();

        private final AtomicInteger id = new AtomicInteger( 1 );


        TempDb() {
        }


        /**
         * Allocates a URL for a new Hsqldb database.
         */
        public String getUrl() {
            return "jdbc:hsqldb:mem:db" + id.getAndIncrement();
        }
    }
}


/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.transaction.locking.ConcurrencyControlType;

public class DocumentMvccTest {

    private static TestHelper testHelper;
    private static final String NAMESPACE = "docMvccTest";

    private static final String FIND_ALL = "db.Users.find({})";
    private static final String INSERT_1 = "db.Users.insertMany([ { UserID: 1, UUID: 10 }, { UserID: 2, UUID: 20 } ])";
    private static final String INSERT_2 = "db.Users.insertMany([ { UserID: 3, UUID: 30 }, { UserID: 4, UUID: 40 } ])";
    private static final String UPDATE_1 = "db.Users.updateOne( { UserID: 2 }, { $set: { UUID: 100 } } )";
    private static final String UPDATE_2 = "db.Users.updateOne( { UserID: 2 }, { $set: { UUID: 200 } } )";
    private static final String DELETE_1 = "db.Users.deleteOne({ UserID: 2 })";

    @BeforeAll
    public static void setUpClass() {
        testHelper = TestHelper.getInstance();
    }

    private void setup() {
        Transaction transaction = testHelper.getTransaction();
        String createNamespaceStatement = String.format( "CREATE DOCUMENT NAMESPACE %s CONCURRENCY MVCC;", NAMESPACE );
        ConcurrencyTestUtils.executeStatement( createNamespaceStatement, "sql", transaction, testHelper );

        transaction = testHelper.getTransaction();
        ConcurrencyTestUtils.executeStatement( "db.createCollection(\"Users\")", "mongo", NAMESPACE, transaction, testHelper );
    }

    private void teardown() {
        Transaction transaction = testHelper.getTransaction();
        ConcurrencyTestUtils.executeStatement( "db.Users.drop()", "mongo", NAMESPACE, transaction, testHelper );

        transaction = testHelper.getTransaction();
        String dropNamespace = "DROP NAMESPACE IF EXISTS " + NAMESPACE;
        ConcurrencyTestUtils.executeStatement( dropNamespace, "sql", transaction, testHelper );
    }

    private void closeAndIgnore( List<ExecutedContext> result ) {
        result.forEach( e -> e.getIterator().getAllRowsAndClose() );
    }

    @Test
    public void testNonMvccCreateNamespace() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE DOCUMENT NAMESPACE nonMvccTest2 CONCURRENCY S2PL" );
                    connection.commit();

                    Optional<LogicalNamespace> optionalNamespace = Catalog.getInstance().getSnapshot().getNamespace( "nonMvccTest2" );
                    assertTrue( optionalNamespace.isPresent() );
                    assertEquals( ConcurrencyControlType.S2PL, optionalNamespace.get().getConcurrencyControlType() );

                } finally {
                    statement.executeUpdate( "DROP NAMESPACE IF EXISTS nonMvccTest2" );
                    statement.close();
                    connection.commit();
                }
            }
        }
    }

    @Test
    public void testMvccCreateNamespace() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "CREATE DOCUMENT NAMESPACE mvccTest2 CONCURRENCY MVCC" );
                    connection.commit();

                    Optional<LogicalNamespace> optionalNamespace = Catalog.getInstance().getSnapshot().getNamespace( "mvccTest2" );
                    assertTrue( optionalNamespace.isPresent() );
                    assertEquals( ConcurrencyControlType.MVCC, optionalNamespace.get().getConcurrencyControlType() );
                } finally {
                    statement.executeUpdate( "DROP NAMESPACE IF EXISTS mvccTest2" );
                    statement.close();
                    connection.commit();
                }
            }
        }
    }

    @Test
    public void testDefaultS2PLCreateNamespace() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "ALTER CONFIG 'runtime/docDefaultConcurrencyControl' SET 'S2PL'" );
                    statement.executeUpdate( "CREATE DOCUMENT NAMESPACE nonMvccTest" );
                    connection.commit();

                    Optional<LogicalNamespace> optionalNamespace = Catalog.getInstance().getSnapshot().getNamespace( "nonMvccTest" );
                    assertTrue( optionalNamespace.isPresent() );
                    assertEquals( ConcurrencyControlType.S2PL, optionalNamespace.get().getConcurrencyControlType() );

                } finally {
                    statement.executeUpdate( "DROP NAMESPACE IF EXISTS nonMvccTest" );
                    connection.commit();
                }
            }
        }
    }


    @Test
    public void testDefaultMVCCCreateNamespace() throws SQLException {
        try ( JdbcConnection jdbcConnection = new JdbcConnection( true ) ) {
            Connection connection = jdbcConnection.getConnection();
            try ( java.sql.Statement statement = connection.createStatement() ) {
                try {
                    statement.executeUpdate( "ALTER CONFIG 'runtime/docDefaultConcurrencyControl' SET 'MVCC'" );
                    statement.executeUpdate( "CREATE DOCUMENT NAMESPACE mvccTest" );
                    connection.commit();

                    Optional<LogicalNamespace> optionalNamespace = Catalog.getInstance().getSnapshot().getNamespace( "mvccTest" );
                    assertTrue( optionalNamespace.isPresent() );
                    assertEquals( ConcurrencyControlType.MVCC, optionalNamespace.get().getConcurrencyControlType() );

                } finally {
                    statement.executeUpdate( "DROP NAMESPACE IF EXISTS mvccTest" );
                    connection.commit();
                }
            }
        }
    }

}


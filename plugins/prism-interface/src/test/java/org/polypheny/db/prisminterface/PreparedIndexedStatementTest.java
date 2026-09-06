/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.prisminterface;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.prisminterface.statementProcessing.StatementProcessor;
import org.polypheny.db.prisminterface.statements.PIPreparedIndexedStatement;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings( "SqlNoDataSourceInspection" )
@Tag( "adapter" )
public class PreparedIndexedStatementTest {

    private static final String TABLE = "pis_test";
    private static TransactionManager transactionManager;
    private PIClient client;


    @BeforeAll
    public static void init() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
        transactionManager = TestHelper.getInstance().getTransactionManager();
    }


    @BeforeEach
    public void start() throws SQLException {
        try ( JdbcConnection conn = new JdbcConnection( true ) ) {
            try ( Statement stmt = conn.getConnection().createStatement() ) {
                stmt.execute( "CREATE TABLE " + TABLE + " (id INTEGER PRIMARY KEY, v BOOLEAN ARRAY(1,3))" );
            }
        }
        LogicalUser user = Catalog.snapshot().getUser( Catalog.USER_NAME ).orElseThrow();
        LogicalNamespace namespace = Catalog.snapshot().getNamespace( Catalog.DEFAULT_NAMESPACE_NAME ).orElseThrow();
        MonitoringPage monitoringPage = new MonitoringPage( "test-pis-" + System.nanoTime(),
                "Test PI Client" );
        client = new PIClient( "test-uuid-pis", user, transactionManager, namespace, monitoringPage, true );
    }


    @AfterEach
    public void stop() throws SQLException {
        try ( JdbcConnection conn = new JdbcConnection( true ) ) {
            try ( Statement stmt = conn.getConnection().createStatement() ) {
                stmt.execute( "DROP TABLE IF EXISTS " + TABLE );
            }
        }
    }


    private PIPreparedIndexedStatement prepareInsert( int stmtId ) throws
            Exception {
        LogicalNamespace namespace = Catalog.snapshot().getNamespace(
                Catalog.DEFAULT_NAMESPACE_NAME ).orElseThrow();
        PIPreparedIndexedStatement stmt = new PIPreparedIndexedStatement(
                stmtId, client, QueryLanguage.from( "sql" ), namespace,
                "INSERT INTO " + TABLE + " (id, v) VALUES (?, ?)"
        );
        StatementProcessor.prepare( stmt );
        return stmt;
    }


    @Test
    public void parameterPolyTypesArePopulatedAfterPrepare() throws Exception {
        PIPreparedIndexedStatement stmt = prepareInsert( 1 );
        assertNotNull( stmt.getParameterPolyTypes() );
        assertEquals( 2, stmt.getParameterPolyTypes().size() );
    }


    @Test
    public void arrayParameterTypeIsPreservedFromPlan() throws Exception {
        PIPreparedIndexedStatement stmt = prepareInsert( 2 );
        AlgDataType arrayType = stmt.getParameterPolyTypes().get( 1 );
        assertEquals( PolyType.ARRAY, arrayType.getPolyType() );
        assertNotEquals( PolyType.ANY, arrayType.getComponentType().getPolyType(),
                "component type must not be erased to ANY" );
        assertEquals( PolyType.BOOLEAN, arrayType.getComponentType().getPolyType() );
    }


    @Test
    public void indexedStatementInsertsRowAndReadsBack() throws Exception {
        PIPreparedIndexedStatement stmt = prepareInsert( 3 );
        List<PolyValue> values = List.of(
                PolyInteger.of( 42 ),
                PolyList.ofElements( PolyBoolean.TRUE, PolyBoolean.FALSE, PolyBoolean.TRUE ) );
        org.polypheny.prism.StatementResult result = stmt.execute( values, stmt.getParameterMetas(), 100 );
        assertEquals( 1, result.getScalar() );

        try ( JdbcConnection conn = new JdbcConnection( true ) ) {
            try ( Statement s = conn.getConnection().createStatement() ) {
                ResultSet rs = s.executeQuery( "SELECT id FROM " + TABLE + " WHERE id = 42" );
                assertTrue( rs.next() );
                assertEquals( 42, rs.getInt( 1 ) );
            }
        }
    }


    @Test
    public void indexedStatementCanBeExecutedMultipleTimesWithDifferentValues() throws Exception {
        PIPreparedIndexedStatement stmt = prepareInsert( 4 );
        for ( int i = 1; i <= 3; i++ ) {
            List<PolyValue> values = List.of(
                    PolyInteger.of( i ),
                    PolyList.ofElements( PolyBoolean.of( i % 2 == 0 ), PolyBoolean.FALSE, PolyBoolean.TRUE ) );
            org.polypheny.prism.StatementResult result = stmt.execute( values, stmt.getParameterMetas(), 100 );
            assertEquals( 1, result.getScalar() );
        }

        try ( JdbcConnection conn = new JdbcConnection( true ) ) {
            try ( Statement s = conn.getConnection().createStatement() ) {
                ResultSet rs = s.executeQuery( "SELECT COUNT(*) FROM " + TABLE );
                assertTrue( rs.next() );
                assertEquals( 3, rs.getInt( 1 ) );
            }
        }
    }


    @Test
    public void executeBatchInsertsAllRows() throws Exception {
        PIPreparedIndexedStatement stmt = prepareInsert( 5 );
        // outer list = columns, inner list = rows
        List<List<PolyValue>> batch = List.of(
                // id column
                List.of( PolyInteger.of( 10 ), PolyInteger.of( 20 ),
                        PolyInteger.of( 30 ) ),
                // val column
                List.of(
                        PolyList.ofElements( PolyBoolean.TRUE, PolyBoolean.FALSE, PolyBoolean.TRUE ),
                        PolyList.ofElements( PolyBoolean.FALSE, PolyBoolean.FALSE, PolyBoolean.TRUE ),
                        PolyList.ofElements( PolyBoolean.TRUE, PolyBoolean.TRUE, PolyBoolean.FALSE ) ) );
        List<Long> counts = stmt.executeBatch( batch );
        assertEquals( 1, counts.size() );
        assertEquals( 3L, counts.get( 0 ) );

        try ( JdbcConnection conn = new JdbcConnection( true ) ) {
            try ( Statement s = conn.getConnection().createStatement() ) {
                ResultSet rs = s.executeQuery( "SELECT COUNT(*) FROM " + TABLE );
                assertTrue( rs.next() );
                assertEquals( 3, rs.getInt( 1 ) );
            }
        }
    }

}

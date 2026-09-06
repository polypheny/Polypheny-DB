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
import org.polypheny.db.prisminterface.statements.PIPreparedNamedStatement;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.prism.StatementResult;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings( "SqlNoDataSourceInspection" )
@Tag( "adapter" )
public class PreparedNamedStatementTest {
    private static final String TABLE = "pns_test";
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
        MonitoringPage monitoringPage = new MonitoringPage( "test-pns-" + System.nanoTime(), "Test PI Client" );
        client = new PIClient( "test-uuid-pns", user, transactionManager, namespace, monitoringPage, true );
    }


    @AfterEach
    public void stop() throws SQLException {
        try ( JdbcConnection conn = new JdbcConnection( true ) ) {
            try ( Statement stmt = conn.getConnection().createStatement() ) {
                stmt.execute( "DROP TABLE IF EXISTS " + TABLE );
            }
        }
    }


    private PIPreparedNamedStatement prepareInsert( int stmtId ) {
        LogicalNamespace namespace = Catalog.snapshot().getNamespace( Catalog.DEFAULT_NAMESPACE_NAME ).orElseThrow();
        PIPreparedNamedStatement stmt = new PIPreparedNamedStatement( stmtId, client, QueryLanguage.from( "sql" ), namespace,
                "INSERT INTO " + TABLE + " (id, v) VALUES (:id, :v)" );
        StatementProcessor.prepare( stmt );
        return stmt;
    }


    @Test
    public void parameterPolyTypesArePopulatedAfterPrepare() {
        PIPreparedNamedStatement stmt = prepareInsert( 1 );
        assertNotNull( stmt.getParameterPolyTypes() );
        assertEquals( 2, stmt.getParameterPolyTypes().size() );
    }


    @Test
    public void arrayParameterTypeIsPreservedFromPlan() {
        PIPreparedNamedStatement stmt = prepareInsert( 2 );
        AlgDataType arrayType = stmt.getParameterPolyTypes().get( 1 );
        assertEquals( PolyType.ARRAY, arrayType.getPolyType() );
        assertNotEquals( PolyType.ANY, arrayType.getComponentType().getPolyType(),
                "component type must not be erased to ANY by value-based type derivation" );
        assertEquals( PolyType.BOOLEAN, arrayType.getComponentType().getPolyType() );
    }


    @Test
    public void namedStatementInsertsRowAndReadsBack() throws Exception {
        PIPreparedNamedStatement stmt = prepareInsert( 3 );
        Map<String, PolyValue> values = Map.of(
                "id", PolyInteger.of( 42 ),
                "v", PolyList.ofElements( PolyBoolean.TRUE, PolyBoolean.FALSE,
                        PolyBoolean.TRUE ) );
        StatementResult result = stmt.execute( values, 100 );
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
    public void namedStatementCanBeExecutedMultipleTimesWithDifferentValues() throws Exception {
        PIPreparedNamedStatement stmt = prepareInsert( 4 );
        for ( int i = 1; i <= 3; i++ ) {
            Map<String, PolyValue> values = Map.of(
                    "id", PolyInteger.of( i ),
                    "v", PolyList.ofElements( PolyBoolean.of( i % 2 == 0 ),
                            PolyBoolean.FALSE, PolyBoolean.TRUE ) );
            StatementResult result = stmt.execute( values, 100 );
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

}

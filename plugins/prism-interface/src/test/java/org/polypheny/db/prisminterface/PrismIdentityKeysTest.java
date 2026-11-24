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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.db.prisminterface.statementProcessing.StatementProcessor;
import org.polypheny.db.prisminterface.statements.BatchProgress;
import org.polypheny.db.prisminterface.statements.PIPreparedIndexedStatement;
import org.polypheny.db.prisminterface.statements.PIUnparameterizedStatement;
import org.polypheny.db.processing.ConstraintStrategy;
import org.polypheny.prism.BatchGeneratedKeys;
import org.polypheny.prism.ClientInfoProperties;
import org.polypheny.prism.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.GeneratedKeysRequest;
import org.polypheny.prism.PrepareStatementRequest;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResult;

@Tag("adapter")
class PrismIdentityKeysTest {

    @BeforeAll
    static void init() {
        TestHelper.getInstance();
    }


    @Test
    void preservesTheUpdateCountAndReturnsKeysInSeparateFrames() throws SQLException {
        try ( JdbcConnection connection = new JdbcConnection( true );
                Statement setup = connection.getConnection().createStatement() ) {
            setup.executeUpdate( "CREATE TABLE prism_identity_keys (id BIGSERIAL PRIMARY KEY, label VARCHAR(100))" );
            PIClient client = new PIClient( "prism-identity-keys", Catalog.snapshot().getUser( Catalog.USER_NAME ).orElseThrow(),
                    TestHelper.getInstance().getTransactionManager(), Catalog.snapshot().getNamespace( Catalog.DEFAULT_NAMESPACE_NAME ).orElseThrow(),
                    new MonitoringPage( "prism-identity-keys", "Prism identity keys test" ), true );
            try {
                GeneratedKeysRequest all = GeneratedKeysRequest.newBuilder().setAll( true ).build();
                StatementResult single = execute( client, "INSERT INTO prism_identity_keys VALUES (DEFAULT, 'one'), (50, 'explicit'), (DEFAULT, 'two')", all );
                assertEquals( 3, single.getScalar() );
                assertFalse( single.hasFrame() );
                RelationalFrame keys = single.getGeneratedKeys().getRelationalFrame();
                assertEquals( "id", keys.getColumnMeta( 0 ).getColumnName() );
                assertTrue( keys.getColumnMeta( 0 ).getIsAutoIncrement() );
                assertEquals( List.of( 1L, 2L ), longs( keys ) );
                StatementResult explicit = execute( client, "INSERT INTO prism_identity_keys VALUES (51, 'explicit')", all );
                assertEquals( 1, explicit.getScalar() );
                assertEquals( 0, explicit.getGeneratedKeys().getRelationalFrame().getRowsCount() );

                GeneratedKeysRequest selection = GeneratedKeysRequest.newBuilder()
                        .setColumnIndexes( GeneratedKeysRequest.ColumnIndexes.newBuilder().addIndexes( 1 ) )
                        .build();
                PIPreparedIndexedStatement prepared = client.getStatementManager().createIndexedPreparedInterfaceStatement( PrepareStatementRequest.newBuilder()
                        .setLanguageName( "sql" )
                        .setStatement( "INSERT INTO prism_identity_keys DEFAULT VALUES" )
                        .setGeneratedKeys( selection )
                        .build() );
                StatementProcessor.prepare( prepared );
                StatementBatchResponse batch = prepared.executeSequentialBatch( List.of( List.of(), List.of() ), new BatchProgress( prepared.getId() ) );
                assertEquals( List.of( 1L, 1L ), batch.getScalarsList() );
                assertEquals( List.of( 0, 1 ), batch.getGeneratedKeysList().stream().map( BatchGeneratedKeys::getStatementIndex ).toList() );
                assertEquals( List.of( 3L, 4L ), batch.getGeneratedKeysList().stream()
                        .flatMap( result -> longs( result.getKeys().getRelationalFrame() ).stream() )
                        .toList() );
            } finally {
                client.rollbackCurrentTransaction( null );
                client.getStatementManager().closeAll();
                setup.executeUpdate( "DROP TABLE prism_identity_keys" );
            }
        }
    }


    @Test
    void sendsCommittedBatchCountsAndKeysOnlyToOptedInClients() throws IOException, SQLException {
        String table = "prism_identity_batch_progress";
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement setup = jdbc.getConnection().createStatement() ) {
            setup.executeUpdate( "CREATE TABLE " + table + " (id BIGSERIAL PRIMARY KEY)" );
            try {
                for ( boolean optIn : new boolean[]{ false, true } ) {
                    try ( PrismConnection connection = new PrismConnection( true, 5 ) ) {
                        if ( optIn ) {
                            Response optedIn = connection.send( Request.newBuilder().setSetClientInfoPropertiesRequest( ClientInfoProperties.newBuilder()
                                    .putProperties( PIClientInfoProperties.PARTIAL_BATCH_RESULTS, "true" ) ) );
                            assertEquals( Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE, optedIn.getTypeCase() );
                        }
                        GeneratedKeysRequest all = GeneratedKeysRequest.newBuilder().setAll( true ).build();
                        Request.Builder batch = Request.newBuilder().setExecuteUnparameterizedStatementBatchRequest(
                                ExecuteUnparameterizedStatementBatchRequest.newBuilder()
                                        .addStatements( ExecuteUnparameterizedStatementRequest.newBuilder().setLanguageName( "sql" )
                                                .setStatement( "INSERT INTO " + table + " DEFAULT VALUES; -- first member" ).setGeneratedKeys( all ) )
                                        .addStatements( ExecuteUnparameterizedStatementRequest.newBuilder().setLanguageName( "sql" )
                                                .setStatement( "INSERT INTO " + table + "_missing DEFAULT VALUES" ).setGeneratedKeys( all ) ) );
                        List<Response> intermediate = new ArrayList<>();
                        Response failure = connection.send( batch, intermediate::add );
                        assertTrue( failure.hasErrorResponse() );
                        assertEquals( "42P01", failure.getErrorResponse().getDetails().getState() );
                        assertEquals( optIn ? 2 : 1, intermediate.size() );
                        StatementBatchResponse started = intermediate.get( 0 ).getStatementBatchResponse();
                        assertEquals( 0, started.getScalarsCount() );
                        if ( optIn ) {
                            StatementBatchResponse completed = intermediate.get( 1 ).getStatementBatchResponse();
                            assertEquals( started.getBatchId(), completed.getBatchId() );
                            assertEquals( List.of( 1L ), completed.getScalarsList() );
                            assertEquals( 0, completed.getGeneratedKeys( 0 ).getStatementIndex() );
                            assertEquals( List.of( 2L ), longs( completed.getGeneratedKeys( 0 ).getKeys().getRelationalFrame() ) );
                        }
                    }
                }
            } finally {
                setup.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @Test
    void reportsTheUniqueSqlStateForIdentityUpdatesUnderBothStrategies() throws SQLException {
        boolean uniqueConstraints = RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean();
        Enum<?> previousStrategy = RuntimeConfig.CONSTRAINT_ENFORCEMENT_STRATEGY.getEnum();
        RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.setBoolean( true );
        try ( JdbcConnection connection = new JdbcConnection( true );
                Statement setup = connection.getConnection().createStatement() ) {
            setup.executeUpdate( "CREATE TABLE prism_identity_sqlstate (pk INTEGER PRIMARY KEY, seq SERIAL, u INTEGER, CONSTRAINT identity_state_u UNIQUE (u))" );
            PIClient client = new PIClient( "prism-identity-sqlstate", Catalog.snapshot().getUser( Catalog.USER_NAME ).orElseThrow(),
                    TestHelper.getInstance().getTransactionManager(), Catalog.snapshot().getNamespace( Catalog.DEFAULT_NAMESPACE_NAME ).orElseThrow(),
                    new MonitoringPage( "prism-identity-sqlstate", "Prism identity SQLSTATE test" ), true );
            try {
                setup.executeUpdate( "INSERT INTO prism_identity_sqlstate (pk, u) VALUES (1, 10), (2, 20)" );
                for ( ConstraintStrategy strategy : ConstraintStrategy.values() ) {
                    RuntimeConfig.CONSTRAINT_ENFORCEMENT_STRATEGY.setEnum( strategy );
                    PIUnparameterizedStatement write = client.getStatementManager().createUnparameterizedStatement( ExecuteUnparameterizedStatementRequest.newBuilder()
                            .setLanguageName( "sql" )
                            .setStatement( "UPDATE prism_identity_sqlstate SET seq = DEFAULT, u = 99" )
                            .build() );
                    RuntimeException error = assertThrows( RuntimeException.class, () -> write.execute( -1 ) );
                    assertEquals( "23505", PIService.createErrorResponse( 1, error ).getErrorResponse().getDetails().getState(), strategy.name() );
                }
            } finally {
                client.rollbackCurrentTransaction( null );
                client.getStatementManager().closeAll();
                setup.executeUpdate( "DROP TABLE prism_identity_sqlstate" );
            }
        } finally {
            RuntimeConfig.CONSTRAINT_ENFORCEMENT_STRATEGY.setEnum( previousStrategy );
            RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.setBoolean( uniqueConstraints );
        }
    }


    private static StatementResult execute( PIClient client, String query, GeneratedKeysRequest request ) {
        PIUnparameterizedStatement statement = client.getStatementManager().createUnparameterizedStatement( ExecuteUnparameterizedStatementRequest.newBuilder()
                .setLanguageName( "sql" )
                .setStatement( query )
                .setGeneratedKeys( request )
                .build() );
        return statement.execute( -1 );
    }


    private static List<Long> longs( RelationalFrame keys ) {
        return keys.getRowsList().stream().map( row -> row.getValues( 0 ).getLong().getLong() ).toList();
    }

}

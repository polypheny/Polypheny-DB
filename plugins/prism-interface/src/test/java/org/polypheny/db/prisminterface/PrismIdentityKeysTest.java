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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.prisminterface.statementProcessing.StatementProcessor;
import org.polypheny.db.prisminterface.statements.PIPreparedIndexedStatement;
import org.polypheny.db.prisminterface.statements.PIUnparameterizedStatement;
import org.polypheny.prism.BatchGeneratedKeys;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.GeneratedKeysRequest;
import org.polypheny.prism.PrepareStatementRequest;
import org.polypheny.prism.RelationalFrame;
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
                StatementBatchResponse batch = prepared.executeSequentialBatch( List.of( List.of(), List.of() ) );
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

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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.polypheny.db.prisminterface.CypherTest.integerValue;
import static org.polypheny.db.prisminterface.CypherTest.relationalFrame;
import static org.polypheny.db.prisminterface.CypherTest.singleRow;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.prism.CommitRequest;
import org.polypheny.prism.ErrorResponse;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.RollbackRequest;

@Tag("adapter")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class PrismMongoNumericErrorTest {

    private static final String TABLE = "prismmongonumeric";


    @BeforeAll
    static void startMongoDb() {
        assumeTrue( "mongodb".equalsIgnoreCase( System.getProperty( "store.default" ) ) );
        TestHelper.getInstance();
    }


    @Test
    void usesSqlNumericSemanticsAndAllowsRollback() throws IOException {
        String namespace = Catalog.DEFAULT_NAMESPACE_NAME;
        try ( Closeable cleanup = () -> dropTable( namespace ); PrismConnection connection = new PrismConnection( false, 4 ) ) {
            connection.execute( namespace, "sql", "CREATE TABLE " + TABLE + " (id INTEGER PRIMARY KEY, number_text VARCHAR(50), divisor INTEGER)", -1 );
            connection.execute( namespace, "sql", "INSERT INTO " + TABLE + " VALUES (1, '42', 2), (2, 'abc', 0)", -1 );
            Response commit = connection.send( Request.newBuilder().setCommitRequest( CommitRequest.getDefaultInstance() ) );
            assertEquals( Response.TypeCase.COMMIT_RESPONSE, commit.getTypeCase(), commit::toString );

            assertSqlErrorAndRecovery( connection, namespace, "SELECT CAST('abc' AS INTEGER) FROM " + TABLE, "22018" );
            assertSqlErrorAndRecovery( connection, namespace, "SELECT id FROM " + TABLE + " WHERE CAST(number_text AS INTEGER) = 9", "22018" );
            assertSqlErrorAndRecovery( connection, namespace, "SELECT 1/0 FROM " + TABLE, "22012" );
            assertSqlErrorAndRecovery( connection, namespace, "SELECT 8/divisor FROM " + TABLE + " WHERE id = 2", "22012" );

            assertValue( connection, namespace, "SELECT CAST('123' AS INTEGER) AS numeric_result FROM " + TABLE + " WHERE id = 1", 123 );
            assertValue( connection, namespace, "SELECT CAST(number_text AS INTEGER) AS numeric_result FROM " + TABLE + " WHERE id = 1", 42 );
            assertValue( connection, namespace, "SELECT 8/2 AS numeric_result FROM " + TABLE + " WHERE id = 1", 4 );
            assertValue( connection, namespace, "SELECT 8/divisor AS numeric_result FROM " + TABLE + " WHERE id = 1", 4 );
        }
    }


    private static void assertSqlErrorAndRecovery( PrismConnection connection, String namespace, String sql, String sqlState ) throws IOException {
        Response response = connection.send( Request.newBuilder().setExecuteUnparameterizedStatementRequest( ExecuteUnparameterizedStatementRequest.newBuilder()
                .setNamespaceName( namespace )
                .setLanguageName( "sql" )
                .setStatement( sql )
                .setFetchSize( -1 ) ) );
        assertEquals( Response.TypeCase.ERROR_RESPONSE, response.getTypeCase(), response::toString );
        ErrorResponse error = response.getErrorResponse();
        assertTrue( error.hasDetails() );
        assertEquals( sqlState, error.getDetails().getState(), error::getMessage );
        assertEquals( error.getMessage(), error.getDetails().getMessage() );

        Response rollback = connection.send( Request.newBuilder().setRollbackRequest( RollbackRequest.getDefaultInstance() ) );
        assertEquals( Response.TypeCase.ROLLBACK_RESPONSE, rollback.getTypeCase(), rollback::toString );
        assertValue( connection, namespace, "SELECT id AS numeric_result FROM " + TABLE + " WHERE id = 1", 1 );
    }


    private static void assertValue( PrismConnection connection, String namespace, String sql, int value ) throws IOException {
        Frame frame = connection.execute( namespace, "sql", sql, -1 ).getResult().getFrame();
        assertEquals( value, integerValue( singleRow( relationalFrame( frame, true, List.of( "numeric_result" ) ) ).getValues( 0 ) ) );
    }


    private static void dropTable( String namespace ) throws IOException {
        try ( PrismConnection cleanup = new PrismConnection( true, 4 ) ) {
            cleanup.execute( namespace, "sql", "DROP TABLE IF EXISTS " + TABLE, -1 );
        }
    }

}

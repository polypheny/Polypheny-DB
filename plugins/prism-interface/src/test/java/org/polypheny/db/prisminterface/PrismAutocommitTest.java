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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.prism.CloseResultRequest;
import org.polypheny.prism.CloseStatementRequest;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.StatementResponse;

@Tag("adapter")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class PrismAutocommitTest {

    @BeforeAll
    static void startDatabase() {
        TestHelper.getInstance();
    }


    @Test
    void sqlCompletesTheTransactionAfterTheLastCursor() throws IOException {
        verifyCursorLifecycle( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "SELECT n FROM (VALUES (1), (2)) AS pairs(n)" );
    }


    @Test
    void mqlCompletesTheTransactionAfterTheLastCursor() throws IOException {
        String namespace = "prismautocommitdocs";
        try {
            try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE DOCUMENT NAMESPACE " + namespace, -1 );
                connection.execute( namespace, "mongo", "db.createCollection(\"pairs\")", -1 );
                connection.execute( namespace, "mongo", "db.pairs.insertOne({value:1})", -1 );
                connection.execute( namespace, "mongo", "db.pairs.insertOne({value:2})", -1 );
            }
            verifyCursorLifecycle( namespace, "mongo", "db.pairs.find({})" );
        } finally {
            try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
                connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "DROP NAMESPACE IF EXISTS " + namespace, -1 );
            }
        }
    }


    private static void verifyCursorLifecycle( String namespace, String language, String query ) throws IOException {
        long initiallyActive = activeTransactions();
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            int paged = openCursor( connection, namespace, language, query );
            assertEquals( initiallyActive + 1, activeTransactions() );
            assertTrue( connection.fetch( paged, 1 ).getIsLast() );
            assertEquals( initiallyActive, activeTransactions() );

            int closedResult = openCursor( connection, namespace, language, query );
            closeResult( connection, closedResult );
            assertEquals( initiallyActive, activeTransactions() );

            int closedStatement = openCursor( connection, namespace, language, query );
            Response closeStatement = connection.send( Request.newBuilder().setCloseStatementRequest(
                    CloseStatementRequest.newBuilder().setStatementId( closedStatement ) ) );
            assertEquals( Response.TypeCase.CLOSE_STATEMENT_RESPONSE, closeStatement.getTypeCase() );
            assertEquals( initiallyActive, activeTransactions() );

            int first = openCursor( connection, namespace, language, query );
            int second = openCursor( connection, namespace, language, query );
            assertTrue( connection.execute( namespace, language, query, -1 ).getResult().getFrame().getIsLast() );
            closeResult( connection, first );
            assertEquals( initiallyActive + 1, activeTransactions() );
            assertTrue( connection.fetch( second, 1 ).getIsLast() );
            assertEquals( initiallyActive, activeTransactions() );
        }

        try ( PrismConnection connection = new PrismConnection( false, 3 ) ) {
            int paged = openCursor( connection, namespace, language, query );
            assertTrue( connection.fetch( paged, 1 ).getIsLast() );
            assertEquals( initiallyActive + 1, activeTransactions() );

            int explicitlyClosed = openCursor( connection, namespace, language, query );
            closeResult( connection, explicitlyClosed );
            assertEquals( initiallyActive + 1, activeTransactions() );
        }
        assertEquals( initiallyActive, activeTransactions() );
    }


    private static long activeTransactions() {
        return TestHelper.getInstance().getTransactionManager().getNumberOfActiveTransactions();
    }


    private static int openCursor( PrismConnection connection, String namespace, String language, String query ) throws IOException {
        StatementResponse response = connection.execute( namespace, language, query, 1 );
        assertTrue( response.getResult().hasFrame() );
        assertFalse( response.getResult().getFrame().getIsLast() );
        return response.getStatementId();
    }


    private static void closeResult( PrismConnection connection, int statementId ) throws IOException {
        Response response = connection.send( Request.newBuilder().setCloseResultRequest(
                CloseResultRequest.newBuilder().setStatementId( statementId ) ) );
        assertEquals( Response.TypeCase.CLOSE_RESULT_RESPONSE, response.getTypeCase() );
    }

}

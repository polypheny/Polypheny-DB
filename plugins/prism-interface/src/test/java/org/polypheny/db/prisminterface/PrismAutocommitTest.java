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

import java.io.Closeable;
import java.io.IOException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.prism.ClientInfoProperties;
import org.polypheny.prism.ClientInfoPropertiesRequest;
import org.polypheny.prism.CloseResultRequest;
import org.polypheny.prism.CloseStatementRequest;
import org.polypheny.prism.CommitRequest;
import org.polypheny.prism.ConnectionProperties;
import org.polypheny.prism.ConnectionPropertiesUpdateRequest;
import org.polypheny.prism.ExecuteIndexedStatementBatchRequest;
import org.polypheny.prism.ExecuteIndexedStatementRequest;
import org.polypheny.prism.ExecuteNamedStatementRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.IndexedParameters;
import org.polypheny.prism.NamedParameters;
import org.polypheny.prism.PrepareStatementRequest;
import org.polypheny.prism.ProtoInteger;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.RollbackRequest;
import org.polypheny.prism.StatementResponse;

@Tag("adapter")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class PrismAutocommitTest {

    private static final String SQL_NAMESPACE = Catalog.DEFAULT_NAMESPACE_NAME;
    private static final String SQL_PAGE_QUERY = "SELECT n FROM (VALUES (1), (2)) AS pairs(n)";


    @BeforeAll
    static void startDatabase() {
        TestHelper.getInstance();
    }


    @Test
    void sqlCompletesTheTransactionAfterTheLastCursor() throws IOException {
        verifyCursorLifecycle( SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );
    }


    @Test
    void failedOverlappingReadsLeaveTheOtherCursorUsable() throws Exception {
        String table = "prismautocommitzero";
        String store = "prismautocommitreadfailure";
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement adapterManagement = jdbc.getConnection().createStatement() ) {
            TestHelper.addHsqldb( store, adapterManagement );
            try ( AutoCloseable removeStore = () -> TestHelper.dropAdapter( store, adapterManagement );
                    Closeable removeTable = () -> dropSqlFixtures( table );
                    PrismConnection connection = new PrismConnection( true, 3 ) ) {
                connection.execute( SQL_NAMESPACE, "sql", "CREATE TABLE " + table
                        + " (id INTEGER NOT NULL, PRIMARY KEY (id)) ON STORE " + store, -1 );
                connection.execute( SQL_NAMESPACE, "sql", "INSERT INTO " + table + " VALUES (0), (1)", -1 );
                String pagedRead = "SELECT id FROM " + table + " ORDER BY id";
                long initiallyActive = activeTransactions();

                int first = openCursor( connection, SQL_NAMESPACE, "sql", pagedRead );
                Request.Builder direct = statementRequest( SQL_NAMESPACE, "sql", "SELECT 10 / id AS quotient FROM " + table );
                assertReadExecutionError( connection.send( direct ) );
                assertEquals( initiallyActive + 1, activeTransactions() );
                assertLastSqlPage( connection, first );
                assertEquals( initiallyActive, activeTransactions() );

                int second = openCursor( connection, SQL_NAMESPACE, "sql", pagedRead );
                int prepared = prepareStatement( connection, "SELECT CAST(? AS INTEGER) / id AS quotient FROM " + table, false );
                Request.Builder indexed = Request.newBuilder().setExecuteIndexedStatementRequest(
                        ExecuteIndexedStatementRequest.newBuilder().setStatementId( prepared )
                                .setParameters( IndexedParameters.newBuilder().addParameters( integer( 10 ) ) ) );
                assertReadExecutionError( connection.send( indexed ) );
                assertEquals( initiallyActive + 1, activeTransactions() );
                assertLastSqlPage( connection, second );
                assertEquals( initiallyActive, activeTransactions() );

                assertReadExecutionError( connection.send( direct ) );
                assertEquals( initiallyActive, activeTransactions() );
                assertPersistedSqlRows( table, 2 );
            }
        }
    }


    @Test
    void rollbackAndDisconnectReleaseAllOverlappingReadTransactions() throws IOException {
        long initiallyActive = activeTransactions();
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            int first = openCursor( connection, SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );
            int second = openCursor( connection, SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );
            assertEquals( initiallyActive + 2, activeTransactions() );
            expect( connection.send( Request.newBuilder().setRollbackRequest( RollbackRequest.getDefaultInstance() ) ),
                    Response.TypeCase.ROLLBACK_RESPONSE );
            assertEquals( initiallyActive, activeTransactions() );
            expect( connection.fetchResponse( first, 1 ), Response.TypeCase.ERROR_RESPONSE );
            expect( connection.fetchResponse( second, 1 ), Response.TypeCase.ERROR_RESPONSE );

            openCursor( connection, SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );
            openCursor( connection, SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );
            assertEquals( initiallyActive + 2, activeTransactions() );
        }
        assertEquals( initiallyActive, activeTransactions() );
    }


    @Test
    void publishesImplicitManualTransactionBoundariesOnlyForOptedInClients() throws IOException {
        String table = "prismautocommitboundary";
        try ( PrismConnection connection = new PrismConnection( false, 5 ) ) {
            openCursor( connection, SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );
            List<Response.TypeCase> intermediateTypes = new ArrayList<>();
            Response withoutOptIn = connection.send( statementRequest( SQL_NAMESPACE, "sql", "DROP TABLE IF EXISTS " + table ),
                    response -> intermediateTypes.add( response.getTypeCase() ) );
            expect( withoutOptIn, Response.TypeCase.STATEMENT_RESPONSE );
            assertEquals( List.of( Response.TypeCase.STATEMENT_RESPONSE ), intermediateTypes );

            enableTransactionBoundaryEvents( connection );
            int cursor = openCursor( connection, SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );
            intermediateTypes.clear();
            Response committed = connection.send( statementRequest( SQL_NAMESPACE, "sql", "DROP TABLE IF EXISTS " + table ),
                    response -> intermediateTypes.add( response.getTypeCase() ) );
            expect( committed, Response.TypeCase.STATEMENT_RESPONSE );
            assertEquals( List.of( Response.TypeCase.STATEMENT_RESPONSE, Response.TypeCase.COMMIT_RESPONSE ), intermediateTypes );
            expect( connection.fetchResponse( cursor, 1 ), Response.TypeCase.ERROR_RESPONSE );

            cursor = openCursor( connection, SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );
            intermediateTypes.clear();
            Response rolledBack = connection.send( statementRequest( SQL_NAMESPACE, "sql", "SELECT * FROM " + table ),
                    response -> intermediateTypes.add( response.getTypeCase() ) );
            expect( rolledBack, Response.TypeCase.ERROR_RESPONSE );
            assertEquals( "42P01", rolledBack.getErrorResponse().getDetails().getState() );
            assertEquals( List.of( Response.TypeCase.STATEMENT_RESPONSE, Response.TypeCase.ROLLBACK_RESPONSE ), intermediateTypes );
            expect( connection.fetchResponse( cursor, 1 ), Response.TypeCase.ERROR_RESPONSE );
        }
    }


    @Test
    void explicitCommitInAutocommitClosesPagedResultsBeforeChangingMode() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 5 ) ) {
            enableTransactionBoundaryEvents( connection );
            int first = openCursor( connection, SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );
            int second = openCursor( connection, SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );
            List<Response.TypeCase> intermediateTypes = new ArrayList<>();
            expect( connection.send( statementRequest( SQL_NAMESPACE, "sql", SQL_PAGE_QUERY ),
                            response -> intermediateTypes.add( response.getTypeCase() ) ), Response.TypeCase.STATEMENT_RESPONSE );
            assertEquals( List.of( Response.TypeCase.STATEMENT_RESPONSE ), intermediateTypes );
            assertLastSqlPage( connection, first );
            int third = openCursor( connection, SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );

            intermediateTypes.clear();
            expect( connection.send( Request.newBuilder().setCommitRequest( CommitRequest.getDefaultInstance() ),
                            response -> intermediateTypes.add( response.getTypeCase() ) ),
                    Response.TypeCase.COMMIT_RESPONSE );
            assertTrue( intermediateTypes.isEmpty() );
            expect( connection.send( Request.newBuilder().setConnectionPropertiesUpdateRequest( ConnectionPropertiesUpdateRequest.newBuilder()
                            .setConnectionProperties( ConnectionProperties.newBuilder().setIsAutoCommit( false ) ) ) ),
                    Response.TypeCase.CONNECTION_PROPERTIES_UPDATE_RESPONSE );
            expect( connection.fetchResponse( second, 1 ), Response.TypeCase.ERROR_RESPONSE );
            expect( connection.fetchResponse( third, 1 ), Response.TypeCase.ERROR_RESPONSE );
        }
    }


    @Test
    void failedExplicitCommitInAutocommitReportsRollbackToOptedInClients() throws IOException {
        String parent = "prismautocommitparent";
        String child = "prismautocommitchild";
        boolean enforced = RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean();
        RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.setBoolean( true );
        try ( PrismConnection connection = new PrismConnection( true, 5 ) ) {
            createSqlFixture( connection, parent );
            connection.execute( SQL_NAMESPACE, "sql", "CREATE TABLE " + child + " (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL)", -1 );
            connection.execute( SQL_NAMESPACE, "sql", "ALTER TABLE " + child + " ADD CONSTRAINT fk_autocommit_parent FOREIGN KEY (parent_id) REFERENCES "
                    + parent + " (id) ON UPDATE RESTRICT ON DELETE RESTRICT", -1 );

            assertFailedExplicitCommit( child, 5, true, List.of( Response.TypeCase.ROLLBACK_RESPONSE ) );
            assertFailedExplicitCommit( child, 5, false, List.of() );
            assertFailedExplicitCommit( child, 4, true, List.of() );
            assertPersistedSqlRows( child, 0 );
        } finally {
            try {
                dropSqlFixtures( child, parent );
            } finally {
                RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.setBoolean( enforced );
            }
        }
    }


    private static void assertFailedExplicitCommit( String table, int clientMinor, boolean optedIn, List<Response.TypeCase> expectedEvents ) throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, clientMinor ) ) {
            if ( optedIn ) {
                enableTransactionBoundaryEvents( connection );
            }
            int cursor = openCursor( connection, SQL_NAMESPACE, "sql", "INSERT INTO " + table + " VALUES (8, 999), (9, 999) RETURNING id" );
            List<Response.TypeCase> intermediateTypes = new ArrayList<>();
            Response failure = connection.send( Request.newBuilder().setCommitRequest( CommitRequest.getDefaultInstance() ),
                    response -> intermediateTypes.add( response.getTypeCase() ) );
            expect( failure, Response.TypeCase.ERROR_RESPONSE );
            assertEquals( "23503", failure.getErrorResponse().getDetails().getState() );
            assertEquals( expectedEvents, intermediateTypes );
            expect( connection.fetchResponse( cursor, 1 ), Response.TypeCase.ERROR_RESPONSE );
        }
    }


    private static void enableTransactionBoundaryEvents( PrismConnection connection ) throws IOException {
        expect( connection.send( Request.newBuilder().setSetClientInfoPropertiesRequest( ClientInfoProperties.newBuilder()
                        .putProperties( PIClientInfoProperties.TRANSACTION_BOUNDARY_EVENTS, "true" ) ) ),
                Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
    }


    @Test
    void optedInClientCanRemoveOneClientInfoProperty() throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 5 ) ) {
            Request.Builder read = Request.newBuilder().setClientInfoPropertiesRequest( ClientInfoPropertiesRequest.getDefaultInstance() );
            String removal = PIClientInfoProperties.REMOVE_CLIENT_INFO_PROPERTY;
            assertFalse( expect( connection.send( read ), Response.TypeCase.CLIENT_INFO_PROPERTIES_RESPONSE )
                    .getClientInfoPropertiesResponse().containsProperties( removal ) );
            enableTransactionBoundaryEvents( connection );
            assertTrue( expect( connection.send( read ), Response.TypeCase.CLIENT_INFO_PROPERTIES_RESPONSE )
                    .getClientInfoPropertiesResponse().containsProperties( removal ) );

            expect( connection.send( Request.newBuilder().setSetClientInfoPropertiesRequest( ClientInfoProperties.newBuilder()
                            .putProperties( "ApplicationName", "importer" ).putProperties( "ClientUser", "operator" ) ) ),
                    Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            expect( connection.send( Request.newBuilder().setSetClientInfoPropertiesRequest( ClientInfoProperties.newBuilder()
                            .putProperties( removal, "ApplicationName" ) ) ),
                    Response.TypeCase.SET_CLIENT_INFO_PROPERTIES_RESPONSE );
            ClientInfoProperties result = expect( connection.send( read ), Response.TypeCase.CLIENT_INFO_PROPERTIES_RESPONSE )
                    .getClientInfoPropertiesResponse();
            assertFalse( result.containsProperties( "ApplicationName" ) );
            assertEquals( "operator", result.getPropertiesOrThrow( "ClientUser" ) );
        }
    }


    @Test
    void rejectsSqlWritesAndDdlUntilTheOpenResultFinishes() throws IOException {
        String table = "prismautocommitwrites";
        String ddlTable = "prismautocommitddl";
        String createTable = "CREATE TABLE " + ddlTable + " (id INTEGER NOT NULL, PRIMARY KEY (id))";
        String insert = "INSERT INTO " + table + " VALUES (41)";
        try {
            try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
                createSqlFixture( connection, table );
                long initiallyActive = activeTransactions();
                int first = openCursor( connection, SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );
                int paged = openCursor( connection, SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );
                assertEquals( initiallyActive + 2, activeTransactions() );
                assertLastSqlPage( connection, first );
                assertEquals( initiallyActive + 1, activeTransactions() );
                assertOpenResultError( connection.send( statementRequest( SQL_NAMESPACE, "sql", insert ) ) );
                assertOpenResultError( connection.send( statementRequest( SQL_NAMESPACE, "sql", createTable ) ) );
                assertEquals( initiallyActive + 1, activeTransactions() );
                assertLastSqlPage( connection, paged );
                assertEquals( initiallyActive, activeTransactions() );

                assertEquals( 1, connection.execute( SQL_NAMESPACE, "sql", createTable, -1 ).getResult().getScalar() );
                assertEquals( 1, connection.execute( SQL_NAMESPACE, "sql", insert, -1 ).getResult().getScalar() );
            }

            assertPersistedSqlRows( table, 1 );
        } finally {
            dropSqlFixtures( table, ddlTable );
        }
    }


    @Test
    void rejectsMqlWritesUntilTheOpenResultFinishes() throws IOException {
        String namespace = "prismautocommitmqlwrites";
        String insert = "db.pairs.insertOne({value:41})";
        try {
            try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
                connection.execute( SQL_NAMESPACE, "sql", "CREATE DOCUMENT NAMESPACE " + namespace, -1 );
                connection.execute( namespace, "mongo", "db.createCollection(\"pairs\")", -1 );
                long initiallyActive = activeTransactions();
                int paged = openCursor( connection, SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );
                assertOpenResultError( connection.send( statementRequest( namespace, "mongo", insert ) ) );
                assertOpenResultError( connection.send( statementRequest( namespace, "mongo", "db.missing.find({})" ) ) );
                assertEquals( initiallyActive + 1, activeTransactions() );
                assertLastSqlPage( connection, paged );
                assertEquals( initiallyActive, activeTransactions() );

                Frame update = connection.execute( namespace, "mongo", insert, -1 ).getResult().getFrame();
                assertTrue( update.hasDocumentFrame() );
                assertTrue( update.getIsLast() );
                assertEquals( 1, update.getDocumentFrame().getDocumentsCount() );
                assertEquals( 1, connection.execute( namespace, "mongo", "db.createCollection(\"missing\")", -1 ).getResult().getScalar() );

                StatementResponse pendingWrite = connection.execute( namespace, "mongo", "db.pairs.insertOne({value:42})", 0 );
                Frame emptyPage = pendingWrite.getResult().getFrame();
                assertTrue( emptyPage.hasDocumentFrame() );
                assertFalse( emptyPage.getIsLast() );
                assertEquals( 0, emptyPage.getDocumentFrame().getDocumentsCount() );
                assertOpenResultError( connection.send( statementRequest( SQL_NAMESPACE, "sql", SQL_PAGE_QUERY ) ) );
                Frame writeCount = connection.fetch( pendingWrite.getStatementId(), 1 );
                assertTrue( writeCount.getIsLast() );
                assertEquals( 1, writeCount.getDocumentFrame().getDocumentsCount() );
                assertEquals( initiallyActive, activeTransactions() );
            }

            try ( PrismConnection observer = new PrismConnection( true, 3 ) ) {
                Frame docs = observer.execute( namespace, "mongo", "db.pairs.find({})", -1 ).getResult().getFrame();
                assertTrue( docs.hasDocumentFrame() );
                assertEquals( 2, docs.getDocumentFrame().getDocumentsCount() );
            }
        } finally {
            try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
                connection.execute( SQL_NAMESPACE, "sql", "DROP NAMESPACE IF EXISTS " + namespace, -1 );
            }
        }
    }


    @Test
    void rejectsPreparedAndBatchedWritesUntilTheOpenResultFinishes() throws IOException {
        String table = "prismautocommitpreparedwrites";
        String insert = "INSERT INTO " + table + " VALUES ";
        try {
            try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
                createSqlFixture( connection, table );
                int indexedId = prepareStatement( connection, insert + "(?)", false );
                int namedId = prepareStatement( connection, insert + "(:id)", true );
                Request.Builder indexed = Request.newBuilder().setExecuteIndexedStatementRequest(
                        ExecuteIndexedStatementRequest.newBuilder().setStatementId( indexedId )
                                .setParameters( IndexedParameters.newBuilder().addParameters( integer( 51 ) ) ) );
                Request.Builder named = Request.newBuilder().setExecuteNamedStatementRequest(
                        ExecuteNamedStatementRequest.newBuilder().setStatementId( namedId )
                                .setParameters( NamedParameters.newBuilder().putParameters( "id", integer( 52 ) ) ) );
                Request.Builder indexedBatch = Request.newBuilder().setExecuteIndexedStatementBatchRequest(
                        ExecuteIndexedStatementBatchRequest.newBuilder().setStatementId( indexedId )
                                .addParameters( IndexedParameters.newBuilder().addParameters( integer( 53 ) ) ) );
                Request.Builder batch = Request.newBuilder().setExecuteUnparameterizedStatementBatchRequest(
                        ExecuteUnparameterizedStatementBatchRequest.newBuilder()
                                .addStatements( statementRequest( SQL_NAMESPACE, "sql", insert + "(54)" ).getExecuteUnparameterizedStatementRequest() ) );

                int paged = openCursor( connection, SQL_NAMESPACE, "sql", SQL_PAGE_QUERY );
                for ( Request.Builder request : List.of( indexed, named, indexedBatch, batch ) ) {
                    assertOpenResultError( connection.send( request ) );
                }
                assertLastSqlPage( connection, paged );

                assertEquals( 1, expect( connection.send( indexed ), Response.TypeCase.STATEMENT_RESULT ).getStatementResult().getScalar() );
                assertEquals( 1, expect( connection.send( named ), Response.TypeCase.STATEMENT_RESULT ).getStatementResult().getScalar() );
                assertEquals( List.of( 1L ), expect( connection.send( indexedBatch ), Response.TypeCase.STATEMENT_BATCH_RESPONSE ).getStatementBatchResponse().getScalarsList() );
                assertEquals( List.of( 1L ), expect( connection.send( batch ), Response.TypeCase.STATEMENT_BATCH_RESPONSE ).getStatementBatchResponse().getScalarsList() );
            }

            assertPersistedSqlRows( table, 4 );
        } finally {
            dropSqlFixtures( table );
        }
    }


    private static void createSqlFixture( PrismConnection connection, String table ) throws IOException {
        connection.execute( SQL_NAMESPACE, "sql", "CREATE TABLE " + table + " (id INTEGER NOT NULL, PRIMARY KEY (id))", -1 );
    }


    private static void dropSqlFixtures( String... tables ) throws IOException {
        try ( PrismConnection connection = new PrismConnection( true, 3 ) ) {
            for ( String table : tables ) {
                connection.execute( SQL_NAMESPACE, "sql", "DROP TABLE IF EXISTS " + table, -1 );
            }
        }
    }


    private static void assertPersistedSqlRows( String table, int expected ) throws IOException {
        try ( PrismConnection observer = new PrismConnection( true, 3 ) ) {
            Frame rows = observer.execute( SQL_NAMESPACE, "sql", "SELECT id FROM " + table, -1 ).getResult().getFrame();
            assertTrue( rows.hasRelationalFrame() );
            assertEquals( expected, rows.getRelationalFrame().getRowsCount() );
        }
    }


    private static int prepareStatement( PrismConnection connection, String query, boolean named ) throws IOException {
        PrepareStatementRequest preparation = PrepareStatementRequest.newBuilder()
                .setLanguageName( "sql" )
                .setNamespaceName( SQL_NAMESPACE )
                .setStatement( query )
                .build();
        Request.Builder request = Request.newBuilder();
        if ( named ) {
            request.setPrepareNamedStatementRequest( preparation );
        } else {
            request.setPrepareIndexedStatementRequest( preparation );
        }
        return expect( connection.send( request ), Response.TypeCase.PREPARED_STATEMENT_SIGNATURE ).getPreparedStatementSignature().getStatementId();
    }


    private static ProtoValue integer( int value ) {
        return ProtoValue.newBuilder().setInteger( ProtoInteger.newBuilder().setInteger( value ) ).build();
    }


    private static Request.Builder statementRequest( String namespace, String language, String query ) {
        return Request.newBuilder().setExecuteUnparameterizedStatementRequest( ExecuteUnparameterizedStatementRequest.newBuilder()
                .setNamespaceName( namespace )
                .setLanguageName( language )
                .setStatement( query )
                .setFetchSize( -1 ) );
    }


    private static void assertLastSqlPage( PrismConnection connection, int statementId ) throws IOException {
        Frame last = connection.fetch( statementId, 1 );
        assertTrue( last.hasRelationalFrame() );
        assertTrue( last.getIsLast() );
        assertEquals( 1, last.getRelationalFrame().getRowsCount() );
    }


    private static void assertOpenResultError( Response response ) {
        String message = expect( response, Response.TypeCase.ERROR_RESPONSE ).getErrorResponse().getMessage();
        assertTrue( message.contains( "open result" ) && message.contains( "autocommit" ), message );
    }


    private static void assertReadExecutionError( Response response ) {
        String message = expect( response, Response.TypeCase.ERROR_RESPONSE ).getErrorResponse().getMessage();
        assertTrue( message.contains( "While executing SQL" ), message );
    }


    private static Response expect( Response response, Response.TypeCase type ) {
        assertEquals( type, response.getTypeCase(), () -> response.getErrorResponse().getMessage() );
        return response;
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
            assertEquals( initiallyActive + 2, activeTransactions() );
            assertTrue( connection.execute( namespace, language, query, -1 ).getResult().getFrame().getIsLast() );
            assertEquals( initiallyActive + 2, activeTransactions() );
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

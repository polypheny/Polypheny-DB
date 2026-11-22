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
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.polypheny.db.prisminterface.CypherTest.relationalFrame;
import static org.polypheny.db.prisminterface.CypherTest.singleRow;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.polypheny.db.TestHelper;
import org.polypheny.db.TestHelper.JdbcConnection;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.prisminterface.CypherTest.PrismConnection;
import org.polypheny.db.prisminterface.utils.PrismValueDeserializer;
import org.polypheny.prism.CommitRequest;
import org.polypheny.prism.ErrorResponse;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.ProtoDocument;
import org.polypheny.prism.ProtoPolyType;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.RollbackRequest;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;

@Tag("adapter")
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class PrismResultTest {

    @BeforeAll
    static void startDatabase() {
        TestHelper.getInstance();
    }


    @Test
    void sqlPreservesUpdateCountsAndEmptyAndPagedRowMetadata() throws IOException {
        String namespace = Catalog.DEFAULT_NAMESPACE_NAME;
        try ( Closeable cleanup = () -> dropFixture( "DROP TABLE IF EXISTS prismresultrows" );
                PrismConnection connection = new PrismConnection( true ) ) {
            assertScalar( connection.execute( namespace, "sql", "CREATE TABLE prismresultrows (id INTEGER NOT NULL, PRIMARY KEY (id))", 1 ).getResult(), 1 );
            assertScalar( connection.execute( namespace, "sql", "INSERT INTO prismresultrows VALUES (7), (8)", 1 ).getResult(), 2 );
            assertScalar( connection.execute( namespace, "sql", "DELETE FROM prismresultrows WHERE id = 9", 1 ).getResult(), 0 );

            Frame empty = connection.execute( namespace, "sql",
                    "SELECT id AS identifier FROM (VALUES (7), (8)) AS fixture(id) WHERE id = 9", 1 ).getResult().getFrame();
            RelationalFrame emptyRows = relationalFrame( empty, true, List.of( "identifier" ) );
            assertEquals( 0, emptyRows.getRowsCount() );
            assertEquals( ProtoPolyType.INTEGER, emptyRows.getColumnMeta( 0 ).getTypeMeta().getProtoValueType() );

            Frame count = connection.execute( namespace, "sql",
                    "SELECT COUNT(*) AS total FROM (VALUES (7), (8)) AS fixture(id)", 1 ).getResult().getFrame();
            assertEquals( 2, integerValue( singleRow( relationalFrame( count, true, List.of( "total" ) ) ).getValues( 0 ) ) );

            StatementResponse select = connection.execute( namespace, "sql", "SELECT id FROM prismresultrows ORDER BY id", 1 );
            RelationalFrame first = relationalFrame( select.getResult().getFrame(), false, List.of( "id" ) );
            RelationalFrame last = relationalFrame( connection.fetch( select.getStatementId(), 1 ), true, List.of( "id" ) );
            assertEquals( 7, integerValue( singleRow( first ).getValues( 0 ) ) );
            assertEquals( 8, integerValue( singleRow( last ).getValues( 0 ) ) );
            assertEquals( first.getColumnMetaList(), last.getColumnMetaList() );
        }
    }


    @Test
    void mqlKeepsItsDocumentFramesAndLegacyUpdateCount() throws IOException {
        String namespace = "prismresultdocs";
        try ( Closeable cleanup = () -> dropFixture( "DROP NAMESPACE IF EXISTS prismresultdocs" );
                PrismConnection connection = new PrismConnection( true, 3 ) ) {
            connection.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", "CREATE DOCUMENT NAMESPACE " + namespace, 1 );
            assertScalar( connection.execute( namespace, "mongo", "db.createCollection(\"entries\")", 1 ).getResult(), 1 );
            for ( String name : List.of( "Alice", "Bob" ) ) {
                Frame inserted = connection.execute( namespace, "mongo", "db.entries.insertOne({name:'" + name + "'})", 1 ).getResult().getFrame();
                ProtoDocument update = singleDocument( inserted, true );
                // Depending on the plan, Neo4j may already return a document with the change count.
                String countField = "neo4j".equalsIgnoreCase( System.getProperty( "store.default" ) ) && update.containsEntries( "value" )
                        ? "value"
                        : "updateCount";
                assertEquals( Set.of( countField ), update.getEntriesMap().keySet() );
                assertEquals( 1, integerValue( update.getEntriesOrThrow( countField ) ) );
            }

            Frame empty = connection.execute( namespace, "mongo", "db.entries.find({name:'missing'})", 1 ).getResult().getFrame();
            assertTrue( empty.hasDocumentFrame() );
            assertTrue( empty.getIsLast() );
            assertEquals( 0, empty.getDocumentFrame().getDocumentsCount() );

            Frame count = connection.execute( namespace, "mongo", "db.entries.aggregate([{\"$count\":\"total\"}])", 1 ).getResult().getFrame();
            ProtoDocument aggregate = singleDocument( count, true );
            assertFalse( aggregate.containsEntries( "updateCount" ) );
            assertEquals( 2, integerValue( aggregate.getEntriesOrThrow( "total" ) ) );

            Frame documentsAsRows = connection.execute(
                    Catalog.DEFAULT_NAMESPACE_NAME, "sql", "SELECT d AS payload FROM prismresultdocs.entries", -1 ).getResult().getFrame();
            RelationalFrame rows = relationalFrame( documentsAsRows, true, List.of( "payload" ) );
            assertEquals( 2, rows.getRowsCount() );
            assertEquals( Set.of( "Alice", "Bob" ), Set.of(
                    rows.getRows( 0 ).getValues( 0 ).getDocument().getEntriesOrThrow( "name" ).getString().getString(),
                    rows.getRows( 1 ).getValues( 0 ).getDocument().getEntriesOrThrow( "name" ).getString().getString() ) );

            StatementResponse select = connection.execute( namespace, "mongo", "db.entries.find({})", 1 );
            ProtoDocument first = singleDocument( select.getResult().getFrame(), false );
            ProtoDocument last = singleDocument( connection.fetch( select.getStatementId(), 1 ), true );
            assertEquals( Set.of( "Alice", "Bob" ), Set.of(
                    first.getEntriesOrThrow( "name" ).getString().getString(),
                    last.getEntriesOrThrow( "name" ).getString().getString() ) );
        }
    }


    @Test
    void mqlOverRelationalStorageUsesADocumentFrame() throws Exception {
        String namespace = Catalog.DEFAULT_NAMESPACE_NAME;
        String store = "prismresultrelationalstore";
        try ( JdbcConnection jdbc = new JdbcConnection( true ); Statement adapterManagement = jdbc.getConnection().createStatement() ) {
            TestHelper.addHsqldb( store, adapterManagement );
            try ( AutoCloseable removeStore = () -> TestHelper.dropAdapter( store, adapterManagement );
                    Closeable removeTable = () -> dropFixture( "DROP TABLE IF EXISTS prismresultcrossmodel" );
                    PrismConnection connection = new PrismConnection( true ) ) {
                connection.execute( namespace, "sql",
                        "CREATE TABLE prismresultcrossmodel (id INTEGER NOT NULL, PRIMARY KEY (id)) ON STORE " + store, 1 );
                assertScalar( connection.execute( namespace, "sql", "INSERT INTO prismresultcrossmodel VALUES (7)", 1 ).getResult(), 1 );
                Frame document = connection.execute( namespace, "mongo", "db.prismresultcrossmodel.find({id:7})", 1 ).getResult().getFrame();
                assertEquals( 7, integerValue( singleDocument( document, true ).getEntriesOrThrow( "id" ) ) );
            }
        }
    }


    @Test
    void sqlDdlErrorsPreserveLegacyMessagesAndAllowRollback() throws IOException {
        String namespace = Catalog.DEFAULT_NAMESPACE_NAME;
        try ( Closeable cleanup = () -> {
            dropFixture( "DROP TABLE IF EXISTS prismresultddlcopy" );
            dropFixture( "DROP TABLE IF EXISTS prismresultddlerrors" );
        }; PrismConnection connection = new PrismConnection( false, 4 ) ) {
            connection.execute( namespace, "sql", "CREATE TABLE prismresultddlerrors (id INTEGER PRIMARY KEY)", 1 );

            assertSqlError( connection, "DROP TABLE prismresultmissing", "42P01",
                    "There exists no entity with the name prismresultmissing and 'IF EXISTS' was not specified" );
            assertSqlError( connection, "ALTER TABLE prismresultmissing ADD COLUMN v INTEGER", "42P01",
                    "Could not find relational entity with name: prismresultmissing" );
            assertSqlError( connection, "CREATE TABLE prismresultddlcopy AS SELECT * FROM prismresultddlerrors", "0A000", "Not yet supported" );

            connection.execute( namespace, "sql", "DROP TABLE IF EXISTS prismresultmissing", 1 );
            Frame frame = connection.execute( namespace, "sql", "SELECT id FROM prismresultddlerrors", -1 ).getResult().getFrame();
            assertEquals( 0, relationalFrame( frame, true, List.of( "id" ) ).getRowsCount() );
        }
    }


    @Test
    void nativeModByZeroPreservesLegacyMessageAndAllowsRollback() throws IOException {
        assumeTrue( "file".equalsIgnoreCase( System.getProperty( "store.default" ) ) );
        String namespace = Catalog.DEFAULT_NAMESPACE_NAME;
        try ( Closeable cleanup = () -> dropFixture( "DROP TABLE IF EXISTS prismresultzerodivisor" );
                PrismConnection connection = new PrismConnection( false, 4 ) ) {
            connection.execute( namespace, "sql", "CREATE TABLE prismresultzerodivisor (id INTEGER PRIMARY KEY)", 1 );
            connection.execute( namespace, "sql", "INSERT INTO prismresultzerodivisor VALUES (1)", 1 );
            Response commit = connection.send( Request.newBuilder().setCommitRequest( CommitRequest.getDefaultInstance() ) );
            assertEquals( Response.TypeCase.COMMIT_RESPONSE, commit.getTypeCase() );

            assertSqlError( connection, "SELECT MOD(id, 0) FROM prismresultzerodivisor", "22012", "java.lang.ArithmeticException: Division by zero" );

            Frame frame = connection.execute( namespace, "sql", "SELECT id FROM prismresultzerodivisor", -1 ).getResult().getFrame();
            assertEquals( 1, integerValue( singleRow( relationalFrame( frame, true, List.of( "id" ) ) ).getValues( 0 ) ) );
        }
    }


    @Test
    void neo4jUsesSqlNumericSemanticsAndAllowsRollback() throws IOException {
        assumeTrue( "neo4j".equalsIgnoreCase( System.getProperty( "store.default" ) ) );
        String namespace = Catalog.DEFAULT_NAMESPACE_NAME;
        try ( Closeable cleanup = () -> dropFixture( "DROP TABLE IF EXISTS prismneonumeric" );
                PrismConnection connection = new PrismConnection( false, 4 ) ) {
            connection.execute( namespace, "sql", "CREATE TABLE prismneonumeric (id INTEGER PRIMARY KEY, number_text VARCHAR(50), divisor INTEGER)", 1 );
            connection.execute( namespace, "sql", "INSERT INTO prismneonumeric VALUES (1, '42', 2), (2, 'abc', 0)", 1 );
            Response commit = connection.send( Request.newBuilder().setCommitRequest( CommitRequest.getDefaultInstance() ) );
            assertEquals( Response.TypeCase.COMMIT_RESPONSE, commit.getTypeCase() );

            sqlError( connection, "SELECT CAST('abc' AS INTEGER) FROM prismneonumeric", "22018" );
            Frame cast = connection.execute( namespace, "sql", "SELECT CAST(number_text AS INTEGER) AS converted FROM prismneonumeric WHERE id = 1", -1 ).getResult().getFrame();
            assertEquals( 42, integerValue( singleRow( relationalFrame( cast, true, List.of( "converted" ) ) ).getValues( 0 ) ) );

            sqlError( connection, "SELECT 1/0 FROM prismneonumeric", "22012" );
            sqlError( connection, "SELECT id FROM prismneonumeric WHERE CAST(number_text AS INTEGER) = 9", "22018" );
            sqlError( connection, "SELECT 8/divisor FROM prismneonumeric WHERE id = 2", "22012" );
            Frame divide = connection.execute( namespace, "sql", "SELECT 8/divisor AS quotient FROM prismneonumeric WHERE id = 1", -1 ).getResult().getFrame();
            assertEquals( 4, integerValue( singleRow( relationalFrame( divide, true, List.of( "quotient" ) ) ).getValues( 0 ) ) );
        }
    }


    private static void assertSqlError( PrismConnection connection, String sql, String sqlState, String legacyMessage ) throws IOException {
        assertEquals( legacyMessage, sqlError( connection, sql, sqlState ).getMessage() );
    }


    private static ErrorResponse sqlError( PrismConnection connection, String sql, String sqlState ) throws IOException {
        Response response = connection.send( Request.newBuilder().setExecuteUnparameterizedStatementRequest( ExecuteUnparameterizedStatementRequest.newBuilder()
                .setNamespaceName( Catalog.DEFAULT_NAMESPACE_NAME )
                .setLanguageName( "sql" )
                .setStatement( sql )
                .setFetchSize( -1 ) ) );
        assertTrue( response.getLast() );
        assertEquals( Response.TypeCase.ERROR_RESPONSE, response.getTypeCase() );
        ErrorResponse error = response.getErrorResponse();
        assertTrue( error.hasDetails() );
        assertEquals( sqlState, error.getDetails().getState(), error::getMessage );
        assertEquals( error.getMessage(), error.getDetails().getMessage() );

        Response rollback = connection.send( Request.newBuilder().setRollbackRequest( RollbackRequest.getDefaultInstance() ) );
        assertEquals( Response.TypeCase.ROLLBACK_RESPONSE, rollback.getTypeCase() );
        return error;
    }


    private static void dropFixture( String sql ) throws IOException {
        try ( PrismConnection cleanup = new PrismConnection( true ) ) {
            cleanup.execute( Catalog.DEFAULT_NAMESPACE_NAME, "sql", sql, 1 );
        }
    }


    private static ProtoDocument singleDocument( Frame frame, boolean last ) {
        assertTrue( frame.hasDocumentFrame(), () -> "Expected a document frame but got " + frame.getResultCase() );
        assertEquals( last, frame.getIsLast() );
        assertEquals( 1, frame.getDocumentFrame().getDocumentsCount() );
        return frame.getDocumentFrame().getDocuments( 0 );
    }


    private static void assertScalar( StatementResult result, long expected ) {
        assertFalse( result.hasFrame() );
        assertEquals( expected, result.getScalar() );
    }


    private static long integerValue( ProtoValue value ) {
        return PrismValueDeserializer.deserializeProtoValue( value ).asNumber().bigDecimalValue().longValueExact();
    }

}

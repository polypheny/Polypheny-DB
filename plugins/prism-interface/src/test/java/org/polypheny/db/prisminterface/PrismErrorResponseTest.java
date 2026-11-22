/*
 * Copyright 2019-2024 The Polypheny Project
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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.exceptions.ConstraintViolationException;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.runtime.SqlStateException;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Static;
import org.polypheny.prism.ErrorDetails;
import org.polypheny.prism.ErrorResponse;
import org.polypheny.prism.Response;

class PrismErrorResponseTest {

    @Test
    void explicitPrismStateAndCode() {
        assertError( new PIServiceException( "No open result", "I9002", 9002 ), "I9002", 9002, "No open result" );
        assertError( new PIServiceException( "Unsupported language", "0A000" ), "0A000", null, "Unsupported language" );
        assertError( new PIServiceException( "Explicit zero", "I9001", 0 ), "I9001", 0, "Explicit zero" );
    }


    @Test
    void jdbcStateAndCode() {
        assertError( new SQLException( "Division by zero", "22012", 310 ), "22012", 310, "Division by zero" );
    }


    @Test
    void jdbcNextExceptionPreservesStateAndCode() {
        BatchUpdateException batch = new BatchUpdateException( "Batch failed", null, 0, new int[0] );
        batch.setNextException( new SQLException( "No usable state", "23", 1 ) );
        batch.setNextException( new SQLException( "Duplicate key", "23505", 27 ) );
        assertError( new RuntimeException( "While executing batch", batch ), "23505", 27, "While executing batch" );
    }


    @Test
    void jdbcExplicitAndCauseStatesTakePrecedenceOverNextException() {
        SQLException duplicate = new SQLException( "Duplicate key", "23505", 27 );
        SQLException explicit = new SQLException( "Cannot connect", "08001", 19 );
        explicit.setNextException( duplicate );
        assertError( explicit, "08001", 19, "Cannot connect" );

        SQLException wrapped = new SQLException( "Batch failed", new SQLException( "Division by zero", "22012", 310 ) );
        wrapped.setNextException( duplicate );
        assertError( wrapped, "22012", 310, "Batch failed" );
    }


    @Test
    void jdbcExceptionLinksCanBeCyclic() {
        SQLException first = new SQLException( "Batch failed" );
        SQLException second = new SQLException( "Additional failure", first );
        first.setNextException( second );
        assertTimeoutPreemptively( Duration.ofSeconds( 2 ), () -> assertError( first, "XX000", null, "Batch failed" ) );
    }


    @Test
    void parseException() {
        Throwable syntaxError = new Exception( "Unexpected token" );
        assertError( new NodeParseException( "Unexpected token", ParserPos.ZERO, null, null, syntaxError, true ),
                "42601", null, "Unexpected token" );
    }


    @Test
    void unexpectedParserFailureUsesInternalState() {
        for ( Throwable failure : List.of( new NullPointerException(), new Exception(), new Error() ) ) {
            NodeParseException wrapped = new NodeParseException( "Parser failed", ParserPos.ZERO, null, null, failure );
            NodeParseException nested = new NodeParseException( "Outer parser failed", ParserPos.ZERO, null, null, wrapped );
            assertError( wrapped, "XX000", null, "Parser failed" );
            assertError( nested, "XX000", null, "Outer parser failed" );
        }
    }


    @Test
    void entityNotFound() {
        assertError( new SqlStateException( "Entity not found", "42P01" ), "42P01", null, "Entity not found" );
    }


    @Test
    void notNullViolation() {
        assertError( Static.RESOURCE.columnNotNullable( "name" ).ex(), "23502", null,
                "Column 'name' has no default value and does not allow NULLs" );
    }


    @Test
    void constraintViolation() {
        assertError( new ConstraintViolationException( "Constraint violation" ), "23000", null, "Constraint violation" );
    }


    @Test
    void authenticationFailure() {
        assertError( new AuthenticationException( "Wrong username or password" ), "28000", null, "Wrong username or password" );
    }


    @Test
    void deadlock() {
        assertError( new DeadlockException( "Deadlock detected" ), "40001", null, "Deadlock detected" );
    }


    @Test
    void unsupportedFeature() {
        assertError( new NotImplementedException( "Unsupported value" ), "0A000", null, "Unsupported value" );
    }


    @Test
    void unexpectedFailureUsesInternalState() {
        assertError( new NullPointerException(), "XX000", null, "NullPointerException" );
        assertError( new NullPointerException( "Column must not be null" ), "XX000", null, "Column must not be null" );
    }


    @Test
    void wrappedFailuresPreserveStateAndLegacyMessage() {
        SQLException division = new SQLException( "Division by zero", "22012", 310 );
        Throwable wrapped = new PIServiceException( "While executing SQL", new RuntimeException( "Execution failed", division ) );
        assertError( wrapped, "22012", 310, "While executing SQL" );
    }


    @Test
    void invalidStatesAreIgnored() {
        assertError( new SQLException( "Unexpected failure", "23", 1 ), "XX000", null, "Unexpected failure" );
    }


    private static void assertError( Throwable failure, String sqlState, Integer errorCode, String legacyMessage ) {
        Response response = PIService.createErrorResponse( 12, failure );
        assertEquals( 12, response.getId() );
        assertTrue( response.getLast() );
        assertTrue( response.hasErrorResponse() );
        ErrorResponse error = response.getErrorResponse();
        assertEquals( legacyMessage, error.getMessage() );
        assertTrue( error.hasDetails() );
        ErrorDetails details = error.getDetails();
        assertTrue( details.hasMessage() );
        assertEquals( legacyMessage, details.getMessage() );
        assertTrue( details.hasState() );
        assertEquals( sqlState, details.getState() );
        assertEquals( errorCode, details.hasErrorCode() ? details.getErrorCode() : null );
    }

}

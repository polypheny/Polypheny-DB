/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.restapi;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.restapi.exception.UnauthorizedAccessException;
import org.polypheny.db.util.Pair;


public class RequestParserTest {


    @Test
    public void testBasicAuthorizationDecoding() {
        Pair<String, String> unibasDbis = RequestParser.decodeBasicAuthorization( "Basic dW5pYmFzOmRiaXM=" );
        assertEquals( "unibas", unibasDbis.left, "Username was decoded incorrectly." );
        assertEquals( "dbis", unibasDbis.right, "Password was decoded incorrectly." );
    }


    @Test
    public void testBasicAuthorizationDecodingGarbageHeader() {
        UnauthorizedAccessException thrown = assertThrows( UnauthorizedAccessException.class, () -> {
            Pair<String, String> unibasDbis = RequestParser.decodeBasicAuthorization( "Basic dW5pY!mFzOmRi!" );
            assertEquals( "Username was decoded incorrectly.", "unibas", unibasDbis.left );
            assertEquals( "Password was decoded incorrectly.", "dbis", unibasDbis.right );
        } );
        assertEquals( "Basic Authorization header is not properly encoded.", thrown.getMessage() );

    }


    @Test
    @Disabled // refactor
    public void testParseCatalogTableName() {
        Catalog mockedCatalog = mock( Catalog.class );
        /*when( mockedCatalog.getTable( "schema1", "table1" ) ).thenReturn( null );
        RequestParser requestParser = new RequestParser(
                mockedCatalog,
                null,
                null,
                "username",
                "testdb" );
        LogicalTable table = requestParser.parseCatalogTableName( "schema1.table1." );
        verify( mockedCatalog ).getTable( "schema1", "table1" );*/
    }


    @Test
    public void testParseFilterOperation() {
        Catalog mockedCatalog = mock( Catalog.class );
        RequestParser requestParser = new RequestParser(
                mockedCatalog,
                null,
                null,
                "username",
                "testdb" );
        HashMap<String, Pair<Operator, String>> operationMap = new HashMap<>();
        operationMap.put( ">=10", new Pair<>( OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ), "10" ) );
        operationMap.put( ">10", new Pair<>( OperatorRegistry.get( OperatorName.GREATER_THAN ), "10" ) );
        operationMap.put( "<=10", new Pair<>( OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ), "10" ) );
        operationMap.put( "<10", new Pair<>( OperatorRegistry.get( OperatorName.LESS_THAN ), "10" ) );
        operationMap.put( "=10", new Pair<>( OperatorRegistry.get( OperatorName.EQUALS ), "10" ) );
        operationMap.put( "!=10", new Pair<>( OperatorRegistry.get( OperatorName.NOT_EQUALS ), "10" ) );
        operationMap.put( "%10", new Pair<>( OperatorRegistry.get( OperatorName.LIKE ), "10" ) );
        operationMap.put( "!%10", new Pair<>( OperatorRegistry.get( OperatorName.NOT_LIKE ), "10" ) );

        operationMap.forEach( ( k, v ) -> {
            Pair<Operator, String> operationPair = requestParser.parseFilterOperation( k );
            assertEquals( v, operationPair );
        } );
    }

}

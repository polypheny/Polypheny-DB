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

package org.polypheny.db.restapi;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.restapi.exception.UnauthorizedAccessException;
import org.polypheny.db.sql.SqlLanguageDependent;
import org.polypheny.db.util.Pair;


public class RequestParserTest extends SqlLanguageDependent {


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
            assertEquals( "unibas", unibasDbis.left, "Username was decoded incorrectly." );
            assertEquals( "dbis", unibasDbis.right, "Password was decoded incorrectly." );
        } );
        assertEquals( "Basic Authorization header is not properly encoded.", thrown.getMessage() );

    }


    @Test
    public void testParseCatalogTableName() {
        Catalog catalog = mock( Catalog.class );
        Snapshot mockSnapshot = mock( Snapshot.class );
        LogicalRelSnapshot mockRelSnapshot = mock( LogicalRelSnapshot.class );
        LogicalTable mockTable = mock( LogicalTable.class );

        when( catalog.getSnapshot() ).thenReturn( mockSnapshot );
        when( mockSnapshot.rel() ).thenReturn( mockRelSnapshot );

        when( mockRelSnapshot.getTable( "schema1", "table1" ) ).thenReturn( Optional.of( mockTable ) );
        RequestParser requestParser = new RequestParser(
                catalog,
                null,
                null,
                "username",
                "testdb" );
        LogicalTable table = requestParser.parseCatalogTableName( "schema1.table1." );
        verify( mockSnapshot ).rel(); // check if the snapshot was called
        verify( mockRelSnapshot ).getTable( "schema1", "table1" );
    }


    @Test
    public void testParseFilterOperation() {
        Catalog mockedCatalog = mock( Catalog.class );
        RequestParser requestParser = new RequestParser(
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

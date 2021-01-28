/*
 * Copyright 2019-2020 The Polypheny Project
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


import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.restapi.exception.UnauthorizedAccessException;
import org.polypheny.db.util.Pair;


public class RequestParserTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testBasicAuthorizationDecoding() {
        Pair<String, String> unibasDbis = RequestParser.decodeBasicAuthorization( "Basic dW5pYmFzOmRiaXM=" );
        assertEquals("Username was decoded incorrectly.", "unibas", unibasDbis.left );
        assertEquals("Password was decoded incorrectly.", "dbis", unibasDbis.right );
    }

    @Test
    public void testBasicAuthorizationDecodingGarbageHeader() {
        thrown.expect( UnauthorizedAccessException.class );
        thrown.expectMessage( "Basic Authorization header is not properly encoded." );
        Pair<String, String> unibasDbis = RequestParser.decodeBasicAuthorization( "Basic dW5pY!mFzOmRi!" );
        assertEquals("Username was decoded incorrectly.", "unibas", unibasDbis.left );
        assertEquals("Password was decoded incorrectly.", "dbis", unibasDbis.right );
    }


    @Test
    public void testParseCatalogTableName() throws UnknownTableException, UnknownSchemaException, UnknownDatabaseException {
        Catalog mockedCatalog = mock( Catalog.class );
        when( mockedCatalog.getTable( "testdb", "schema1", "table1" ) ).thenReturn( null );
        RequestParser requestParser = new RequestParser( mockedCatalog, null, null, "testdb", "username" );
        CatalogTable table = requestParser.parseCatalogTableName( "schema1.table1." );
        verify( mockedCatalog ).getTable( "testdb", "schema1", "table1" );
    }

}

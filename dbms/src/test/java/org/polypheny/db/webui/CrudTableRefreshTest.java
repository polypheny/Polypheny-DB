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

package org.polypheny.db.webui;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.javalin.websocket.WsMessageContext;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.webui.models.requests.RequestModel;
import org.polypheny.db.webui.models.requests.UIRequest;
import org.polypheny.db.webui.models.results.RelationalResult;


class CrudTableRefreshTest {

    @Test
    void refreshesSchemaEvenWhenReadingConvertedRowsWouldFail() throws Exception {
        for ( boolean schemaOnly : List.of( true, false ) ) {
            String message = "{\"type\":\"RefreshRequest\",\"entityId\":50,\"schemaOnly\":" + schemaOnly + "}";
            var request = HttpServer.mapper.readValue( message, UIRequest.class );
            var snapshot = mock( Snapshot.class );
            var relational = mock( LogicalRelSnapshot.class );
            when( snapshot.rel() ).thenReturn( relational );
            when( relational.getTable( 50 ) ).thenReturn( Optional.of( new LogicalTable( 50, "customers_mapped", 1, EntityType.SOURCE, null, false ) ) );

            var crud = mock( Crud.class, CALLS_REAL_METHODS );
            doReturn( new Crud.SourceMaterializationRefreshResult( List.of( "Source schema refreshed" ), null, false ) )
                    .when( crud ).refreshSourceSchemaIfNeeded( request );
            String conversionError = "Mapping could not convert column 'name' to INTEGER.";
            var sql = new QueryLanguage( DataModel.RELATIONAL, "sql", List.of(), null, () -> null, null, query -> List.of(), query -> query );
            doReturn( RelationalResult.builder().language( sql ).error( conversionError ).build() ).when( crud ).getTable( request );

            var context = mock( WsMessageContext.class );
            when( context.message() ).thenReturn( message );
            when( context.sessionId() ).thenReturn( "refresh-test" );
            when( context.messageAsClass( RequestModel.class ) ).thenReturn( HttpServer.mapper.readValue( message, RequestModel.class ) );
            when( context.messageAsClass( UIRequest.class ) ).thenReturn( request );
            var catalog = mock( Catalog.class );
            when( catalog.getSnapshot() ).thenReturn( snapshot );
            try ( var catalogs = mockStatic( Catalog.class ); var languages = mockStatic( LanguageManager.class ) ) {
                catalogs.when( Catalog::getInstance ).thenReturn( catalog );
                catalogs.when( Catalog::snapshot ).thenReturn( snapshot );
                languages.when( LanguageManager::getLanguages ).thenReturn( List.of( sql ) );
                new WebSocket( crud ).onMessage( context );
            }

            var result = ArgumentCaptor.forClass( RelationalResult.class );
            verify( context ).send( result.capture() );
            verify( crud ).refreshSourceSchemaIfNeeded( request );
            assertArrayEquals( new String[]{ "Source schema refreshed" }, result.getValue().changeDescriptions );
            if ( schemaOnly ) {
                verify( crud, never() ).getTable( request );
                assertEquals( "customers_mapped", result.getValue().table );
                assertNull( result.getValue().error );
            } else {
                verify( crud ).getTable( request );
                assertEquals( conversionError, result.getValue().error );
            }
        }
    }

}

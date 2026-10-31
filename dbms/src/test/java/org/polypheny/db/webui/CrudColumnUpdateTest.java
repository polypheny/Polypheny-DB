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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.javalin.http.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.ArgumentCaptor;
import org.junit.jupiter.api.Test;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.requests.ColumnRequest;
import org.polypheny.db.webui.models.results.RelationalResult;


class CrudColumnUpdateTest {

    @BeforeAll
    static void configureTestMode() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void renamesModifiableSourceAndTableColumnsWithoutCollectionElementNullability() throws Exception {
        for ( String tableType : List.of( "SOURCE", "ENTITY", "VIEW", "MATERIALIZED" ) ) {
            var request = request( "INTEGER", "INTEGER", null, true, tableType, "" );
            assertNull( request.newColumn.elementsNullable );
            String entityType = tableType.equals( "VIEW" ) ? "VIEW" : tableType.equals( "MATERIALIZED" ) ? "MATERIALIZED VIEW" : "TABLE";
            assertEquals( List.of( "ALTER " + entityType + " \"public\".\"emp\" RENAME COLUMN \"age\" TO \"age1\"" ), execute( request ) );
        }
    }


    @Test
    void renamesCollectionColumnsWithoutChangingTheirType() throws Exception {
        assertEquals( List.of( "ALTER TABLE \"public\".\"emp\" RENAME COLUMN \"age\" TO \"age1\"" ),
                execute( request( "INTEGER", "INTEGER", "ARRAY", true, "SOURCE", "" ) ) );
    }


    @Test
    void editsScalarTypesWithoutCollectionElementNullability() throws Exception {
        var request = request( "INTEGER", "BIGINT", null, false, "ENTITY", "" );
        assertEquals( List.of( "ALTER TABLE \"public\".\"emp\" RENAME COLUMN \"age\" TO \"age1\"",
                "ALTER TABLE \"public\".\"emp\" MODIFY COLUMN \"age1\" SET TYPE BIGINT" ), execute( request ) );
    }


    @Test
    void preservesExplicitCollectionElementNullability() throws Exception {
        for ( boolean nullable : List.of( false, true ) ) {
            var request = request( "INTEGER", "BIGINT", "ARRAY", false, "ENTITY", ",\"elementsNullable\":" + nullable );
            assertEquals( List.of( "ALTER TABLE \"public\".\"emp\" RENAME COLUMN \"age\" TO \"age1\"",
                    "ALTER TABLE \"public\".\"emp\" MODIFY COLUMN \"age1\" SET TYPE BIGINT"
                            + (nullable ? "" : " NOT NULL") + " ARRAY(1,5)" ), execute( request ) );
        }
    }


    private ColumnRequest request( String oldType, String newType, String collectionType, boolean renameOnly, String tableType, String extra ) throws Exception {
        String column = "\"collectionsType\":" + (collectionType == null ? "null" : "\"" + collectionType + "\"")
                + ",\"nullable\":false,\"dimension\":1,\"cardinality\":5";
        return HttpServer.mapper.readValue( "{\"entityId\":50,\"tableType\":\"" + tableType + "\",\"renameOnly\":" + renameOnly
                + ",\"oldColumn\":{\"id\":101,\"name\":\"age\",\"dataType\":\"" + oldType + "\"," + column + "},"
                + "\"newColumn\":{\"id\":101,\"name\":\"age1\",\"dataType\":\"" + newType + "\"," + column + extra + "}}", ColumnRequest.class );
    }


    @Test
    void rejectsReadOnlyTablesBeforeGeneratingColumnChanges() throws Exception {
        assertEquals( List.of(), execute( request( "INTEGER", "INTEGER", null, true, "SOURCE", "" ), false ) );
    }


    private List<String> execute( ColumnRequest request ) {
        return execute( request, true );
    }


    private List<String> execute( ColumnRequest request, boolean modifiable ) {
        var snapshot = mock( Snapshot.class );
        var relational = mock( LogicalRelSnapshot.class );
        when( snapshot.rel() ).thenReturn( relational );
        when( relational.getTable( 50 ) ).thenReturn( Optional.of( new LogicalTable( 50, "emp", 1, EntityType.SOURCE, null, modifiable ) ) );
        when( snapshot.getNamespace( 1 ) ).thenReturn( Optional.of( new LogicalNamespace( 1, "public", DataModel.RELATIONAL, false, false ) ) );
        var context = mock( Context.class );
        when( context.bodyAsClass( ColumnRequest.class ) ).thenReturn( request );
        var language = new QueryLanguage( DataModel.RELATIONAL, "sql", List.of(), null, () -> null, null, query -> List.of(), query -> query );
        var result = RelationalResult.builder().language( language ).build();
        List<String> queries = new ArrayList<>();
        try ( var catalogs = mockStatic( Catalog.class );
                var languages = mockStatic( LanguageManager.class );
                var execution = mockStatic( LanguageCrud.class ) ) {
            catalogs.when( Catalog::snapshot ).thenReturn( snapshot );
            languages.when( LanguageManager::getLanguages ).thenReturn( List.of( language ) );
            execution.when( () -> LanguageCrud.anyQueryResult( any(), any() ) ).thenAnswer( invocation -> {
                queries.add( invocation.<QueryContext>getArgument( 0 ).getQuery() );
                return List.of( result );
            } );
            mock( Crud.class, CALLS_REAL_METHODS ).updateColumn( context );
        }
        if ( modifiable ) {
            verify( context, org.mockito.Mockito.times( queries.size() ) ).json( result );
        } else {
            var response = ArgumentCaptor.forClass( RelationalResult.class );
            verify( context ).json( response.capture() );
            assertEquals( "Unable to modify a table marked as read-only!", response.getValue().error );
        }
        return queries;
    }

}

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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.PolyphenyDb;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.mapping.RelMappingState;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.RelMappingUpdate;
import org.polypheny.db.information.InformationPolyAlg;
import org.polypheny.db.information.InformationPolyAlg.PlanType;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.routing.UiRoutingPageUtil;
import org.polypheny.db.transaction.QueryAnalyzer;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.requests.QueryRequest;
import org.polypheny.db.webui.models.requests.UIRequest;
import org.polypheny.db.webui.models.results.Result;


@Tag("adapter")
class RelMappedTablePreviewIntegrationTest {

    @BeforeAll
    static void startDatabase() {
        boolean previous = PolyphenyDb.noAutoDocker;
        try {
            PolyphenyDb.noAutoDocker = true;
            TestHelper.getInstance();
        } finally {
            PolyphenyDb.noAutoDocker = previous;
        }
    }


    @Test
    void showsFailedCellsOnTheirOwnPageAndKeepsSqlStrict( @TempDir Path directory ) throws Exception {
        String suffix = UUID.randomUUID().toString().substring( 0, 8 );
        String adapter = "previewcsv" + suffix;
        String table = "previewdata" + suffix;
        Files.writeString( directory.resolve( table + ".csv" ), "id:long,amount:string,optional:int\n1,abc,7\n2,42,\n3,2147483648,9\n" );
        String location = directory.toString().replace( "\\", "\\\\" );
        var previousCrud = LanguageCrud.crud;
        var crud = new Crud( TestHelper.getInstance().getTransactionManager() ) {
            @Override
            public int getPageSize() {
                return 2;
            }
        };
        try {
            assertNull( query( crud, "ALTER ADAPTERS ADD \"" + adapter + "\" USING 'CSV' AS 'Source' WITH '"
                    + "{\"method\":\"link\",\"directoryName\":\"" + location + "\",\"directory\":\"" + location + "\",\"maxStringLength\":\"40\"}'" ).error );
            long adapterId = Catalog.snapshot().getAdapter( adapter ).orElseThrow().id;
            var source = Catalog.snapshot().rel().getTable( Catalog.DEFAULT_NAMESPACE_NAME, table ).orElseThrow();
            var draft = RelMappingState.draft( Catalog.getInstance(), adapterId, source.id );
            var columns = draft.columns().stream().map( column -> switch ( column.name ) {
                case "amount" -> new RelMappingUpdate.ColumnName( column.id, column.name, true, PolyType.INTEGER, true );
                case "optional" -> new RelMappingUpdate.ColumnName( column.id, column.name, true, null, true, null, null, DirectColumnMappingRule.DEFINITION.id() );
                default -> new RelMappingUpdate.ColumnName( column.id, column.name, true );
            } ).toList();
            var update = new RelMappingUpdate( adapterId, -1, draft.revision(), draft.mapping().name(), draft.table().name, columns, draft.namespace().id );
            var transaction = TestHelper.getInstance().getTransaction();
            var mapped = DdlManager.getInstance().createRelMapping( source.id, update, transaction.createStatement() );
            var optional = mapped.columns().stream().filter( column -> column.name.equals( "optional" ) ).findFirst().orElseThrow();
            assertTrue( optional.nullable );
            assertTrue( mapped.mapping().rules().contains( new DirectColumnMappingRule( optional.id ) ) );

            var request = UIRequest.builder().entityId( mapped.table().id ).namespace( Catalog.DEFAULT_NAMESPACE_NAME ).currentPage( 1 ).build();
            var first = crud.getTable( request );
            assertNull( first.error );
            assertEquals( 2, first.highestPage );
            assertArrayEquals( new String[]{ "1", null, "7" }, first.data[0] );
            assertArrayEquals( new String[]{ "2", "42", null }, first.data[1] );
            assertEquals( 1, first.cellErrors.size() );
            assertEquals( "Mapping '%s', target 'public.%s', column 'amount': cannot convert value \"abc\" from VARCHAR(40) to INTEGER."
                    .formatted( mapped.mapping().name(), mapped.table().name ), first.cellErrors.get( 0 ).get( 1 ) );
            var response = HttpServer.mapper.readTree( first.toJson() );
            assertEquals( first.cellErrors.get( 0 ).get( 1 ), response.path( "cellErrors" ).path( "0" ).path( "1" ).asText() );
            assertTrue( response.path( "data" ).path( 1 ).path( 2 ).isNull() );

            var second = crud.getTable( request.toBuilder().currentPage( 2 ).build() );
            assertArrayEquals( new String[]{ "3", null, "9" }, second.data[0] );
            assertTrue( second.cellErrors.get( 0 ).get( 1 ).contains( "2147483648" ) );

            var filtered = crud.getTable( request.toBuilder().filter( Map.of( "id", "2" ) ).build() );
            assertEquals( 1, filtered.data.length );
            assertArrayEquals( new String[]{ "2", "42", null }, filtered.data[0] );
            assertTrue( filtered.cellErrors.isEmpty() );
            assertEquals( first.cellErrors.get( 0 ).get( 1 ), query( crud, "SELECT * FROM public." + mapped.table().name ).error );

            verifyAnalyzedMappingPlans( crud, adapterId, source.id, table + "_plan" );
        } finally {
            try {
                if ( Catalog.snapshot().getAdapter( adapter ).isPresent() ) {
                    assertNull( query( crud, "ALTER ADAPTERS DROP \"" + adapter + "\"" ).error );
                }
            } finally {
                LanguageCrud.crud = previousCrud;
            }
        }
    }


    @Test
    void reportsTheSameTemporalMappingFailureInSqlAndShowData( @TempDir Path directory ) throws Exception {
        String suffix = UUID.randomUUID().toString().substring( 0, 8 );
        String adapter = "temporalpreview" + suffix;
        String table = "temporaldata" + suffix;
        Files.writeString( directory.resolve( table + ".csv" ), "id:long,gender:string,name:string\n1,Male,Ada\n2,2026-11-19 12:34:56,Max\n" );
        String location = directory.toString().replace( "\\", "\\\\" );
        var previousCrud = LanguageCrud.crud;
        var crud = new Crud( TestHelper.getInstance().getTransactionManager() );
        try {
            assertNull( query( crud, "ALTER ADAPTERS ADD \"" + adapter + "\" USING 'CSV' AS 'Source' WITH '"
                    + "{\"method\":\"link\",\"directoryName\":\"" + location + "\",\"directory\":\"" + location + "\",\"maxStringLength\":\"40\"}'" ).error );
            long adapterId = Catalog.snapshot().getAdapter( adapter ).orElseThrow().id;
            var source = Catalog.snapshot().rel().getTable( Catalog.DEFAULT_NAMESPACE_NAME, table ).orElseThrow();
            var draft = RelMappingState.draft( Catalog.getInstance(), adapterId, source.id );
            var columns = draft.columns().stream().map( column -> column.name.equals( "gender" )
                    ? new RelMappingUpdate.ColumnName( column.id, "date1", true, PolyType.TIMESTAMP, null )
                    : new RelMappingUpdate.ColumnName( column.id, column.name, true ) ).toList();
            var update = new RelMappingUpdate( adapterId, -1, draft.revision(), draft.mapping().name(), draft.table().name, columns, draft.namespace().id );
            var mapped = DdlManager.getInstance().createRelMapping( source.id, update, TestHelper.getInstance().getTransaction().createStatement() );

            var request = UIRequest.builder().entityId( mapped.table().id ).namespace( Catalog.DEFAULT_NAMESPACE_NAME ).currentPage( 1 ).build();
            var preview = crud.getTable( request );
            assertNull( preview.error );
            String expected = "Mapping '%s', target 'public.%s', column 'date1': cannot convert value \"Male\" from VARCHAR(40) to TIMESTAMP(0)."
                    .formatted( mapped.mapping().name(), mapped.table().name );
            assertEquals( expected, preview.cellErrors.get( 0 ).get( 1 ) );
            assertEquals( "Ada", preview.data[0][2] );
            assertEquals( "Max", preview.data[1][2] );
            assertNotNull( preview.data[1][1] );
            assertEquals( 1, preview.cellErrors.size() );
            assertEquals( expected, query( crud, "SELECT * FROM public." + mapped.table().name ).error );
        } finally {
            try {
                if ( Catalog.snapshot().getAdapter( adapter ).isPresent() ) {
                    assertNull( query( crud, "ALTER ADAPTERS DROP \"" + adapter + "\"" ).error );
                }
            } finally {
                LanguageCrud.crud = previousCrud;
            }
        }
    }


    private static void verifyAnalyzedMappingPlans( Crud crud, long adapterId, long sourceId, String target ) throws Exception {
        var draft = RelMappingState.draft( Catalog.getInstance(), adapterId, sourceId );
        var columns = draft.columns().stream().map( column -> column.name.equals( "id" )
                ? new RelMappingUpdate.ColumnName( column.id, column.name, true, PolyType.DOUBLE, false )
                : new RelMappingUpdate.ColumnName( column.id, column.name, true ) ).toList();
        var update = new RelMappingUpdate( adapterId, -1, draft.revision(), target, target, columns, draft.namespace().id );
        var manager = crud.getTransactionManager();
        DdlManager.getInstance().createRelMapping( sourceId, update, TestHelper.getInstance().getTransaction().createStatement() );

        String sql = "SELECT amount, id FROM " + target;
        String origin = "PolyAlgParsingTest";
        var analyzer = new QueryAnalyzer();
        var transaction = manager.startTransaction( Catalog.defaultUserId, Catalog.defaultNamespaceId, analyzer, origin );
        try {
            var context = QueryContext.builder().query( sql ).language( QueryLanguage.from( "sql" ) ).origin( origin )
                    .isAnalysed( true ).usesCache( false ).transactionManager( manager ).transactions( List.of( transaction ) ).build();
            var result = LanguageManager.getINSTANCE().anyQuery( context ).get( 0 );
            assertTrue( result.getException().isEmpty(), () -> result.getException().orElseThrow().toString() );
            try ( var rows = result.getIterator() ) {
                var data = rows.getNextBatch();
                assertEquals( 3, data.size() );
                assertEquals( "abc", data.get( 0 ).get( 0 ).asString().value );
                assertEquals( 1D, data.get( 0 ).get( 1 ).asDouble().value );
            }

            for ( int attempts = 0; UiRoutingPageUtil.runningTasks() > 0 && attempts < 100; attempts++ ) {
                Thread.sleep( 50 );
            }
            var allocation = findPlan( analyzer, PlanType.ALLOCATION );
            assertTrue( allocation.jsonPolyAlg.contains( "REL_MAPPING" ), () -> allocation.jsonPolyAlg );
            assertTrue( allocation.jsonPolyAlg.contains( target ) );
            assertTrue( allocation.jsonPolyAlg.contains( "id (BIGINT) → id (DOUBLE)" ), () -> allocation.jsonPolyAlg );

            var physical = findPlan( analyzer, PlanType.PHYSICAL );
            assertTrue( physical.jsonPolyAlg.contains( "E_MAPPING_CONVERT" ), () -> physical.jsonPolyAlg );
            var estimatedCpu = HttpServer.mapper.readTree( physical.jsonPolyAlg ).findParents( "displayName" ).stream()
                    .filter( metric -> metric.path( "displayName" ).asText().equals( "Mapping conversion CPU" ) ).findFirst().orElseThrow();
            assertTrue( estimatedCpu.path( "value" ).asDouble() > 0 );
            assertTrue( physical.getTextualPolyAlg().contains( ":DOUBLE NOT NULL" ), physical::getTextualPolyAlg );

            var parseTransaction = TestHelper.getInstance().getTransaction();
            try {
                var allocationRoot = PolyPlanBuilder.buildFromPolyAlg( allocation.getTextualPolyAlg(), PlanType.ALLOCATION, parseTransaction.createStatement() );
                assertEquals( allocation.getTextualPolyAlg(), allocationRoot.alg.buildPolyAlgebra( (String) null ) );
                var physicalRoot = PolyPlanBuilder.buildFromPolyAlg( physical.getTextualPolyAlg(), PlanType.PHYSICAL, parseTransaction.createStatement() );
                assertTrue( physicalRoot.alg.buildPolyAlgebra( (String) null ).contains( ":DOUBLE NOT NULL" ) );
            } finally {
                parseTransaction.rollback( "Finished checking mapping plan parsing" );
            }
        } finally {
            if ( transaction.isActive() ) {
                transaction.rollback( "Finished checking the analyzed mapping plan" );
            }
        }
    }


    private static InformationPolyAlg findPlan( QueryAnalyzer analyzer, PlanType type ) {
        var plan = Arrays.stream( analyzer.getInformationArray() ).filter( InformationPolyAlg.class::isInstance ).map( InformationPolyAlg.class::cast )
                .filter( candidate -> candidate.planType.equals( type.name() ) ).findFirst().orElse( null );
        assertNotNull( plan, () -> "Missing " + type + " plan" );
        return plan;
    }


    private static Result<?, ?> query( Crud crud, String sql ) {
        var request = new QueryRequest( sql, false, true, "sql", Catalog.DEFAULT_NAMESPACE_NAME );
        var context = QueryContext.builder().query( sql ).language( QueryLanguage.from( "sql" ) ).origin( Crud.ORIGIN )
                .transactionManager( crud.getTransactionManager() ).build();
        return LanguageCrud.anyQueryResult( context, request ).get( 0 );
    }

}

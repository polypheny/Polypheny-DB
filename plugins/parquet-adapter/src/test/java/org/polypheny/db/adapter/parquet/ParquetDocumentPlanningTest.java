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

package org.polypheny.db.adapter.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import sun.misc.Unsafe;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.parquet.document.ParquetDocumentSource;
import org.polypheny.db.adapter.parquet.document.planning.ParquetDocFilter;
import org.polypheny.db.adapter.parquet.document.planning.ParquetDocFilterRule;
import org.polypheny.db.adapter.parquet.document.planning.ParquetDocScan;
import org.polypheny.db.adapter.parquet.document.schema.ParquetDocument;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class ParquetDocumentPlanningTest {


    @BeforeAll
    static void initHomeDir() {
        try {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        } catch ( Exception e ) {
            // Already initialized by another test.
        }
    }


    @Test
    void documentBuildsTupleTypeFromExportedColumnsAndFallsBackToDocumentId() throws Exception {
        TestParquetSource.exportedColumns = Map.of(
                "orders", List.of(
                        exportedColumn( "name", PolyType.VARCHAR, 0, true ),
                        exportedColumn( "score", PolyType.INTEGER, 1, false ) ) );
        ParquetDocument document = new ParquetDocument( collection( "orders" ), List.of( sourceFile() ), testSource() );
        AlgDataType tupleType = document.getTupleType( AlgDataTypeFactory.DEFAULT );

        assertEquals( List.of( "name", "score" ), tupleType.getFieldNames() );
        assertTrue( tupleType.getFields().get( 0 ).getType().isNullable() );
        assertFalse( tupleType.getFields().get( 1 ).getType().isNullable() );
        assertEquals( List.of( sourceFile() ), document.getSourceFiles() );

        TestParquetSource.exportedColumns = Map.of();
        ParquetDocument fallback = new ParquetDocument( collection( "missing" ), List.of(), testSource() );

        assertEquals( DocumentType.ofId(), fallback.getTupleType( AlgDataTypeFactory.DEFAULT ) );
    }


    @Test
    void docScanCopiesFiltersDerivesDocumentRowAndRegistersFilterRule() {
        ParquetDocument document = new ParquetDocument( collection( "orders" ), List.of( sourceFile() ), null );
        AlgCluster cluster = cluster();
        ParquetAdapterFilter<PolyValue> filter = new ParquetAdapterFilter<>( 0, Kind.EQUALS, PolyString.of( "Alice" ) );
        ParquetDocScan scan = new ParquetDocScan( cluster, document, List.of( filter ) );

        assertEquals( List.of( filter ), scan.getFilters() );
        assertEquals( List.of( DocumentType.DOCUMENT_ID ), scan.deriveRowType().getFieldNames() );
        assertTrue( scan.algCompareString().contains( "$filters=" ) );

        AlgNode copied = scan.copy( cluster.traitSetOf( EnumerableConvention.INSTANCE ), List.of() );
        assertNotSame( scan, copied );
        assertEquals( scan.getFilters(), ((ParquetDocScan) copied).getFilters() );

        VolcanoPlanner planner = new VolcanoPlanner();
        scan.register( planner );
    }


    @Test
    void docFilterCopyPreservesEntityAndDelegatesToScanInput() {
        ParquetDocument document = new ParquetDocument( collection( "orders" ), List.of( sourceFile() ), null );
        AlgCluster cluster = cluster();
        ParquetDocScan scan = new ParquetDocScan( cluster, document, List.of() );
        RexNode condition = ref( cluster, 0 );
        ParquetDocFilter filter = new ParquetDocFilter( cluster, cluster.traitSetOf( EnumerableConvention.INSTANCE ), scan, condition, document );

        ParquetDocFilter copied = filter.copy( cluster.traitSetOf( EnumerableConvention.INSTANCE ), scan, condition );

        assertNotSame( filter, copied );
        assertSame( document, copied.getEntity() );
        assertSame( scan, copied.getInput() );
    }


    @Test
    @SuppressWarnings("unchecked")
    void docFilterRuleSplitsNestedAndConjunctions() throws Exception {
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        AlgDataType boolType = typeFactory.createPolyType( PolyType.BOOLEAN );
        RexNode left = new RexIndexRef( 0, boolType );
        RexNode middle = new RexIndexRef( 1, boolType );
        RexNode right = new RexIndexRef( 2, boolType );
        RexNode condition = new RexCall(
                boolType,
                new org.polypheny.db.nodes.SpecialOperator( Kind.AND.name(), Kind.AND ),
                List.of( left, new RexCall( boolType, new org.polypheny.db.nodes.SpecialOperator( Kind.AND.name(), Kind.AND ), List.of( middle, right ) ) ) );
        ParquetDocFilterRule rule = new ParquetDocFilterRule( AlgFactories.LOGICAL_BUILDER, null );
        Method splitConjunctions = ParquetDocFilterRule.class.getDeclaredMethod( "splitConjunctions", RexNode.class );
        splitConjunctions.setAccessible( true );

        List<RexNode> split = (List<RexNode>) splitConjunctions.invoke( rule, condition );
        List<RexNode> single = (List<RexNode>) splitConjunctions.invoke( rule, left );

        assertEquals( List.of( left, middle, right ), split );
        assertEquals( List.of( left ), single );
    }


    private static AlgCluster cluster() {
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        VolcanoPlanner planner = new VolcanoPlanner();
        return AlgCluster.createDocument( planner, new RexBuilder( typeFactory ), null );
    }


    @SuppressWarnings("SameParameterValue")
    private static RexNode ref( AlgCluster cluster, int index ) {
        return new RexIndexRef( index, cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN ) );
    }


    private static PhysicalCollection collection( String name ) {
        return new PhysicalCollection( 10L, 20L, 30L, 40L, name, "public", 50L );
    }


    private static ParquetSourceFile sourceFile() {
        return new ParquetSourceFile( "file:/tmp/orders.parquet", Map.of(), Map.of() );
    }


    private static ExportedColumn exportedColumn( String name, PolyType type, int position, boolean nullable ) {
        return new ExportedColumn( name, type, null, null, null, null, null, nullable, "public", "orders", name, position, false );
    }


    private static TestParquetSource testSource() throws Exception {
        java.lang.reflect.Field field = Unsafe.class.getDeclaredField( "theUnsafe" );
        field.setAccessible( true );
        return (TestParquetSource) ((Unsafe) field.get( null )).allocateInstance( TestParquetSource.class );
    }


    @AdapterProperties(
            name = "Test Parquet Source",
            description = "Test-only source for document planning unit tests.",
            usedModes = DeployMode.EMBEDDED,
            defaultMode = DeployMode.EMBEDDED)
    private static class TestParquetSource extends ParquetDocumentSource {

        private static Map<String, List<ExportedColumn>> exportedColumns = Map.of();


        private TestParquetSource( Path directory ) {
            super( 900L, "test_parquet_doc_source", Map.of( "method", "link", "directory", directory.toString(), "directoryName", directory.toString() ), DeployMode.EMBEDDED );
        }


        @Override
        public Map<String, List<ExportedColumn>> getExportedColumns() {
            return exportedColumns;
        }

    }

}

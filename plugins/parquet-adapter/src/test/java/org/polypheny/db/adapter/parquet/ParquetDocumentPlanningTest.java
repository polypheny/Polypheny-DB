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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.parquet.document.ParquetDocumentSource;
import org.polypheny.db.adapter.parquet.document.execution.ParquetDocAggregateExecutor;
import org.polypheny.db.adapter.parquet.document.planning.ParquetDocMetadataScan;
import org.polypheny.db.adapter.parquet.document.planning.ParquetDocRules;
import org.polypheny.db.adapter.parquet.document.planning.ParquetDocScan;
import org.polypheny.db.adapter.parquet.document.planning.ParquetDocConvention;
import org.polypheny.db.adapter.parquet.document.schema.ParquetDocument;
import org.polypheny.db.adapter.parquet.relational.schema.DiscoveredTableBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import sun.misc.Unsafe;


class ParquetDocumentPlanningTest {

    @TempDir
    Path tempDir;


    @BeforeAll
    static void initHomeDir() {
        try {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        } catch ( Exception e ) {
            // Already initialized by another test.
        }
    }


    private static AlgCluster cluster() {
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        VolcanoPlanner planner = new VolcanoPlanner();
        return AlgCluster.createDocument( planner, new RexBuilder( typeFactory ), null );
    }


    private static PhysicalCollection collection( String name ) {
        return new PhysicalCollection( 10L, 20L, 30L, 40L, name, "public", 50L );
    }


    private static ParquetSourceFile sourceFile( Path file ) throws Exception {
        return ParquetSourceFile.of( file.toUri().toURL().toString(), Map.of() );
    }


    private static DiscoveredTableBinding binding( List<ParquetSourceFile> sourceFiles, Map<String, List<String>> columnPaths ) {
        return new DiscoveredTableBinding( sourceFiles, null, List.of(), columnPaths );
    }


    private static ExportedColumn exportedColumn( String name, PolyType type, int position, boolean nullable ) {
        return new ExportedColumn( name, type, null, null, null, null, null, nullable, "public", "orders", name, position, false );
    }


    private static TestParquetSource testSource() throws Exception {
        java.lang.reflect.Field field = Unsafe.class.getDeclaredField( "theUnsafe" );
        field.setAccessible( true );
        return (TestParquetSource) ((Unsafe) field.get( null )).allocateInstance( TestParquetSource.class );
    }


    @Test
    void documentBuildsTupleTypeFromExportedColumnsAndFallsBackToDocumentId() throws Exception {
        Path file = tempDir.resolve( "tuple-type.parquet" );
        writeOrdersParquet( file );
        ParquetSourceFile sourceFile = sourceFile( file );
        TestParquetSource.exportedColumns = Map.of(
                "orders", List.of(
                        exportedColumn( "name", PolyType.VARCHAR, 0, true ),
                        exportedColumn( "score", PolyType.INTEGER, 1, false ) ) );
        ParquetDocument document = new ParquetDocument( collection( "orders" ), binding( List.of( sourceFile ), Map.of( "name", List.of( "name" ), "score", List.of( "score" ) ) ), testSource() );
        AlgDataType tupleType = document.getTupleType( AlgDataTypeFactory.DEFAULT );

        assertEquals( List.of( "name", "score" ), tupleType.getFieldNames() );
        assertTrue( tupleType.getFields().get( 0 ).getType().isNullable() );
        assertFalse( tupleType.getFields().get( 1 ).getType().isNullable() );
        assertEquals( List.of( sourceFile ), document.getSourceFiles() );

        TestParquetSource.exportedColumns = Map.of();
        ParquetDocument fallback = new ParquetDocument( collection( "missing" ), binding( List.of(), Map.of() ), testSource() );

        assertEquals( DocumentType.ofId(), fallback.getTupleType( AlgDataTypeFactory.DEFAULT ) );
    }


    @Test
    void docScanCopiesFiltersDerivesDocumentRowAndRegistersFilterRule() throws Exception {
        TestParquetSource.exportedColumns = Map.of();
        ParquetDocument document = new ParquetDocument( collection( "orders" ), binding( List.of(), Map.of() ), testSource() );
        AlgCluster cluster = cluster();
        ParquetAdapterFilter<PolyValue> filter = new ParquetAdapterFilter<>( 0, Kind.EQUALS, PolyString.of( "Alice" ) );
        ParquetDocScan scan = new ParquetDocScan( cluster, document, List.of( filter ) );

        assertEquals( List.of( filter ), scan.getFilters() );
        assertEquals( List.of( DocumentType.DOCUMENT_ID ), scan.deriveRowType().getFieldNames() );
        assertTrue( scan.algCompareString().contains( "$filters=" ) );
        assertTrue( scan.getTraitSet().containsIfApplicable( ParquetDocConvention.INSTANCE ) );

        AlgNode copied = scan.copy( cluster.traitSetOf( ParquetDocConvention.INSTANCE ), List.of() );
        assertNotSame( scan, copied );
        assertEquals( scan.getFilters(), ((ParquetDocScan) copied).getFilters() );

        VolcanoPlanner planner = new VolcanoPlanner();
        scan.register( planner );
    }


    @Test
    void documentRulesIncludePartialAggregateRewrites() {
        List<String> ruleNames = ParquetDocRules.rules( ParquetDocConvention.INSTANCE ).stream()
                .map( AlgOptRule::toString )
                .toList();

        assertTrue( ruleNames.stream().anyMatch( name -> name.contains( "documentAggregateOnScan" ) ) );
        assertTrue( ruleNames.stream().anyMatch( name -> name.contains( "partialAggregateOnUnion" ) ) );
        assertTrue( ruleNames.stream().anyMatch( name -> name.contains( "partialAggregateOnCalcUnion" ) ) );
    }


    @Test
    void documentAggregateExecutorGroupsPrimitiveColumns() throws Exception {
        Path file = tempDir.resolve( "orders.parquet" );
        writeOrdersParquet( file );
        List<ExportedColumn> columns = List.of(
                exportedColumn( "customer", PolyType.VARCHAR, 0, false ),
                exportedColumn( "amount", PolyType.DOUBLE, 1, false ) );
        ParquetDocAggregateExecutor executor = new ParquetDocAggregateExecutor(
                List.of( ParquetSourceFile.of( file.toUri().toURL().toString(), Map.of() ) ),
                columns,
                Map.of( "customer", List.of( "customer" ), "amount", List.of( "amount" ) ) );

        Map<String, List<Number>> rows = new LinkedHashMap<>();
        try ( Enumerator<PolyValue[]> enumerator = executor.createDataEnumerator(
                new DataContext.SlimDataContext(),
                new int[]{ 0, 1 },
                List.of(),
                new int[]{ 0 },
                new String[]{ Kind.COUNT.name(), Kind.SUM.name() },
                new int[]{ -1, 1 } ).enumerator() ) {
            while ( enumerator.moveNext() ) {
                PolyValue[] row = enumerator.current();
                rows.put( row[0].asString().value, List.of( row[1].asNumber().longValue(), row[2].asNumber().doubleValue() ) );
            }
        }

        assertEquals( Map.of(
                "Alice", List.of( 2L, 17.5D ),
                "Bob", List.of( 1L, 3.0D ) ), rows );
        assertTrue( executor.supportsAggregate(
                new int[]{ 0, 1 },
                List.of(),
                new int[]{ 0 },
                new String[]{ Kind.COUNT.name(), Kind.SUM.name() },
                new int[]{ -1, 1 } ) );
    }


    @Test
    void documentAggregateExecutorUsesSharedMetadataPath() throws Exception {
        Path file = tempDir.resolve( "metadata-orders.parquet" );
        writeOrdersParquet( file );
        List<ExportedColumn> columns = List.of(
                exportedColumn( "customer", PolyType.VARCHAR, 0, false ),
                exportedColumn( "amount", PolyType.DOUBLE, 1, false ) );
        ParquetDocAggregateExecutor executor = new ParquetDocAggregateExecutor(
                List.of( ParquetSourceFile.of( file.toUri().toURL().toString(), Map.of() ) ),
                columns,
                Map.of( "customer", List.of( "customer" ), "amount", List.of( "amount" ) ) );

        AggregateCall count = AggregateCall.create(
                aggregateFunction( Kind.COUNT, "COUNT" ),
                false,
                false,
                List.of(),
                -1,
                AlgCollations.EMPTY,
                AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.BIGINT ),
                "count" );

        assertTrue( executor.supportsMetadataAggregate( new int[]{ 0, 1 }, List.of(), ImmutableBitSet.of(), List.of( count ) ) );
        try ( Enumerator<PolyValue[]> enumerator = executor.createMetadataEnumerator(
                new DataContext.SlimDataContext(),
                new int[]{ 0, 1 },
                List.of(),
                new int[0],
                new String[]{ Kind.COUNT.name() },
                new int[]{ -1 } ).enumerator() ) {
            assertTrue( enumerator.moveNext() );
            assertEquals( 3L, enumerator.current()[0].asNumber().longValue() );
            assertFalse( enumerator.moveNext() );
        }
    }


    @Test
    void documentMetadataScanCopiesAndHasZeroCost() throws Exception {
        Path file = tempDir.resolve( "metadata-scan.parquet" );
        writeOrdersParquet( file );
        TestParquetSource.exportedColumns = Map.of(
                "orders", List.of(
                        exportedColumn( "customer", PolyType.VARCHAR, 0, false ),
                        exportedColumn( "amount", PolyType.DOUBLE, 1, false ) ) );
        ParquetDocument document = new ParquetDocument(
                collection( "orders" ),
                binding( List.of( sourceFile( file ) ), Map.of( "customer", List.of( "customer" ), "amount", List.of( "amount" ) ) ),
                testSource() );
        AlgCluster cluster = cluster();
        ParquetDocScan scan = new ParquetDocScan( cluster, document, List.of() );
        ParquetDocMetadataScan metadataScan = new ParquetDocMetadataScan( scan );

        assertInstanceOf( ParquetDocMetadataScan.class, metadataScan.copy( metadataScan.getTraitSet(), List.of() ) );
        assertEquals( 0D, metadataScan.computeSelfCost( new VolcanoPlanner(), null ).getRows() );
    }


    @Test
    void documentReusesAggregateSchemaMetadata() throws Exception {
        Path file = tempDir.resolve( "cached-metadata.parquet" );
        writeOrdersParquet( file );
        TestParquetSource.exportedColumns = Map.of(
                "orders", List.of(
                        exportedColumn( "customer", PolyType.VARCHAR, 0, false ),
                        exportedColumn( "amount", PolyType.DOUBLE, 1, false ) ) );
        ParquetDocument document = new ParquetDocument(
                collection( "orders" ),
                binding( List.of( sourceFile( file ) ), Map.of( "customer", List.of( "customer" ), "amount", List.of( "amount" ) ) ),
                testSource() );
        Files.delete( file );

        AggregateCall count = AggregateCall.create(
                aggregateFunction( Kind.COUNT, "COUNT" ),
                false,
                false,
                List.of(),
                -1,
                AlgCollations.EMPTY,
                AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.BIGINT ),
                "count" );

        assertTrue( document.supportsMetadataAggregate( new int[]{ 0, 1 }, List.of(), ImmutableBitSet.of(), List.of( count ) ) );
        assertTrue( document.supportsMetadataAggregate( new int[]{ 0, 1 }, List.of(), ImmutableBitSet.of(), List.of( count ) ) );
    }


    private static void writeOrdersParquet( Path file ) throws Exception {
        org.apache.parquet.schema.MessageType schema = Types.buildMessage()
                .required( PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( "customer" )
                .required( PrimitiveTypeName.DOUBLE ).named( "amount" )
                .named( "orders" );
        SimpleGroupFactory groupFactory = new SimpleGroupFactory( schema );
        try ( ParquetWriter<Group> writer = ExampleParquetWriter.builder( new LocalOutputFile( file ) )
                .withType( schema )
                .build() ) {
            writer.write( groupFactory.newGroup().append( "customer", "Alice" ).append( "amount", 10.0D ) );
            writer.write( groupFactory.newGroup().append( "customer", "Bob" ).append( "amount", 3.0D ) );
            writer.write( groupFactory.newGroup().append( "customer", "Alice" ).append( "amount", 7.5D ) );
        }
    }


    private static AggFunction aggregateFunction( Kind kind, String name ) {
        return (AggFunction) Proxy.newProxyInstance(
                ParquetDocumentPlanningTest.class.getClassLoader(),
                new Class<?>[]{ AggFunction.class },
                ( proxy, method, args ) -> switch ( method.getName() ) {
                    case "getKind" -> kind;
                    case "getName", "toString", "getAllowedSignatures" -> name;
                    case "getOperatorName" -> OperatorName.COUNT;
                    case "isAggregator", "isQuantifierAllowed", "allowsFilter" -> true;
                    case "hashCode" -> System.identityHashCode( proxy );
                    case "equals" -> proxy == args[0];
                    default -> null;
                } );
    }


    private record LocalOutputFile( Path path ) implements OutputFile {

        @Override
        public PositionOutputStream create( long blockSizeHint ) throws IOException {
            return createOrOverwrite( blockSizeHint );
        }


        @Override
        public PositionOutputStream createOrOverwrite( long blockSizeHint ) throws IOException {
            return new LocalPositionOutputStream( Files.newOutputStream( path ) );
        }


        @Override
        public boolean supportsBlockSize() {
            return false;
        }


        @Override
        public long defaultBlockSize() {
            return 0;
        }


        @Override
        public String getPath() {
            return path.toString();
        }

    }


    private static class LocalPositionOutputStream extends PositionOutputStream {

        private final OutputStream out;
        private long position;


        private LocalPositionOutputStream( OutputStream out ) {
            this.out = out;
        }


        @Override
        public long getPos() {
            return position;
        }


        @Override
        public void write( int b ) throws IOException {
            out.write( b );
            position++;
        }


        @Override
        public void write( byte @NotNull [] b, int off, int len ) throws IOException {
            out.write( b, off, len );
            position += len;
        }


        @Override
        public void flush() throws IOException {
            out.flush();
        }


        @Override
        public void close() throws IOException {
            out.close();
        }

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

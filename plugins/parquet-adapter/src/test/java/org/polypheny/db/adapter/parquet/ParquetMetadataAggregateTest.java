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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.execution.aggregate.ParquetFileGroupedAggregateRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelDataAggregateExecutor;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelMetadataAggregateExecutor;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelProjectExecutor;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRowRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnStatistics;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetConstantColumnResolver;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.aggregate.ColumnAggregateProjection;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class ParquetMetadataAggregateTest {

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


    @Test
    void metadataAggregateAllowsPartitionFiltersAndGroups() throws Exception {
        ParquetRelTable table = partitionedTable();
        int[] fields = new int[]{ 0, 1, 2 };

        assertTrue( table.supportsMetadataAggregate(
                fields,
                List.of( new ParquetAdapterFilter<>( 1, Kind.EQUALS, null ) ),
                ImmutableBitSet.of( 1, 2 ),
                List.of() ) );

        assertFalse( table.supportsMetadataAggregate(
                fields,
                List.of( new ParquetAdapterFilter<>( 0, Kind.GREATER_THAN, null ) ),
                ImmutableBitSet.of(),
                List.of() ) );

        assertFalse( table.supportsMetadataAggregate(
                fields,
                List.of(),
                ImmutableBitSet.of( 0 ),
                List.of() ) );
    }


    @Test
    void projectedScanUsesFolderPartitionValuesWhenPhysicalColumnsCollide() throws Exception {
        ParquetRelTable table = partitionedTableWithPhysicalPartitionColumns();
        List<ParquetAdapterFilter<PolyValue>> filters = List.of( ParquetAdapterFilter.logical(
                Kind.AND,
                List.of(
                        new ParquetAdapterFilter<>( 1, Kind.EQUALS, PolyString.of( "2022" ) ),
                        new ParquetAdapterFilter<>( 2, Kind.EQUALS, PolyString.of( "10" ) ) ) ) );

        List<List<Object>> rows = new java.util.ArrayList<>();
        try ( Enumerator<PolyValue[]> enumerator = projectExecutor( table ).enumeratorForFirstFile( new int[]{ 0, 1, 2 } ) ) {
            assertInstanceOf( ParquetRowRelEnumerator.class, enumerator );
        }
        try ( Enumerator<PolyValue[]> enumerator = projectExecutor( table ).createEnumerator(
                parameterContext( Map.of() ),
                new int[]{ 0, 1, 2 },
                filters ).enumerator() ) {
            while ( enumerator.moveNext() ) {
                PolyValue[] row = enumerator.current();
                rows.add( List.of( row[0].asNumber().longValue(), row[1].asString().value, row[2].asString().value ) );
            }
        }

        assertEquals( List.of( List.of( 1L, "2022", "10" ), List.of( 2L, "2022", "10" ) ), rows );
    }


    @Test
    void projectedScanReadsMinimalDataColumnWhenOnlyPartitionColumnsAreProjected() throws Exception {
        ParquetRelTable table = partitionedTable();

        try ( Enumerator<PolyValue[]> enumerator = projectExecutor( table ).enumeratorForFirstFile( new int[]{ 2 } ) ) {
            assertInstanceOf( ParquetRowRelEnumerator.class, enumerator );
            assertTrue( enumerator.moveNext() );
            assertEquals( "01", enumerator.current()[0].asString().value );
        }
    }


    @Test
    void metadataAggregateUsesFileConstantPhysicalFilters() throws Exception {
        ParquetRelTable table = physicalPartitionColumnTable(
                List.of(
                        List.of( row( 1L, "2022", "10" ), row( 2L, "2022", "10" ) ),
                        List.<Object[]>of( row( 3L, "2022", "11" ) ) ) );
        int[] fields = new int[]{ 0, 1, 2 };
        List<ParquetAdapterFilter<PolyValue>> filters = List.of( ParquetAdapterFilter.logical(
                Kind.AND,
                List.of(
                        dynamicFilter( 1, 0L ),
                        dynamicFilter( 2, 1L ) ) ) );

        assertTrue( table.supportsMetadataAggregate( fields, filters, ImmutableBitSet.of(), List.of() ) );

        ParquetRelMetadataAggregateExecutor executor = metadataExecutor( table );
        DataContext dataContext = parameterContext( Map.of( 0L, PolyString.of( "2022" ), 1L, PolyString.of( "10" ) ) );
        try ( Enumerator<PolyValue[]> enumerator = executor.createEnumerator(
                dataContext,
                fields,
                filters,
                new int[0],
                new String[]{ Kind.COUNT.name() },
                new int[]{ -1 } ).enumerator() ) {
            assertTrue( enumerator.moveNext() );
            assertEquals( 2L, enumerator.current()[0].asNumber().longValue() );
            assertFalse( enumerator.moveNext() );
        }
    }


    @Test
    void metadataAggregateRejectsMixedPhysicalFilterColumns() throws Exception {
        ParquetRelTable table = physicalPartitionColumnTable(
                List.of( List.of(
                        row( 1L, "2022", "10" ),
                        row( 2L, "2022", "11" ) ) ) );
        int[] fields = new int[]{ 0, 1, 2 };

        assertFalse( table.supportsMetadataAggregate(
                fields,
                List.of( new ParquetAdapterFilter<>( 2, Kind.EQUALS, PolyString.of( "10" ) ) ),
                ImmutableBitSet.of(),
                List.of() ) );
    }


    @Test
    void metadataAggregateGroupsByFileConstantPhysicalColumns() throws Exception {
        ParquetRelTable table = physicalPartitionColumnTable(
                List.of(
                        List.of( row( 1L, "2022", "10" ), row( 2L, "2022", "10" ) ),
                        List.<Object[]>of( row( 3L, "2022", "11" ) ) ) );
        int[] fields = new int[]{ 0, 1, 2 };

        assertTrue( table.supportsMetadataAggregate( fields, List.of(), ImmutableBitSet.of( 1, 2 ), List.of() ) );

        Map<List<String>, Long> counts = new LinkedHashMap<>();
        try ( Enumerator<PolyValue[]> enumerator = metadataExecutor( table ).createEnumerator(
                parameterContext( Map.of() ),
                fields,
                List.of(),
                new int[]{ 1, 2 },
                new String[]{ Kind.COUNT.name() },
                new int[]{ -1 } ).enumerator() ) {
            while ( enumerator.moveNext() ) {
                PolyValue[] row = enumerator.current();
                counts.put( List.of( row[0].asString().value, row[1].asString().value ), row[2].asNumber().longValue() );
            }
        }

        assertEquals( Map.of( List.of( "2022", "10" ), 2L, List.of( "2022", "11" ), 1L ), counts );
    }


    @Test
    void fileGroupedAggregateGroupsPhysicalConstantColumns() throws Exception {
        ParquetRelTable table = physicalPartitionColumnTable(
                List.of(
                        List.of( row( 1L, "2022", "10" ), row( 2L, "2022", "10" ) ),
                        List.<Object[]>of( row( 3L, "2022", "11" ) ) ) );

        Map<List<String>, Double> sums = new LinkedHashMap<>();
        try ( ParquetFileGroupedAggregateRelEnumerator enumerator = new ParquetFileGroupedAggregateRelEnumerator(
                table,
                new int[]{ 0, 1, 2 },
                new int[]{ 1, 2 },
                new String[]{ Kind.SUM.name() },
                new int[]{ 0 },
                new ColumnAggregateProjection( new int[]{ 0 }, Map.of( 0, 0 ) ),
                List.of(),
                new AtomicBoolean( false ) ) ) {
            while ( enumerator.moveNext() ) {
                PolyValue[] row = enumerator.current();
                sums.put( List.of( row[0].asString().value, row[1].asString().value ), row[2].asNumber().doubleValue() );
            }
        }

        assertEquals( Map.of( List.of( "2022", "10" ), 3D, List.of( "2022", "11" ), 3D ), sums );
    }


    @Test
    void streamingGroupedAggregateSkipsFileConstantPhysicalFilter() throws Exception {
        ParquetRelTable table = physicalPartitionColumnTable(
                List.of(
                        List.of( row( 1L, "2022", "10" ), row( 1L, "2022", "10" ), row( 2L, "2022", "10" ) ),
                        List.<Object[]>of( row( 3L, "2021", "10" ) ) ) );

        assertEquals(
                Map.of( 1L, 2L, 2L, 1L ),
                streamingGroupedCounts( table, List.of( new ParquetAdapterFilter<>( 1, Kind.EQUALS, PolyString.of( "2022" ) ) ) ) );
    }


    @Test
    void streamingGroupedAggregateRetainsResidualAndFilter() throws Exception {
        ParquetRelTable table = physicalPartitionColumnTable(
                List.of(
                        List.of( row( 1L, "2022", "10" ), row( 1L, "2022", "10" ), row( 2L, "2022", "10" ) ),
                        List.<Object[]>of( row( 3L, "2021", "10" ) ) ) );
        ParquetAdapterFilter<PolyValue> filter = ParquetAdapterFilter.logical(
                Kind.AND,
                List.of(
                        new ParquetAdapterFilter<>( 1, Kind.EQUALS, PolyString.of( "2022" ) ),
                        new ParquetAdapterFilter<>( 0, Kind.GREATER_THAN, PolyLong.of( 1L ) ) ) );

        assertEquals( Map.of( 2L, 1L ), streamingGroupedCounts( table, List.of( filter ) ) );
    }


    @Test
    void streamingGroupedAggregateRetainsResidualOrFilter() throws Exception {
        ParquetRelTable table = physicalPartitionColumnTable(
                List.of(
                        List.of( row( 1L, "2022", "10" ), row( 2L, "2022", "10" ) ),
                        List.of( row( 3L, "2021", "10" ), row( 4L, "2021", "10" ) ) ) );
        ParquetAdapterFilter<PolyValue> filter = ParquetAdapterFilter.logical(
                Kind.OR,
                List.of(
                        new ParquetAdapterFilter<>( 1, Kind.EQUALS, PolyString.of( "2022" ) ),
                        new ParquetAdapterFilter<>( 0, Kind.EQUALS, PolyLong.of( 3L ) ) ) );

        assertEquals( Map.of( 1L, 1L, 2L, 1L, 3L, 1L ), streamingGroupedCounts( table, List.of( filter ) ) );
    }


    @Test
    void fileGroupedScalarCountReturnsZeroWhenFiltersRejectEveryFile() throws Exception {
        ParquetRelTable table = physicalPartitionColumnTable(
                List.of( List.of( row( 1L, "2022", "10" ), row( 2L, "2022", "10" ) ) ) );

        try ( ParquetFileGroupedAggregateRelEnumerator enumerator = new ParquetFileGroupedAggregateRelEnumerator(
                table,
                new int[]{ 0, 1, 2 },
                new int[0],
                new String[]{ Kind.COUNT.name() },
                new int[]{ -1 },
                new ColumnAggregateProjection( new int[0], Map.of() ),
                List.of( new ParquetAdapterFilter<>( 1, Kind.EQUALS, PolyString.of( "2099" ) ) ),
                new AtomicBoolean( false ) ) ) {
            assertTrue( enumerator.moveNext() );
            assertEquals( 0L, enumerator.current()[0].asNumber().longValue() );
            assertFalse( enumerator.moveNext() );
        }
    }


    @Test
    void constantColumnResolverRejectsMixedNullAndNonNullValues() {
        ParquetColumnBinding binding = new ParquetColumnBinding( 10, "month", ParquetColumnRole.DATA, List.of( "month" ) );
        ParquetSourceFile sourceFile = new ParquetSourceFile(
                "file:/unused.parquet",
                Map.of(),
                Map.of( List.of( "month" ), new ParquetColumnStatistics( PolyType.TEXT, 2, 2, 1L, "10", "10", true ) ) );

        assertTrue( new ParquetConstantColumnResolver().resolve( sourceFile, binding ).isEmpty() );
    }


    private ParquetRelTable partitionedTable() throws Exception {
        Path file = tempDir.resolve( "part-000.parquet" );
        writeIdParquet( file, List.of( 1L, 2L ) );

        PhysicalColumn id = column( 10, "id", 0, PolyType.BIGINT );
        PhysicalColumn year = column( 11, "year", 1, PolyType.VARCHAR );
        PhysicalColumn month = column( 12, "month", 2, PolyType.VARCHAR );
        PhysicalTable physicalTable = new PhysicalTable( 1, 1, 1, "orders", List.of( id, year, month ), 1, "public", List.of(), 1 );

        ParquetSourceFile sourceFile = ParquetSourceFile.of( file.toUri().toURL().toString(), Map.of( "year", "2025", "month", "01" ) );
        ParquetTableBinding binding = new ParquetTableBinding(
                List.of( sourceFile ),
                null,
                List.of(),
                Map.of(
                        id.id, new ParquetColumnBinding( id.id, "id", ParquetColumnRole.DATA, List.of( "id" ) ),
                        year.id, new ParquetColumnBinding( year.id, "year", ParquetColumnRole.PARTITION, List.of() ),
                        month.id, new ParquetColumnBinding( month.id, "month", ParquetColumnRole.PARTITION, List.of() ) ) );
        return new ParquetRelTable( 1, physicalTable, binding, null );
    }


    private ParquetRelTable physicalPartitionColumnTable( List<List<Object[]>> fileRows ) throws Exception {
        PhysicalColumn id = column( 10, "id", 0, PolyType.BIGINT );
        PhysicalColumn year = column( 11, "year", 1, PolyType.VARCHAR );
        PhysicalColumn month = column( 12, "month", 2, PolyType.VARCHAR );
        PhysicalTable physicalTable = new PhysicalTable( 1, 1, 1, "orders", List.of( id, year, month ), 1, "public", List.of(), 1 );

        List<ParquetSourceFile> sourceFiles = new java.util.ArrayList<>();
        for ( int i = 0; i < fileRows.size(); i++ ) {
            Path file = tempDir.resolve( "physical-part-" + i + ".parquet" );
            writePhysicalPartitionParquet( file, fileRows.get( i ) );
            sourceFiles.add( ParquetSourceFile.of( file.toUri().toURL().toString(), Map.of() ) );
        }
        ParquetTableBinding binding = new ParquetTableBinding(
                sourceFiles,
                null,
                List.of(),
                Map.of(
                        id.id, new ParquetColumnBinding( id.id, "id", ParquetColumnRole.DATA, List.of( "id" ) ),
                        year.id, new ParquetColumnBinding( year.id, "year", ParquetColumnRole.DATA, List.of( "year" ) ),
                        month.id, new ParquetColumnBinding( month.id, "month", ParquetColumnRole.DATA, List.of( "month" ) ) ) );
        return new ParquetRelTable( 1, physicalTable, binding, null );
    }


    private ParquetRelTable partitionedTableWithPhysicalPartitionColumns() throws Exception {
        Path file = tempDir.resolve( "partition-collision.parquet" );
        writePhysicalPartitionParquet( file, List.of( row( 1L, "2099", "99" ), row( 2L, "2099", "99" ) ) );

        PhysicalColumn id = column( 10, "id", 0, PolyType.BIGINT );
        PhysicalColumn year = column( 11, "year", 1, PolyType.VARCHAR );
        PhysicalColumn month = column( 12, "month", 2, PolyType.VARCHAR );
        PhysicalTable physicalTable = new PhysicalTable( 1, 1, 1, "orders", List.of( id, year, month ), 1, "public", List.of(), 1 );

        ParquetSourceFile sourceFile = ParquetSourceFile.of( file.toUri().toURL().toString(), Map.of( "year", "2022", "month", "10" ) );
        ParquetTableBinding binding = new ParquetTableBinding(
                List.of( sourceFile ),
                null,
                List.of(),
                Map.of(
                        id.id, new ParquetColumnBinding( id.id, "id", ParquetColumnRole.DATA, List.of( "id" ) ),
                        year.id, new ParquetColumnBinding( year.id, "year", ParquetColumnRole.PARTITION, List.of() ),
                        month.id, new ParquetColumnBinding( month.id, "month", ParquetColumnRole.PARTITION, List.of() ) ) );
        return new ParquetRelTable( 1, physicalTable, binding, null );
    }


    private static ParquetRelMetadataAggregateExecutor metadataExecutor( ParquetRelTable table ) {
        ParquetSchemaReader schemaReader = new ParquetSchemaReader( table.getBinding().sourceFiles().stream().map( ParquetSourceFile::asSource ).toList() );
        return new ParquetRelMetadataAggregateExecutor( table, null, new int[]{ 0, 1, 2 }, schemaReader ) {
            @Override
            protected void registerAdapter( DataContext dataContext ) {
            }
        };
    }


    private static ParquetRelDataAggregateExecutor aggregateExecutor( ParquetRelTable table ) {
        ParquetSchemaReader schemaReader = new ParquetSchemaReader( table.getBinding().sourceFiles().stream().map( ParquetSourceFile::asSource ).toList() );
        return new ParquetRelDataAggregateExecutor( table, null, new int[]{ 0, 1, 2 }, schemaReader ) {
            @Override
            protected void registerAdapter( DataContext dataContext ) {
            }
        };
    }


    private static TestProjectExecutor projectExecutor( ParquetRelTable table ) {
        ParquetSchemaReader schemaReader = new ParquetSchemaReader( table.getBinding().sourceFiles().stream().map( ParquetSourceFile::asSource ).toList() );
        return new TestProjectExecutor( table, schemaReader );
    }


    private static class TestProjectExecutor extends ParquetRelProjectExecutor {

        private TestProjectExecutor( ParquetRelTable table, ParquetSchemaReader schemaReader ) {
            super( table, null, new int[]{ 0, 1, 2 }, schemaReader );
        }


        private Enumerator<PolyValue[]> enumeratorForFirstFile( int[] fields ) {
            return enumeratorForFile(
                    table,
                    table.getBinding().sourceFiles().get( 0 ),
                    fields,
                    fieldIndexes,
                    schemaReader,
                    new AtomicBoolean( false ),
                    FiltersContainer.empty );
        }


        @Override
        protected void registerAdapter( DataContext dataContext ) {
        }

    }


    private static Map<Long, Long> streamingGroupedCounts( ParquetRelTable table, List<ParquetAdapterFilter<PolyValue>> filters ) {
        Map<Long, Long> counts = new LinkedHashMap<>();
        try ( Enumerator<PolyValue[]> enumerator = aggregateExecutor( table ).createEnumerator(
                parameterContext( Map.of() ),
                new int[]{ 0, 1, 2 },
                filters,
                new int[]{ 0 },
                new String[]{ Kind.COUNT.name() },
                new int[]{ -1 } ).enumerator() ) {
            while ( enumerator.moveNext() ) {
                PolyValue[] row = enumerator.current();
                counts.put( row[0].asNumber().longValue(), row[1].asNumber().longValue() );
            }
        }
        return counts;
    }


    private static DataContext parameterContext( Map<Long, PolyValue> values ) {
        return new DataContext.SlimDataContext() {
            @Override
            public List<Map<Long, PolyValue>> getParameterValues() {
                return List.of( values );
            }
        };
    }


    private static ParquetAdapterFilter<PolyValue> dynamicFilter( int columnIndex, long parameterIndex ) {
        return new ParquetAdapterFilter<>( columnIndex, List.of(), Kind.EQUALS, null, parameterIndex );
    }


    private static PhysicalColumn column( long id, String name, int position, PolyType type ) {
        return new PhysicalColumn( id, name, name, 1, 1, 1, position, type, null, null, null, null, null, false, null, null );
    }


    private static Object[] row( Object... values ) {
        return values;
    }


    private static void writeIdParquet( Path file, List<Long> ids ) throws Exception {
        MessageType schema = Types.buildMessage()
                .required( PrimitiveTypeName.INT64 ).named( "id" )
                .named( "test" );
        SimpleGroupFactory groupFactory = new SimpleGroupFactory( schema );
        try ( ParquetWriter<Group> writer = ExampleParquetWriter.builder( new LocalOutputFile( file ) )
                .withType( schema )
                .build() ) {
            for ( long id : ids ) {
                writer.write( groupFactory.newGroup().append( "id", id ) );
            }
        }
    }


    private static void writePhysicalPartitionParquet( Path file, List<Object[]> rows ) throws Exception {
        MessageType schema = Types.buildMessage()
                .required( PrimitiveTypeName.INT64 ).named( "id" )
                .required( PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( "year" )
                .required( PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( "month" )
                .named( "test" );
        SimpleGroupFactory groupFactory = new SimpleGroupFactory( schema );
        try ( ParquetWriter<Group> writer = ExampleParquetWriter.builder( new LocalOutputFile( file ) )
                .withType( schema )
                .build() ) {
            for ( Object[] row : rows ) {
                writer.write( groupFactory.newGroup()
                        .append( "id", (Long) row[0] )
                        .append( "year", (String) row[1] )
                        .append( "month", (String) row[2] ) );
            }
        }
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

}

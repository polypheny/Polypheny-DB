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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnStatistics;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetNamespace;
import org.polypheny.db.adapter.parquet.shared.statistics.ParquetColumnStatisticsReader;
import org.polypheny.db.adapter.parquet.shared.statistics.ParquetTableStatisticsReader;
import org.polypheny.db.adapter.statistics.ProvidedColumnStatistics;
import org.polypheny.db.adapter.statistics.ProvidedEntityStatistics;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.util.Sources;

class ParquetTableStatisticsReaderTest {

    private static final long TABLE_ID = 200L;
    private static final long NAMESPACE_ID = 1L;
    private static final long ALLOCATION_ID = 300L;
    private static final long ADAPTER_ID = 400L;

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
    void providedColumnStatisticsDefensivelyCopiesUniqueValues() {
        List<PolyValue> uniqueValues = new ArrayList<>();
        uniqueValues.add( PolyString.of( "EMEA" ) );

        ProvidedColumnStatistics statistics = new ProvidedColumnStatistics( 1L, PolyString.of( "EMEA" ), PolyString.of( "EMEA" ), uniqueValues, false );
        uniqueValues.add( PolyString.of( "APAC" ) );

        assertEquals( 1, statistics.uniqueValues().size() );
        assertThrows( UnsupportedOperationException.class, () -> statistics.uniqueValues().add( PolyString.of( "NA" ) ) );
        assertTrue( new ProvidedColumnStatistics( 1L, null, null, null, true ).uniqueValues().isEmpty() );
    }


    @Test
    void readerAggregatesDataAndPartitionColumnStatistics() throws Exception {
        Path file = writeStatsParquet();
        ParquetTableStatisticsReader reader = new ParquetTableStatisticsReader( new ParquetSchemaReader( Sources.of( file.toFile() ) ), bindingFor( file ) );

        assertEquals( 5L, reader.getEntityStatistics( false ).orElseThrow().rowCount() );

        ProvidedColumnStatistics amount = reader.getColumnStatistics( logicalColumn( 10L, "amount", TABLE_ID, PolyType.DOUBLE, 0 ), 10 ).orElseThrow();
        assertEquals( 4L, amount.count() );
        assertEquals( 5.0D, Objects.requireNonNull( amount.min() ).asNumber().doubleValue() );
        assertEquals( 60.0D, Objects.requireNonNull( amount.max() ).asNumber().doubleValue() );

        ProvidedColumnStatistics region = reader.getColumnStatistics( logicalColumn( 11L, "region", TABLE_ID, PolyType.VARCHAR, 1 ), 10 ).orElseThrow();
        assertEquals( 5L, region.count() );
        assertEquals( "APAC", Objects.requireNonNull( region.min() ).asString().value );
        assertEquals( "EMEA", Objects.requireNonNull( region.max() ).asString().value );
        assertEquals( List.of( "EMEA", "APAC" ), region.uniqueValues().stream().map( value -> value.asString().value ).toList() );

        ProvidedColumnStatistics limited = reader.getColumnStatistics( logicalColumn( 11L, "region", TABLE_ID, PolyType.VARCHAR, 1 ), 1 ).orElseThrow();
        assertTrue( limited.uniqueValues().isEmpty() );
        assertTrue( limited.full() );
    }


    @Test
    void columnStatisticsReaderConvertsTemporalAndBinaryFooterStatistics() throws Exception {
        Path file = writeTemporalStatsParquet();

        Map<List<String>, ParquetColumnStatistics> statistics = ParquetColumnStatisticsReader.readAll( new ParquetSchemaReader( Sources.of( file.toFile() ) ) );

        ParquetColumnStatistics createdDate = statistics.get( List.of( "created_date" ) );
        assertEquals( PolyType.DATE, createdDate.type() );
        assertEquals( "19000", createdDate.min() );
        assertEquals( "19001", createdDate.max() );

        ParquetColumnStatistics createdTime = statistics.get( List.of( "created_time" ) );
        assertEquals( PolyType.TIME, createdTime.type() );
        assertEquals( "1000", createdTime.min() );
        assertEquals( "2500", createdTime.max() );

        ParquetColumnStatistics createdAt = statistics.get( List.of( "created_at" ) );
        assertEquals( PolyType.TIMESTAMP, createdAt.type() );
        assertEquals( "1609459200000", createdAt.min() );
        assertEquals( "1609462800000", createdAt.max() );

        ParquetColumnStatistics payload = statistics.get( List.of( "payload" ) );
        assertEquals( PolyType.VARBINARY, payload.type() );
        assertEquals( "alpha", payload.min() );
        assertEquals( "zulu", payload.max() );
    }


    @Test
    void readerFallsBackForMissingColumnsAndUnreliableRanges() throws Exception {
        Path file = writeStatsParquet();
        Map<List<String>, ParquetColumnStatistics> firstStats = new LinkedHashMap<>();
        firstStats.put( List.of( "amount" ), new ParquetColumnStatistics( PolyType.DOUBLE, 2, 2, 0L, "1.0", "2.0", true ) );
        Map<List<String>, ParquetColumnStatistics> secondStats = new LinkedHashMap<>();
        secondStats.put( List.of( "amount" ), new ParquetColumnStatistics( PolyType.VARCHAR, 3, 3, 0L, "a", "z", true ) );
        List<ParquetSourceFile> sourceFiles = List.of(
                new ParquetSourceFile( file.toUri().toURL().toExternalForm(), Map.of(), firstStats ),
                new ParquetSourceFile( file.toUri().toURL().toExternalForm(), Map.of(), secondStats ) );
        Map<Long, ParquetColumnBinding> columns = new LinkedHashMap<>();
        columns.put( 10L, new ParquetColumnBinding( 10L, "amount", ParquetColumnRole.DATA, List.of( "amount" ) ) );
        columns.put( 12L, new ParquetColumnBinding( 12L, "missing", ParquetColumnRole.DATA, List.of( "missing" ) ) );
        ParquetTableStatisticsReader reader = new ParquetTableStatisticsReader(
                new ParquetSchemaReader( Sources.of( file.toFile() ) ),
                new ParquetTableBinding( sourceFiles, null, List.of(), columns ) );

        ProvidedColumnStatistics unreliableAmount = reader.getColumnStatistics( logicalColumn( 10L, "amount", TABLE_ID, PolyType.DOUBLE, 0 ), 10 ).orElseThrow();
        assertEquals( 5L, unreliableAmount.count() );
        assertTrue( Objects.requireNonNull( unreliableAmount.min() ).isNull() );
        assertTrue( Objects.requireNonNull( unreliableAmount.max() ).isNull() );

        ProvidedColumnStatistics missing = reader.getColumnStatistics( logicalColumn( 12L, "missing", TABLE_ID, PolyType.DOUBLE, 1 ), 10 ).orElseThrow();
        assertEquals( 5L, missing.count() );
        assertTrue( Objects.requireNonNull( missing.min() ).isNull() );
        assertTrue( Objects.requireNonNull( missing.max() ).isNull() );
        assertTrue( missing.full() );
    }


    @Test
    void nestedEntityStatisticsUsesLargestNestedValueCount() throws Exception {
        Path file = writeStatsParquet();
        Map<List<String>, ParquetColumnStatistics> stats = new LinkedHashMap<>();
        stats.put( List.of( "items", "sku" ), new ParquetColumnStatistics( PolyType.VARCHAR, 2, 5, 0L, "a", "z", true ) );
        stats.put( List.of( "items", "quantity" ), new ParquetColumnStatistics( PolyType.INTEGER, 2, 7, 0L, "1", "9", true ) );
        ParquetSourceFile sourceFile = new ParquetSourceFile( file.toUri().toURL().toExternalForm(), Map.of(), stats );
        Map<Long, ParquetColumnBinding> columns = new LinkedHashMap<>();
        columns.put( 20L, new ParquetColumnBinding( 20L, "sku", ParquetColumnRole.DATA, List.of( "items", "sku" ) ) );
        columns.put( 21L, new ParquetColumnBinding( 21L, "quantity", ParquetColumnRole.DATA, List.of( "items", "quantity" ) ) );
        ParquetTableStatisticsReader reader = new ParquetTableStatisticsReader(
                new ParquetSchemaReader( Sources.of( file.toFile() ) ),
                new ParquetTableBinding( List.of( sourceFile ), "orders", List.of( "items" ), columns ) );

        assertEquals( 7L, reader.getEntityStatistics( true ).orElseThrow().rowCount() );
        assertEquals( 2L, reader.getEntityStatistics( false ).orElseThrow().rowCount() );
    }


    @Test
    void providedEntityStatisticsAllowsUnknownRowCount() {
        ProvidedEntityStatistics statistics = new ProvidedEntityStatistics( null );

        assertNull( statistics.rowCount() );
    }


    @Test
    void parquetRelTableProvidesStatisticsOnlyForMatchingLogicalEntity() throws Exception {
        Path file = writeStatsParquet();
        LogicalColumn amount = logicalColumn( 10L, "amount", TABLE_ID, PolyType.DOUBLE, 0 );
        PhysicalColumn amountPhysical = new PhysicalColumn(
                amount.id,
                "amount",
                amount.name,
                ALLOCATION_ID,
                TABLE_ID,
                ADAPTER_ID,
                0,
                amount.type,
                null,
                null,
                null,
                null,
                null,
                true,
                null,
                null );
        PhysicalTable physicalTable = new PhysicalTable(
                500L,
                ALLOCATION_ID,
                TABLE_ID,
                "orders",
                List.of( amountPhysical ),
                NAMESPACE_ID,
                "public",
                List.of( amount.id ),
                ADAPTER_ID );
        ParquetRelTable relTable = new ParquetRelTable( 600L, physicalTable, bindingFor( file ), null );

        assertEquals( 5L, relTable.getEntityStatistics( TABLE_ID ).orElseThrow().rowCount() );
        assertTrue( relTable.getEntityStatistics( TABLE_ID + 1 ).isEmpty() );
        assertEquals( 4L, relTable.getColumnStatistics( amount, 10 ).orElseThrow().count() );
        assertTrue( relTable.getColumnStatistics( logicalColumn( amount.id, amount.name, TABLE_ID + 1, amount.type, 0 ), 10 ).isEmpty() );
    }


    @Test
    void namespaceCreatesRootBindingAndEntityWrappers() throws Exception {
        Path source = writeStatsParquet();
        Path ordersFile = tempDir.resolve( "orders.parquet" );
        Files.copy( source, ordersFile );
        LogicalColumn amount = logicalColumn( 10L, "amount", TABLE_ID, PolyType.DOUBLE, 0 );
        PhysicalColumn amountPhysical = new PhysicalColumn(
                amount.id,
                "amount",
                amount.name,
                ALLOCATION_ID,
                TABLE_ID,
                ADAPTER_ID,
                0,
                amount.type,
                null,
                null,
                null,
                null,
                null,
                true,
                null,
                null );
        PhysicalTable physicalTable = new PhysicalTable(
                500L,
                ALLOCATION_ID,
                TABLE_ID,
                "orders",
                List.of( amountPhysical ),
                NAMESPACE_ID,
                "public",
                List.of( amount.id ),
                ADAPTER_ID );
        ParquetNamespace namespace = new ParquetNamespace( NAMESPACE_ID, ADAPTER_ID, tempDir.toUri().toURL() );

        ParquetTableBinding binding = namespace.createRootBinding( physicalTable );
        ParquetRelTable relTable = namespace.createParquetTable( 600L, physicalTable, binding, null );

        assertEquals( 1, binding.sourceFiles().size() );
        assertEquals( List.of( "amount" ), binding.columnsByColumnId().get( amount.id ).sourcePathElements() );
        assertEquals( 1L, relTable.getEntityStatistics( TABLE_ID ).orElseThrow().rowCount() );
    }


    private ParquetTableBinding bindingFor( Path file ) throws Exception {
        URL fileUrl = file.toUri().toURL();
        Map<List<String>, ParquetColumnStatistics> firstStats = new LinkedHashMap<>();
        firstStats.put( List.of( "amount" ), new ParquetColumnStatistics( PolyType.DOUBLE, 2, 2, 0L, "10.5", "42.0", true ) );
        Map<List<String>, ParquetColumnStatistics> secondStats = new LinkedHashMap<>();
        secondStats.put( List.of( "amount" ), new ParquetColumnStatistics( PolyType.DOUBLE, 3, 3, 1L, "5.0", "60.0", true ) );
        List<ParquetSourceFile> sourceFiles = List.of(
                new ParquetSourceFile( fileUrl.toExternalForm(), Map.of( "region", "EMEA" ), firstStats ),
                new ParquetSourceFile( fileUrl.toExternalForm(), Map.of( "region", "APAC" ), secondStats ) );
        Map<Long, ParquetColumnBinding> columns = new LinkedHashMap<>();
        columns.put( 10L, new ParquetColumnBinding( 10L, "amount", ParquetColumnRole.DATA, List.of( "amount" ) ) );
        columns.put( 11L, new ParquetColumnBinding( 11L, "region", ParquetColumnRole.PARTITION, List.of() ) );
        return new ParquetTableBinding( sourceFiles, null, List.of(), columns );
    }


    private static LogicalColumn logicalColumn( long id, String name, long tableId, PolyType type, int position ) {
        return new LogicalColumn( id, name, tableId, NAMESPACE_ID, position, type, null, null, null, null, null, true, null, null );
    }


    private Path writeStatsParquet() throws Exception {
        Path file = tempDir.resolve( "stats.parquet" );
        MessageType schema = Types.buildMessage()
                .optional( PrimitiveTypeName.DOUBLE ).named( "amount" )
                .named( "stats_schema" );
        SimpleGroupFactory factory = new SimpleGroupFactory( schema );
        try ( ParquetWriter<Group> writer = ExampleParquetWriter.builder( new LocalOutputFile( file ) )
                .withType( schema )
                .build() ) {
            writer.write( factory.newGroup().append( "amount", 1.0D ) );
        }
        return file;
    }


    private Path writeTemporalStatsParquet() throws Exception {
        Path file = tempDir.resolve( "temporal-stats.parquet" );
        MessageType schema = Types.buildMessage()
                .optional( PrimitiveTypeName.INT32 ).as( LogicalTypeAnnotation.dateType() ).named( "created_date" )
                .optional( PrimitiveTypeName.INT32 ).as( LogicalTypeAnnotation.timeType( true, LogicalTypeAnnotation.TimeUnit.MILLIS ) ).named( "created_time" )
                .optional( PrimitiveTypeName.INT64 ).as( LogicalTypeAnnotation.timestampType( true, LogicalTypeAnnotation.TimeUnit.MILLIS ) ).named( "created_at" )
                .optional( PrimitiveTypeName.BINARY ).named( "payload" )
                .named( "temporal_stats_schema" );
        SimpleGroupFactory factory = new SimpleGroupFactory( schema );
        try ( ParquetWriter<Group> writer = ExampleParquetWriter.builder( new LocalOutputFile( file ) )
                .withType( schema )
                .build() ) {
            writer.write( factory.newGroup()
                    .append( "created_date", 19000 )
                    .append( "created_time", 1000 )
                    .append( "created_at", 1609459200000L )
                    .append( "payload", "alpha" ) );
            writer.write( factory.newGroup()
                    .append( "created_date", 19001 )
                    .append( "created_time", 2500 )
                    .append( "created_at", 1609462800000L )
                    .append( "payload", "zulu" ) );
        }
        return file;
    }


    private record LocalOutputFile( Path path ) implements OutputFile {

        @Override
        public PositionOutputStream create( long blockSizeHint ) throws IOException {
            return createOrOverwrite( blockSizeHint );
        }


        @SuppressWarnings("resource")
        @Override
        public PositionOutputStream createOrOverwrite( long blockSizeHint ) throws IOException {
            Files.createDirectories( path.getParent() );
            OutputStream outputStream = Files.newOutputStream( path );
            return new PositionOutputStream() {
                private long position;


                @Override
                public long getPos() {
                    return position;
                }


                @Override
                public void write( int b ) throws IOException {
                    outputStream.write( b );
                    position++;
                }


                @Override
                public void write( byte @NotNull [] b, int off, int len ) throws IOException {
                    outputStream.write( b, off, len );
                    position += len;
                }


                @Override
                public void close() throws IOException {
                    outputStream.close();
                }
            };
        }


        @Override
        public boolean supportsBlockSize() {
            return false;
        }


        @Override
        public long defaultBlockSize() {
            return 0;
        }

    }

}

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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Types;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.parquet.relational.schema.DiscoveredTable;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSchemaNormalizer;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSyntheticColumns;
import org.polypheny.db.adapter.parquet.shared.io.ParquetFileDiscovery;


class ParquetFileDiscoveryTest {

    private static final String PREFIX = "parquet_test";


    @TempDir
    Path tempDir;


    @Test
    void groupsSchemaEvolvedFilesIntoOneTable() throws Exception {
        writeParquet( tempDir.resolve( "orders-part-000.parquet" ), schema( "id", "customer_id", "amount", "status" ), row( 1L, 10L, 100L, 1L ) );
        writeParquet( tempDir.resolve( "orders-part-001.parquet" ), schema( "id", "customer_id", "amount", "status", "discount" ), row( 2L, 20L, 200L, 1L, 3L ) );

        Map<String, DiscoveredTable> tables = ParquetFileDiscovery.discoverTables( tempDir.toUri().toURL(), PREFIX );

        assertEquals( 1, tables.size() );
        DiscoveredTable table = tables.values().iterator().next();
        assertEquals( 2, table.binding().sourceFiles().size() );
        assertEquals( List.of( "id", "customer_id", "amount", "status", "discount" ), columnNames( table.columns() ) );
    }


    @Test
    void keepsUnrelatedFilesAsSeparateTables() throws Exception {
        writeParquet( tempDir.resolve( "customers.parquet" ), schema( "id", "name" ), row( 1L, "Alice" ) );
        writeParquet( tempDir.resolve( "orders.parquet" ), schema( "id", "user_id", "amount" ), row( 10L, 1L, 99L ) );

        Map<String, DiscoveredTable> tables = ParquetFileDiscovery.discoverTables( tempDir.toUri().toURL(), PREFIX );

        assertEquals( 2, tables.size() );
        assertTrue( tables.containsKey( PREFIX + "__customers" ) );
        assertTrue( tables.containsKey( PREFIX + "__orders" ) );
        assertEquals( 1, tables.get( PREFIX + "__customers" ).binding().sourceFiles().size() );
        assertEquals( 1, tables.get( PREFIX + "__orders" ).binding().sourceFiles().size() );
    }


    @Test
    void discoversPartitionFoldersAsOneTable() throws Exception {
        Path january = tempDir.resolve( "year=2025" ).resolve( "month=01" );
        Path february = tempDir.resolve( "year=2025" ).resolve( "month=02" );
        Files.createDirectories( january );
        Files.createDirectories( february );
        writeParquet( january.resolve( "part-000.parquet" ), schema( "id", "customer_id", "amount", "status" ), row( 1L, 10L, 100L, 1L ) );
        writeParquet( february.resolve( "part-000.parquet" ), schema( "id", "customer_id", "amount", "status", "discount" ), row( 2L, 20L, 200L, 1L, 3L ) );

        Map<String, DiscoveredTable> tables = ParquetFileDiscovery.discoverTables( tempDir.toUri().toURL(), PREFIX );

        assertEquals( 1, tables.size() );
        DiscoveredTable table = tables.values().iterator().next();
        assertEquals( List.of( "id", "customer_id", "amount", "status", "discount", "year", "month" ), columnNames( table.columns() ) );
        assertEquals( 2, table.binding().sourceFiles().size() );
        assertTrue( table.binding().sourceFiles().stream().allMatch( sourceFile -> sourceFile.partitionValues().get( "year" ).equals( "2025" ) ) );
        assertEquals( List.of( "01", "02" ), table.binding().sourceFiles().stream().map( sourceFile -> sourceFile.partitionValues().get( "month" ) ).toList() );
    }


    @Test
    void discoversPartitionedTableFoldersUnderRoot() throws Exception {
        Path ordersEu = tempDir.resolve( "orders" ).resolve( "region=eu" );
        Path ordersUs = tempDir.resolve( "orders" ).resolve( "region=us" );
        Path customers = tempDir.resolve( "customers" );
        Files.createDirectories( ordersEu );
        Files.createDirectories( ordersUs );
        Files.createDirectories( customers );
        writeParquet( ordersEu.resolve( "part-000.parquet" ), schema( "id", "amount" ), row( 1L, 10L ) );
        writeParquet( ordersUs.resolve( "part-000.parquet" ), schema( "id", "amount" ), row( 2L, 20L ) );
        writeParquet( customers.resolve( "customers.parquet" ), schema( "id", "name" ), row( 1L, "Alice" ) );

        Map<String, DiscoveredTable> tables = ParquetFileDiscovery.discoverTables( tempDir.toUri().toURL(), PREFIX );

        assertEquals( 2, tables.size() );
        assertTrue( tables.containsKey( PREFIX + "__orders" ) );
        assertTrue( tables.containsKey( PREFIX + "__customers" ) );
        assertEquals( List.of( "id", "amount", "region" ), columnNames( tables.get( PREFIX + "__orders" ).columns() ) );
        assertEquals( 2, tables.get( PREFIX + "__orders" ).binding().sourceFiles().size() );
    }


    @Test
    void hidesParquetColumnWhenItMatchesPartitionValue() throws Exception {
        Path partition = tempDir.resolve( "year=2025" );
        Files.createDirectories( partition );
        writeParquet( partition.resolve( "part-000.parquet" ), schema( "id", "year", "amount" ), row( 1L, 2025L, 10L ) );

        Map<String, DiscoveredTable> tables = ParquetFileDiscovery.discoverTables( tempDir.toUri().toURL(), PREFIX );

        DiscoveredTable table = tables.values().iterator().next();
        assertEquals( List.of( "id", "amount", "year" ), columnNames( table.columns() ) );
        assertEquals( Map.of( "id", List.of( "id" ), "amount", List.of( "amount" ) ), table.binding().columnPaths() );
    }


    @Test
    void failsWhenParquetColumnConflictsWithPartitionValue() throws Exception {
        Path partition = tempDir.resolve( "year=2025" );
        Files.createDirectories( partition );
        writeParquet( partition.resolve( "part-000.parquet" ), schema( "id", "year", "amount" ), row( 1L, 2024L, 10L ) );

        assertThrows( RuntimeException.class, () -> ParquetFileDiscovery.discoverTables( tempDir.toUri().toURL(), PREFIX ) );
    }


    @Test
    void normalizedSchemaUsesDiscoveredMultiFileTable() throws Exception {
        Path january = tempDir.resolve( "year=2025" ).resolve( "month=01" );
        Path february = tempDir.resolve( "year=2025" ).resolve( "month=02" );
        Files.createDirectories( january );
        Files.createDirectories( february );
        writeParquet( january.resolve( "part-000.parquet" ), schema( "id", "customer_id", "amount", "status" ), row( 1L, 10L, 100L, 1L ) );
        writeParquet( february.resolve( "part-000.parquet" ), schema( "id", "customer_id", "amount", "status", "discount" ), row( 2L, 20L, 200L, 1L, 3L ) );

        var normalizedSchema = new ParquetSchemaNormalizer( tempDir.toUri().toURL(), PREFIX ).normalize();

        assertEquals( 1, normalizedSchema.getTables().size() );
        String tableName = normalizedSchema.getTables().keySet().iterator().next();
        assertEquals( List.of( ParquetSyntheticColumns.ROW_ID, "id", "customer_id", "amount", "status", "discount", "year", "month" ), columnNames( normalizedSchema.getTables().get( tableName ) ) );
        assertEquals( 2, normalizedSchema.getBinding( tableName ).sourceFiles().size() );
    }


    @Test
    void flatDiscoveryBindsTopLevelListColumnToRootPath() throws Exception {
        writeParquet( tempDir.resolve( "flickr.parquet" ), flickrSchema(), flickrRow() );

        Map<String, DiscoveredTable> tables = ParquetFileDiscovery.discoverTables( tempDir.toUri().toURL(), PREFIX );

        DiscoveredTable table = tables.get( PREFIX + "__flickr" );
        assertEquals( List.of( "id", "captions", "image" ), columnNames( table.columns() ) );
        assertEquals( List.of( "captions" ), table.binding().columnPaths().get( "captions" ) );
    }


    private static List<String> columnNames( List<ExportedColumn> columns ) {
        return columns.stream().map( ExportedColumn::name ).toList();
    }


    private static Object[] row( Object... values ) {
        return values;
    }


    private static MessageType schema( String... columns ) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for ( String column : columns ) {
            if ( column.equals( "name" ) ) {
                builder.required( org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY )
                        .as( org.apache.parquet.schema.LogicalTypeAnnotation.stringType() )
                        .named( column );
            } else {
                builder.required( org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64 ).named( column );
            }
        }
        return builder.named( "test" );
    }


    private static MessageType flickrSchema() {
        return MessageTypeParser.parseMessageType( """
                message test {
                  optional binary id (STRING);
                  optional group captions (LIST) {
                    repeated group list {
                      optional binary element (STRING);
                    }
                  }
                  optional binary image;
                }
                """ );
    }


    private static Object[] flickrRow() {
        return row( "image-1.jpg", List.of( "a dog in grass", "a puppy outside" ), "image-bytes" );
    }


    private static void writeParquet( Path file, MessageType schema, Object[] values ) throws Exception {
        SimpleGroupFactory groupFactory = new SimpleGroupFactory( schema );
        Group group = groupFactory.newGroup();
        for ( int i = 0; i < values.length; i++ ) {
            Object value = values[i];
            if ( value instanceof String string ) {
                group.add( i, string );
            } else if ( value instanceof List<?> list ) {
                Group captions = group.addGroup( i );
                for ( Object element : list ) {
                    captions.addGroup( "list" ).append( "element", (String) element );
                }
            } else {
                group.add( i, ((Number) value).longValue() );
            }
        }

        try ( ParquetWriter<Group> writer = ExampleParquetWriter.builder( new LocalOutputFile( file ) )
                .withType( schema )
                .build() ) {
            writer.write( group );
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

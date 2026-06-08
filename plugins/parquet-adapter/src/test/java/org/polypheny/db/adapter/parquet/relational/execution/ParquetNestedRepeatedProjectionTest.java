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

package org.polypheny.db.adapter.parquet.relational.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSyntheticColumns;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class ParquetNestedRepeatedProjectionTest {

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
    void nestedListTableProjectsParquetRootFieldsFromBindings() throws Exception {
        Path file = tempDir.resolve( "flickr.parquet" );
        writeFlickrParquet( file );

        ParquetSourceFile sourceFile = new ParquetSourceFile( file.toUri().toURL().toString(), Map.of(), Map.of() );
        ParquetRelTable table = captionsListTable( sourceFile );
        ParquetSchemaReader schemaReader = new ParquetSchemaReader( sourceFile.asSource() );

        try ( Enumerator<PolyValue[]> enumerator = ParquetRelExecutor.enumeratorForFile(
                table,
                sourceFile,
                new int[]{ 0, 1, 2, 3 },
                new int[]{ 0, 1, 2, 3 },
                schemaReader,
                new AtomicBoolean( false ),
                FiltersContainer.empty ) ) {
            assertTrue( enumerator.moveNext() );
            PolyValue[] first = enumerator.current();
            assertEquals( "0/captions[0]/list[0]", first[0].asString().value );
            assertEquals( "0/captions[0]", first[1].asString().value );
            assertEquals( 0L, first[2].asNumber().longValue() );
            assertEquals( "a dog in grass", first[3].asString().value );

            assertTrue( enumerator.moveNext() );
            PolyValue[] second = enumerator.current();
            assertEquals( "0/captions[0]/list[1]", second[0].asString().value );
            assertEquals( "0/captions[0]", second[1].asString().value );
            assertEquals( 1L, second[2].asNumber().longValue() );
            assertEquals( "a puppy outside", second[3].asString().value );

            assertFalse( enumerator.moveNext() );
        }
    }


    private static ParquetRelTable captionsListTable( ParquetSourceFile sourceFile ) {
        PhysicalColumn rowId = column( 10, ParquetSyntheticColumns.ROW_ID, 0, PolyType.VARCHAR );
        PhysicalColumn parentRowId = column( 11, ParquetSyntheticColumns.PARENT_ROW_ID, 1, PolyType.VARCHAR );
        PhysicalColumn ordinal = column( 12, ParquetSyntheticColumns.ELEM_ORDINAL, 2, PolyType.BIGINT );
        PhysicalColumn element = column( 13, "element", 3, PolyType.VARCHAR );
        PhysicalTable physicalTable = new PhysicalTable(
                1,
                1,
                1,
                "flickr8k__captions__list",
                List.of( rowId, parentRowId, ordinal, element ),
                1,
                "public",
                List.of(),
                1 );

        ParquetTableBinding binding = new ParquetTableBinding(
                List.of( sourceFile ),
                "flickr8k__captions",
                List.of( "captions", "list" ),
                Map.of(
                        rowId.id, new ParquetColumnBinding( rowId.id, rowId.name, ParquetColumnRole.PRIMARY_KEY, List.of( "captions", "list" ) ),
                        parentRowId.id, new ParquetColumnBinding( parentRowId.id, parentRowId.name, ParquetColumnRole.PARENT_KEY, List.of( "captions", "list" ) ),
                        ordinal.id, new ParquetColumnBinding( ordinal.id, ordinal.name, ParquetColumnRole.ORDINAL, List.of( "captions", "list" ) ),
                        element.id, new ParquetColumnBinding( element.id, element.name, ParquetColumnRole.DATA, List.of( "captions", "list", "element" ) ) ) );
        return new ParquetRelTable( 1, physicalTable, binding, null );
    }


    private static PhysicalColumn column( long id, String name, int position, PolyType type ) {
        return new PhysicalColumn( id, name, name, 1, 1, 1, position, type, null, null, null, null, null, false, null, null );
    }


    private static void writeFlickrParquet( Path file ) throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType( """
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

        SimpleGroupFactory groupFactory = new SimpleGroupFactory( schema );
        Group row = groupFactory.newGroup().append( "id", "image-1.jpg" );
        Group captions = row.addGroup( "captions" );
        captions.addGroup( "list" ).append( "element", "a dog in grass" );
        captions.addGroup( "list" ).append( "element", "a puppy outside" );

        try ( ParquetWriter<Group> writer = ExampleParquetWriter.builder( new LocalOutputFile( file ) )
                .withType( schema )
                .build() ) {
            writer.write( row );
        }
    }


    private record LocalOutputFile( Path path ) implements OutputFile {

        @Override
        public PositionOutputStream create( long blockSizeHint ) {
            return createOrOverwrite( blockSizeHint );
        }


        @Override
        public PositionOutputStream createOrOverwrite( long blockSizeHint ) {
            try {
                Files.createDirectories( path.getParent() );
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
            try {
                return new LocalPositionOutputStream( Files.newOutputStream( path ) );
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
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

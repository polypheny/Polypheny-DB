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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.polypheny.db.adapter.parquet.shared.io.ParquetPrimitiveRowReader;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.util.Sources;


class ParquetPrimitiveRowReaderTest {

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
    void readsPrimitiveProjectionWithoutGroupMaterialization() throws Exception {
        Path file = tempDir.resolve( "flat.parquet" );
        MessageType schema = Types.buildMessage()
                .required( PrimitiveTypeName.INT64 ).named( "id" )
                .optional( PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( "name" )
                .required( PrimitiveTypeName.DOUBLE ).named( "amount" )
                .named( "test" );
        writeParquet( file, schema );

        var source = Sources.of( file.toFile() );
        var schemaReader = new ParquetSchemaReader( source );
        assertTrue( ParquetPrimitiveRowReader.supports( schemaReader.buildProjectionSchema( new int[]{ 0, 1, 2 } ) ) );

        try ( ParquetPrimitiveRowReader reader = new ParquetPrimitiveRowReader( source, new AtomicBoolean( false ), new int[]{ 0, 1, 2 }, List.of() ) ) {
            PolyValue[] first = reader.next();
            assertEquals( 1L, first[0].asNumber().longValue() );
            assertEquals( "Alice", first[1].asString().value );
            assertEquals( 10.5D, first[2].asNumber().doubleValue() );

            PolyValue[] second = reader.next();
            assertEquals( 2L, second[0].asNumber().longValue() );
            assertTrue( second[1].isNull() );
            assertEquals( 20.25D, second[2].asNumber().doubleValue() );

            assertNull( reader.next() );
        }
    }


    @Test
    void rejectsRepeatedProjection() {
        MessageType schema = Types.buildMessage()
                .repeated( PrimitiveTypeName.INT64 ).named( "id" )
                .named( "test" );

        assertFalse( ParquetPrimitiveRowReader.supports( schema ) );
    }


    private static void writeParquet( Path file, MessageType schema ) throws Exception {
        SimpleGroupFactory groupFactory = new SimpleGroupFactory( schema );
        Group first = groupFactory.newGroup()
                .append( "id", 1L )
                .append( "name", "Alice" )
                .append( "amount", 10.5D );
        Group second = groupFactory.newGroup()
                .append( "id", 2L )
                .append( "amount", 20.25D );

        try ( ParquetWriter<Group> writer = ExampleParquetWriter.builder( new LocalOutputFile( file ) )
                .withType( schema )
                .build() ) {
            writer.write( first );
            writer.write( second );
        }
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

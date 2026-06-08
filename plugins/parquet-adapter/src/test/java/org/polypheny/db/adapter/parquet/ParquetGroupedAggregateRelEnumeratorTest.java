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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.polypheny.db.adapter.parquet.relational.execution.aggregate.ParquetGroupedAggregateRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateCallDescriptor;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class ParquetGroupedAggregateRelEnumeratorTest {

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
    void supportsDistinctOnlyGroupBy() throws Exception {
        Path file = tempDir.resolve( "flags.parquet" );
        MessageType schema = Types.buildMessage()
                .required( PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( "flag_a" )
                .required( PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( "flag_b" )
                .named( "test" );
        writeParquet( file, schema, List.of(
                row( "Y", "N" ),
                row( "Y", "N" ),
                row( "N", "N" ),
                row( "Y", "Y" ) ) );

        Set<List<String>> values = new HashSet<>();
        try ( ParquetGroupedAggregateRelEnumerator enumerator = new ParquetGroupedAggregateRelEnumerator(
                table( file ),
                new int[]{ 0, 1 },
                2,
                new AggregateCallDescriptor[0],
                List.of(),
                new AtomicBoolean( false ) ) ) {
            while ( enumerator.moveNext() ) {
                PolyValue[] row = enumerator.current();
                values.add( List.of( row[0].asString().value, row[1].asString().value ) );
            }
        }

        assertEquals( Set.of( List.of( "Y", "N" ), List.of( "N", "N" ), List.of( "Y", "Y" ) ), values );
    }


    @Test
    void scalarCountReturnsZeroWhenStatisticsPruneEveryFile() throws Exception {
        Path file = tempDir.resolve( "flags.parquet" );
        MessageType schema = Types.buildMessage()
                .required( PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( "flag_a" )
                .required( PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( "flag_b" )
                .named( "test" );
        writeParquet( file, schema, List.<Object[]>of( row( "Y", "N" ) ) );

        try ( ParquetGroupedAggregateRelEnumerator enumerator = new ParquetGroupedAggregateRelEnumerator(
                table( file ),
                new int[]{ 0 },
                0,
                new AggregateCallDescriptor[]{ AggregateCallDescriptor.countStar() },
                List.of( new ParquetAdapterFilter<>( 0, Kind.EQUALS, PolyString.of( "Z" ) ) ),
                new AtomicBoolean( false ) ) ) {
            assertTrue( enumerator.moveNext() );
            assertEquals( 0L, enumerator.current()[0].asNumber().longValue() );
            assertFalse( enumerator.moveNext() );
        }
    }


    private static ParquetRelTable table( Path file ) throws Exception {
        PhysicalColumn flagA = column( 10, "flag_a", 0 );
        PhysicalColumn flagB = column( 11, "flag_b", 1 );
        PhysicalTable physicalTable = new PhysicalTable( 1, 1, 1, "flags", List.of( flagA, flagB ), 1, "public", List.of(), 1 );
        ParquetSourceFile sourceFile = ParquetSourceFile.of( file.toUri().toURL().toString(), Map.of() );
        ParquetTableBinding binding = new ParquetTableBinding(
                List.of( sourceFile ),
                null,
                List.of(),
                Map.of(
                        flagA.id, new ParquetColumnBinding( flagA.id, "flag_a", ParquetColumnRole.DATA, List.of( "flag_a" ) ),
                        flagB.id, new ParquetColumnBinding( flagB.id, "flag_b", ParquetColumnRole.DATA, List.of( "flag_b" ) ) ) );
        return new ParquetRelTable( 1, physicalTable, binding, null );
    }


    private static PhysicalColumn column( long id, String name, int position ) {
        return new PhysicalColumn( id, name, name, 1, 1, 1, position, PolyType.VARCHAR, null, null, null, null, null, false, null, null );
    }


    private static Object[] row( Object... values ) {
        return values;
    }


    private static void writeParquet( Path file, MessageType schema, List<Object[]> rows ) throws Exception {
        SimpleGroupFactory groupFactory = new SimpleGroupFactory( schema );
        try ( ParquetWriter<Group> writer = ExampleParquetWriter.builder( new LocalOutputFile( file ) )
                .withType( schema )
                .build() ) {
            for ( Object[] row : rows ) {
                Group group = groupFactory.newGroup();
                group.add( 0, (String) row[0] );
                group.add( 1, (String) row[1] );
                writer.write( group );
            }
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

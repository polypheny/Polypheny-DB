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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateCallDescriptor;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateGroupState;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetPrimitivePredicate;
import org.polypheny.db.adapter.parquet.shared.aggregate.GroupKey;
import org.polypheny.db.adapter.parquet.shared.io.aggregate.page.NoOpColumnConverter;
import org.polypheny.db.adapter.parquet.shared.io.aggregate.page.ParquetCountAggregatePageReader;
import org.polypheny.db.adapter.parquet.shared.util.HadoopConfigurationFactory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


class ParquetCountAggregatePageReaderTest {

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
    void evaluatesPredicateForEveryRow() throws Exception {
        MessageType schema = Types.buildMessage()
                .required( PrimitiveTypeName.DOUBLE ).named( "trip_miles" )
                .named( "test" );
        Path file = writeParquet( schema );
        List<ParquetAdapterFilter<PolyValue>> filters = List.of(
                new ParquetAdapterFilter<>( 0, Kind.GREATER_THAN_OR_EQUAL, PolyDouble.of( 5D ) ) );

        Configuration conf = HadoopConfigurationFactory.create( getClass().getClassLoader() );
        org.apache.hadoop.fs.Path parquetPath = new org.apache.hadoop.fs.Path( file.toUri() );
        try ( ParquetFileReader fileReader = ParquetFileReader.open( HadoopInputFile.fromPath( parquetPath, conf ), ParquetReadOptions.builder().build() ) ) {
            fileReader.setRequestedSchema( schema );
            try ( PageReadStore pages = fileReader.readNextRowGroup() ) {
                ColumnReadStore store = new ColumnReadStoreImpl( pages, new NoOpColumnConverter( schema ), schema, fileReader.getFileMetaData().getCreatedBy() );
                ColumnDescriptor[] descriptors = schema.getColumns().toArray( ColumnDescriptor[]::new );
                AggregateCallDescriptor[] aggregateCalls = new AggregateCallDescriptor[]{ AggregateCallDescriptor.countStar() };
                Map<GroupKey, AggregateGroupState> aggregates = new LinkedHashMap<>();

                new ParquetCountAggregatePageReader( pages, store, descriptors, ParquetPrimitivePredicate.compile( schema, filters ) )
                        .read( aggregates, aggregateCalls, filters, new AtomicBoolean( false ) );

                assertEquals( 3, aggregates.get( GroupKey.Empty ).count( 0 ) );
            }
        }
    }


    @Test
    void keepsUnreadPredicateColumnsAlignedAfterShortCircuit() throws Exception {
        MessageType schema = Types.buildMessage()
                .required( PrimitiveTypeName.DOUBLE ).named( "trip_distance" )
                .required( PrimitiveTypeName.DOUBLE ).named( "total_amount" )
                .named( "test" );
        Path file = writeTwoColumnParquet( schema );
        List<ParquetAdapterFilter<PolyValue>> filters = List.of(
                new ParquetAdapterFilter<>( 0, Kind.GREATER_THAN_OR_EQUAL, PolyDouble.of( 10D ) ),
                new ParquetAdapterFilter<>( 1, Kind.GREATER_THAN_OR_EQUAL, PolyDouble.of( 40D ) ) );

        Configuration conf = HadoopConfigurationFactory.create( getClass().getClassLoader() );
        org.apache.hadoop.fs.Path parquetPath = new org.apache.hadoop.fs.Path( file.toUri() );
        try ( ParquetFileReader fileReader = ParquetFileReader.open( HadoopInputFile.fromPath( parquetPath, conf ), ParquetReadOptions.builder().build() ) ) {
            fileReader.setRequestedSchema( schema );
            try ( PageReadStore pages = fileReader.readNextRowGroup() ) {
                ColumnReadStore store = new ColumnReadStoreImpl( pages, new NoOpColumnConverter( schema ), schema, fileReader.getFileMetaData().getCreatedBy() );
                ColumnDescriptor[] descriptors = schema.getColumns().toArray( ColumnDescriptor[]::new );
                AggregateCallDescriptor[] aggregateCalls = new AggregateCallDescriptor[]{ AggregateCallDescriptor.countStar() };
                Map<GroupKey, AggregateGroupState> aggregates = new LinkedHashMap<>();

                new ParquetCountAggregatePageReader( pages, store, descriptors, ParquetPrimitivePredicate.compile( schema, filters ) )
                        .read( aggregates, aggregateCalls, filters, new AtomicBoolean( false ) );

                assertEquals( 0, aggregates.get( GroupKey.Empty ).count( 0 ) );
            }
        }
    }


    private Path writeParquet( MessageType schema ) throws Exception {
        Path file = tempDir.resolve( "count.parquet" );
        SimpleGroupFactory factory = new SimpleGroupFactory( schema );
        try ( ParquetWriter<Group> writer = ExampleParquetWriter.builder( new LocalOutputFile( file ) )
                .withType( schema )
                .build() ) {
            writer.write( factory.newGroup().append( "trip_miles", 1D ) );
            writer.write( factory.newGroup().append( "trip_miles", 5D ) );
            writer.write( factory.newGroup().append( "trip_miles", 7D ) );
            writer.write( factory.newGroup().append( "trip_miles", 8D ) );
        }
        return file;
    }


    private Path writeTwoColumnParquet( MessageType schema ) throws Exception {
        Path file = tempDir.resolve( "count_two_columns.parquet" );
        SimpleGroupFactory factory = new SimpleGroupFactory( schema );
        try ( ParquetWriter<Group> writer = ExampleParquetWriter.builder( new LocalOutputFile( file ) )
                .withType( schema )
                .build() ) {
            writer.write( factory.newGroup().append( "trip_distance", 1D ).append( "total_amount", 100D ) );
            writer.write( factory.newGroup().append( "trip_distance", 10D ).append( "total_amount", 20D ) );
            writer.write( factory.newGroup().append( "trip_distance", 11D ).append( "total_amount", 20D ) );
            writer.write( factory.newGroup().append( "trip_distance", 12D ).append( "total_amount", 20D ) );
        }
        return file;
    }


    private record LocalOutputFile( Path path ) implements OutputFile {

        @Override
        public PositionOutputStream create( long blockSizeHint ) throws IOException {
            return createOrOverwrite( blockSizeHint );
        }


        @Override
        public PositionOutputStream createOrOverwrite( long blockSizeHint ) throws IOException {
            Files.createDirectories( path.getParent() );
            @SuppressWarnings("resource") OutputStream outputStream = Files.newOutputStream( path );
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

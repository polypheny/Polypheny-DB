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

package org.polypheny.db.adapter.parquet.shared.io;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetNativeFilterBuilder;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.adapter.parquet.shared.util.HadoopConfigurationFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;


/**
 * Reads primitive columns directly into PolyValue[].
 * It supports only primitive non-repeated projections. No nested structures.
 * It avoids GroupRecordConverter and materializes directly into PolyValue[].
 * For primitive columns it is much faster than regular ParquetSourceReader.
 */
public class ParquetPrimitiveRowReader implements AutoCloseable {

    private final AtomicBoolean cancelFlag;
    private final ParquetFileReader fileReader;
    private final boolean useNativeFilter;
    private final MessageType projectionSchema;

    private RecordReader<PolyValue[]> recordReader;
    private long rowCountInGroup;
    private long rowIndexInGroup;


    public ParquetPrimitiveRowReader( Source source, AtomicBoolean cancelFlag, int[] fields, List<ParquetAdapterFilter<PolyValue>> filters ) {
        this.cancelFlag = cancelFlag == null ? new AtomicBoolean( false ) : cancelFlag;

        try {
            URI uri = source.isFile() ? source.file().toURI() : source.url().toURI();
            Path path = new Path( uri );
            Configuration conf = HadoopConfigurationFactory.create( this.getClass().getClassLoader() );

            ParquetSchemaReader schemaReader = new ParquetSchemaReader( source );
            this.projectionSchema = schemaReader.buildProjectionSchema( fields );
            var recordFilter = ParquetNativeFilterBuilder.build( schemaReader.getSchema(), filters == null ? List.of() : filters );
            this.useNativeFilter = FilterCompat.isFilteringRequired( recordFilter );

            ParquetReadOptions readOptions = ParquetReadOptions.builder()
                    .useStatsFilter()
                    .useDictionaryFilter()
                    .useColumnIndexFilter()
                    .useBloomFilter()
                    .useRecordFilter()
                    .withRecordFilter( recordFilter )
                    .build();

            this.fileReader = ParquetFileReader.open( HadoopInputFile.fromPath( path, conf ), readOptions );
            this.fileReader.setRequestedSchema( projectionSchema );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Unable to open parquet file: " + source.path(), e );
        }
    }


    public static boolean supports( MessageType projectionSchema ) {
        if ( projectionSchema.getFieldCount() == 0 ) {
            return false;
        }
        for ( Type field : projectionSchema.getFields() ) {
            if ( !field.isPrimitive() || field.isRepetition( Type.Repetition.REPEATED ) ) {
                return false;
            }
        }
        return true;
    }


    public PolyValue[] next() {
        try {
            if ( ParquetCancellation.shouldStop( rowIndexInGroup, cancelFlag ) ) {
                return null;
            }
            if ( !ensureRecordReader() ) {
                return null;
            }

            PolyValue[] row = recordReader.read();
            rowIndexInGroup++;
            if ( rowIndexInGroup >= rowCountInGroup ) {
                recordReader = null;
            }
            return row;
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error while reading parquet data", e );
        }
    }


    @Override
    public void close() throws IOException {
        fileReader.close();
    }


    private boolean ensureRecordReader() throws IOException {
        if ( recordReader != null && rowIndexInGroup < rowCountInGroup ) {
            return true;
        }

        PageReadStore pages = useNativeFilter
                ? fileReader.readNextFilteredRowGroup()
                : fileReader.readNextRowGroup();
        if ( pages == null ) {
            return false;
        }

        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO( projectionSchema );
        this.recordReader = columnIO.getRecordReader( pages, new PrimitiveRowMaterializer( projectionSchema ) );
        this.rowCountInGroup = pages.getRowCount();
        this.rowIndexInGroup = 0;
        return true;
    }


    private static class PrimitiveRowMaterializer extends RecordMaterializer<PolyValue[]> {

        private final PrimitiveRowConverter root;
        private PolyValue[] currentRow;


        private PrimitiveRowMaterializer( MessageType projectionSchema ) {
            this.root = new PrimitiveRowConverter( projectionSchema );
        }


        @Override
        public PolyValue[] getCurrentRecord() {
            return currentRow;
        }


        @Override
        public GroupConverter getRootConverter() {
            return root;
        }


        private class PrimitiveRowConverter extends GroupConverter {

            private final Converter[] converters;


            private PrimitiveRowConverter( MessageType projectionSchema ) {
                this.converters = new Converter[projectionSchema.getFieldCount()];
                for ( int i = 0; i < projectionSchema.getFieldCount(); i++ ) {
                    converters[i] = new PrimitiveFieldConverter( i, projectionSchema.getType( i ).asPrimitiveType() );
                }
            }


            @Override
            public Converter getConverter( int fieldIndex ) {
                return converters[fieldIndex];
            }


            @Override
            public void start() {
                currentRow = new PolyValue[converters.length];
                Arrays.fill( currentRow, PolyNull.NULL );
            }


            @Override
            public void end() {
            }

        }


        private class PrimitiveFieldConverter extends PrimitiveConverter {

            private static final ParquetTypeConverter TYPE_CONVERTER = new ParquetTypeConverter();

            private final int index;
            private final PrimitiveType type;


            private PrimitiveFieldConverter( int index, PrimitiveType type ) {
                this.index = index;
                this.type = type;
            }


            @Override
            public void addBoolean( boolean value ) {
                currentRow[index] = TYPE_CONVERTER.fromObjToPolyValue( type, value );
            }


            @Override
            public void addInt( int value ) {
                currentRow[index] = TYPE_CONVERTER.fromObjToPolyValue( type, value );
            }


            @Override
            public void addLong( long value ) {
                currentRow[index] = TYPE_CONVERTER.fromObjToPolyValue( type, value );
            }


            @Override
            public void addFloat( float value ) {
                currentRow[index] = TYPE_CONVERTER.fromObjToPolyValue( type, value );
            }


            @Override
            public void addDouble( double value ) {
                currentRow[index] = TYPE_CONVERTER.fromObjToPolyValue( type, value );
            }


            @Override
            public void addBinary( Binary value ) {
                currentRow[index] = TYPE_CONVERTER.fromObjToPolyValue( type, value );
            }

        }

    }

}

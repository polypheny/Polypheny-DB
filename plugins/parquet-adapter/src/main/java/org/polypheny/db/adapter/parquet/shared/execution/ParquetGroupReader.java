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

package org.polypheny.db.adapter.parquet.shared.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import lombok.Getter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.model.AdapterFilter;
import org.polypheny.db.adapter.parquet.shared.util.HadoopConfigurationFactory;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelFilterTranslator;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Source;

public class ParquetGroupReader implements AutoCloseable {

    private final AtomicBoolean cancelFlag;
    private final ParquetFileReader fileReader;
    private final boolean useNativeFilter;

    @Getter
    private final MessageType projectionSchema;

    private RecordReader<Group> recordReader;
    private long rowCountInGroup;
    private long rowIndexInGroup;

    @Getter
    private long currentRowNumber = -1;


    public ParquetGroupReader( Source source, AtomicBoolean cancelFlag, int[] fields, List<AdapterFilter> filters ) {
        this.cancelFlag = cancelFlag;

        try {
            Path path = new Path( source.url().toURI() );
            Configuration conf = HadoopConfigurationFactory.create( this.getClass().getClassLoader() );
            var inputFile = HadoopInputFile.fromPath( path, conf );

            MessageType schema;
            try ( ParquetFileReader schemaReader = ParquetFileReader.open( inputFile ) ) {
                schema = schemaReader.getFooter().getFileMetaData().getSchema();
            }

            int[] projectedFields = buildProjectedFields( fields, schema.getFieldCount() );
            this.projectionSchema = buildProjectionSchema( schema, projectedFields );

            var recordFilter = new ParquetRelFilterTranslator().translate( schema, filters == null ? List.of() : filters );
            this.useNativeFilter = FilterCompat.isFilteringRequired( recordFilter );

            ParquetReadOptions.Builder readOptionsBuilder = ParquetReadOptions.builder()
                    .useStatsFilter()
                    .useDictionaryFilter()
                    .useColumnIndexFilter()
                    .useBloomFilter()
                    .useRecordFilter();

            if ( useNativeFilter ) {
                readOptionsBuilder.withRecordFilter( recordFilter );
            }

            this.fileReader = ParquetFileReader.open( inputFile, readOptionsBuilder.build() );
            this.fileReader.setRequestedSchema( projectionSchema );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Unable to open parquet file: " + source.path(), e );
        }
    }


    public Group next() {
        try {
            for ( ; ; ) {
                if ( cancelFlag.get() ) {
                    return null;
                }

                if ( !ensureRecordReader() ) {
                    return null;
                }

                Group group = recordReader.read();
                rowIndexInGroup++;
                if ( group == null ) {
                    recordReader = null;
                    continue;
                }

                currentRowNumber++;
                return group;
            }
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error while reading parquet data", e );
        }
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
        this.recordReader = columnIO.getRecordReader( pages, new GroupRecordConverter( projectionSchema ) );
        this.rowCountInGroup = pages.getRowCount();
        this.rowIndexInGroup = 0;
        return true;
    }


    private static int[] buildProjectedFields( int[] projectedFields, int schemaFieldCount ) {
        if ( projectedFields == null || projectedFields.length == 0 ) {
            return IntStream.range( 0, schemaFieldCount ).toArray();
        }
        return projectedFields;
    }


    private static MessageType buildProjectionSchema( MessageType fullSchema, int[] projectedFieldIndexes ) {
        if ( projectedFieldIndexes.length == 0 ) {
            return fullSchema;
        }

        List<Type> fields = new ArrayList<>( projectedFieldIndexes.length );
        for ( int index : projectedFieldIndexes ) {
            fields.add( fullSchema.getType( index ) );
        }
        return new MessageType( fullSchema.getName(), fields );
    }


    @Override
    public void close() {
        try {
            fileReader.close();
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Error closing parquet reader", e );
        }
    }

}

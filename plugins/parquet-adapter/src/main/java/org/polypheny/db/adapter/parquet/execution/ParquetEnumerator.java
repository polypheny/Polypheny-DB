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

package org.polypheny.db.adapter.parquet.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.model.FilterInfo;
import org.polypheny.db.adapter.parquet.schema.ParquetTypeConverter;
import org.polypheny.db.adapter.parquet.util.HadoopConfigurationFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

public class ParquetEnumerator implements Enumerator<PolyValue[]> {

    private final AtomicBoolean cancelFlag;
    private final ParquetFileReader fileReader;
    private final MessageType projectionSchema;
    private final ValueExtractor valueExtractor;
    private final ParquetTypeConverter typeConverter;
    private final boolean useNativeFilter;

    private RecordReader<Group> recordReader;
    private long rowCountInGroup;
    private long rowIndexInGroup;
    private PolyValue[] current;


    /**
     * Constructor:
     * creates file reader with push-down filter configuration
     *
     * @param source file
     * @param cancelFlag used in moveNext()
     * @param fields columns
     * @throws GenericRuntimeException on failure
     */
    public ParquetEnumerator( Source source, AtomicBoolean cancelFlag, int[] fields ) {
        this( source, cancelFlag, fields, List.of() );
    }


    /**
     * Constructor:
     * creates file reader with push-down filter configuration
     *
     * @param source file
     * @param cancelFlag used in moveNext()
     * @param fields columns
     * @param filters list of parquet filters
     * @throws GenericRuntimeException on failure
     */
    public ParquetEnumerator( Source source, AtomicBoolean cancelFlag, int[] fields, List<FilterInfo> filters ) {

        this.cancelFlag = cancelFlag;
        this.valueExtractor = new ValueExtractor();
        this.typeConverter = new ParquetTypeConverter();

        try {
            // build HadoopInputFile object from provided path
            Path path = new Path( source.url().toURI() );
            Configuration conf = HadoopConfigurationFactory.create( this.getClass().getClassLoader() );
            var inputFile = HadoopInputFile.fromPath( path, conf );

            // create parquet file schema
            MessageType schema;
            try ( ParquetFileReader schemaReader = ParquetFileReader.open( inputFile ) ) {
                schema = schemaReader.getFooter().getFileMetaData().getSchema();
            }

            // create projection from provided projected fields and the whole schema:
            // if no projection provided return all existing in schema columns
            int[] projectedFields = buildProjectedFields( fields, schema.getFieldCount() );
            // build new parquet schema based on provided projection
            this.projectionSchema = buildProjectionSchema( schema, projectedFields );

            // translate filters provided by Polypheny into a Parquet-native filter object
            var recordFilter = new ParquetPredicateBuilder().translate( schema, filters == null ? List.of() : filters );
            // check if there is an actual predicate to apply or is there no filter at all
            this.useNativeFilter = FilterCompat.isFilteringRequired( recordFilter );

            // set configuration for pushing down
            ParquetReadOptions.Builder readOptionsBuilder = ParquetReadOptions.builder()
                    .useStatsFilter()
                    .useDictionaryFilter()
                    .useColumnIndexFilter()
                    .useBloomFilter()
                    .useRecordFilter();

            if ( useNativeFilter ) {
                readOptionsBuilder.withRecordFilter( recordFilter );
            }

            // enable Parquet to apply the filter internally
            this.fileReader = ParquetFileReader.open( inputFile, readOptionsBuilder.build() );
            this.fileReader.setRequestedSchema( projectionSchema );

            this.recordReader = null;
            this.rowCountInGroup = 0;
            this.rowIndexInGroup = 0;

        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Unable to open parquet file: " + source.path(), e );
        }
    }

    //region Enumerator operations


    /**
     * return current row
     *
     * @return current row
     */
    @Override
    public PolyValue[] current() {
        return current;
    }


    /**
     * Move to the next matching row
     *
     * @return True if there is a new row or False otherwise.
     */
    @Override
    public boolean moveNext() {
        try {
            for ( ; ; ) {
                if ( cancelFlag.get() ) {
                    current = null;
                    return false;
                }
                // ensure that reader deals with a correct row group
                if ( !ensureRecordReader() ) {
                    current = null;
                    return false;
                }
                // read one row from current row group
                Group group = recordReader.read();
                rowIndexInGroup++;
                if ( group == null ) {
                    recordReader = null;
                    continue;
                }
                // convert to PolyValue object array
                current = extractRow( group );
                return true;
            }
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error while reading parquet data", e );
        }
    }


    /**
     * Reset is not supported
     */
    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }


    /**
     * Close reader
     */
    @Override
    public void close() {
        try {
            fileReader.close();
        } catch ( IOException e ) {
            throw new GenericRuntimeException( "Error closing parquet reader", e );
        }
    }
    //endregion


    /**
     * Ensures a record reader is available for the current row group.
     *
     * @return true if reader was created, false - if no compatible row groups found
     * @throws IOException when read fails
     */
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

        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO( this.projectionSchema );
        this.recordReader = columnIO.getRecordReader( pages, new GroupRecordConverter( this.projectionSchema ) );
        this.rowCountInGroup = pages.getRowCount();
        this.rowIndexInGroup = 0;
        return true;
    }


    /**
     * Extracts one row using the requested read schema.
     */
    private PolyValue[] extractRow( Group group ) {
        PolyValue[] row = new PolyValue[projectionSchema.getFieldCount()];
        for ( int readIndex = 0; readIndex < projectionSchema.getFieldCount(); readIndex++ ) {
            var type = projectionSchema.getType( readIndex );
            var value = valueExtractor.extractValue( group, readIndex, type );
            row[readIndex] = typeConverter.fromObjToPolyValue( type, value );
        }
        return row;
    }


    /**
     * If no projection provided return all existing in schema columns
     */
    private static int[] buildProjectedFields( int[] projectedFields, int schemaFieldCount ) {
        if ( projectedFields == null || projectedFields.length == 0 ) {
            return IntStream.range( 0, schemaFieldCount ).toArray();
        }
        return projectedFields;
    }


    /**
     * Builds the projected parquet schema.
     *
     * @param fullSchema - full parquet file schema
     * @param projectedFieldIndexes - required projection
     * @return org.apache.parquet.schema.MessageType - new schema
     */
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

}

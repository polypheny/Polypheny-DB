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

package org.polypheny.db.adapter.parquet.shared.io.aggregate;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateColumnAccumulator;
import org.polypheny.db.adapter.parquet.shared.aggregate.ColumnAggregateResult;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.io.aggregate.page.NoOpColumnConverter;
import org.polypheny.db.adapter.parquet.shared.util.HadoopConfigurationFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Source;


/**
 * This class is the fastest option to aggregate primitive columns with no filters. The columns are being read and aggregated one by one not as a row.
 */
public class ParquetNoFilterColumnAggregateReader implements AutoCloseable {

    private final AtomicBoolean cancelFlag;
    private final ParquetFileReader fileReader;
    private final MessageType projectionSchema;
    private final String createdBy;


    public ParquetNoFilterColumnAggregateReader( Source source, AtomicBoolean cancelFlag, int[] fields ) {
        this.cancelFlag = cancelFlag == null ? new AtomicBoolean( false ) : cancelFlag;

        try {
            URI uri = source.isFile() ? source.file().toURI() : source.url().toURI();
            Path path = new Path( uri );
            Configuration conf = HadoopConfigurationFactory.create( this.getClass().getClassLoader() );

            ParquetSchemaReader schemaReader = new ParquetSchemaReader( source );
            this.projectionSchema = schemaReader.buildProjectionSchema( fields );

            ParquetReadOptions readOptions = ParquetReadOptions.builder()
                    .useStatsFilter()
                    .useDictionaryFilter()
                    .useColumnIndexFilter()
                    .useBloomFilter()
                    .build();

            this.fileReader = ParquetFileReader.open( HadoopInputFile.fromPath( path, conf ), readOptions );
            this.fileReader.setRequestedSchema( projectionSchema );
            this.createdBy = fileReader.getFileMetaData().getCreatedBy();
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Unable to open parquet file: " + source.path(), e );
        }
    }


    /**
     * Checks if the projection schema can be supported by this reader.
     *
     * @param projectionSchema a projection schema to validate.
     * @return {@code true} if the projection schema contains only primitive, numeric and non-repeated columns and {@code false} otherwise.
     */
    public static boolean supports( MessageType projectionSchema ) {
        if ( projectionSchema.getFieldCount() == 0 ) {
            return false;
        }
        for ( Type field : projectionSchema.getFields() ) {
            if ( !field.isPrimitive() || field.isRepetition( Type.Repetition.REPEATED ) || !isNumeric( field.asPrimitiveType() ) ) {
                return false;
            }
        }
        return true;
    }


    /**
     * Checks if the column type is a numeric type.
     *
     * @param type the column type to check.
     * @return {@code true} if the column type is numeric type and {@code false} otherwise.
     */
    private static boolean isNumeric( PrimitiveType type ) {
        return switch ( type.getPrimitiveTypeName() ) {
            case INT32, INT64, FLOAT, DOUBLE -> true;
            case BOOLEAN, BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> false;
        };
    }


    /**
     * Reads all projection columns one by one and aggregates their values. The supported aggregations are COUNT, SUM, MIN and MAX.
     *
     * @return an array of aggregation results per column.
     */
    public ColumnAggregateResult[] readAggregateColumns() {
        ColumnAggregateResult[] results = new ColumnAggregateResult[projectionSchema.getFieldCount()];
        Arrays.setAll( results, ignored -> ColumnAggregateResult.empty() );

        try {
            PageReadStore pages;

            while ( !cancelFlag.get() && (pages = fileReader.readNextRowGroup()) != null ) {
                try {
                    readPage( pages, results );
                } finally {
                    pages.close();
                }
            }
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error while reading parquet aggregate values", e );
        }

        return results;
    }


    @Override
    public void close() throws IOException {
        fileReader.close();
    }


    /**
     * Reads projected columns from the page and adds their aggregate results.
     *
     * @param pages a pages store.
     * @param results aggregated column results.
     */
    private void readPage( PageReadStore pages, ColumnAggregateResult[] results ) {
        ColumnReadStore store = new ColumnReadStoreImpl( pages, new NoOpColumnConverter( projectionSchema ), projectionSchema, createdBy );
        ColumnDescriptor[] descriptors = projectionSchema.getColumns().toArray( ColumnDescriptor[]::new );
        AggregateColumnAccumulator[] accumulators = new AggregateColumnAccumulator[descriptors.length];

        for ( int i = 0; i < descriptors.length; i++ ) {
            accumulators[i] = AggregateColumnAccumulator.create( store.getColumnReader( descriptors[i] ), descriptors[i], pages.getRowCount(), cancelFlag );
        }

        for ( AggregateColumnAccumulator accumulator : accumulators ) {
            accumulator.read();
        }

        for ( int i = 0; i < accumulators.length; i++ ) {
            results[i] = results[i].merge( accumulators[i].result() );
        }
    }


}

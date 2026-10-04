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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateCallDescriptor;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateGroupState;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetNativeFilterBuilder;
import org.polypheny.db.adapter.parquet.shared.aggregate.GroupKey;
import org.polypheny.db.adapter.parquet.shared.io.ParquetPrimitivePredicate;
import org.polypheny.db.adapter.parquet.shared.io.aggregate.page.ParquetCountAggregatePageReader;
import org.polypheny.db.adapter.parquet.shared.io.aggregate.page.ParquetGroupedAggregatePageReader;
import org.polypheny.db.adapter.parquet.shared.io.aggregate.page.NoOpColumnConverter;
import org.polypheny.db.adapter.parquet.shared.util.HadoopConfigurationFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;


/**
 * This reader performs grouped aggregation from Parquet pages and can handle row filters.
 */
public class ParquetGroupedAggregateReader implements AutoCloseable {

    private final AtomicBoolean cancelFlag;
    private final ParquetFileReader fileReader;
    private final MessageType projectionSchema;
    private final String createdBy;
    private final boolean useNativeFilter;
    private final List<ParquetAdapterFilter<PolyValue>> aggregateReaderFilters;
    private final ParquetPrimitivePredicate primitivePredicate;
    private final int groupFieldCount;
    private final AggregateCallDescriptor[] aggregateCalls;


    public ParquetGroupedAggregateReader( Source source, AtomicBoolean cancelFlag, MessageType projectionSchema, int groupFieldCount, AggregateCallDescriptor[] aggregateCalls, List<ParquetAdapterFilter<PolyValue>> aggregateReaderFilters ) {
        this.cancelFlag = cancelFlag == null ? new AtomicBoolean( false ) : cancelFlag;
        this.groupFieldCount = groupFieldCount;
        this.aggregateCalls = aggregateCalls;
        this.projectionSchema = projectionSchema;
        this.aggregateReaderFilters = aggregateReaderFilters == null ? List.of() : List.copyOf( aggregateReaderFilters );
        this.primitivePredicate = ParquetPrimitivePredicate.compile( projectionSchema, this.aggregateReaderFilters );

        try {
            URI uri = source.isFile() ? source.file().toURI() : source.url().toURI();
            Path path = new Path( uri );
            Configuration conf = HadoopConfigurationFactory.create( this.getClass().getClassLoader() );
            var recordFilter = ParquetNativeFilterBuilder.build( projectionSchema, this.aggregateReaderFilters );
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
            this.createdBy = fileReader.getFileMetaData().getCreatedBy();
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Unable to open parquet file: " + source.path(), e );
        }
    }


    /**
     * Validates if a reader can support the projected schema. The main criteria is that all fields must be primitive and non-repeated.
     *
     * @param projectionSchema a schema to validate.
     * @return true if the projected schema is supported and false otherwise.
     */
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


    /**
     * Reads all rows from the source file and aggregates them.
     *
     * @return an aggregated map.
     */
    public Map<GroupKey, AggregateGroupState> readAll() {
        Map<GroupKey, AggregateGroupState> aggregates = new LinkedHashMap<>();
        try {
            PageReadStore pages;
            while ( !cancelFlag.get() && (pages = nextRowGroup()) != null ) {
                try {
                    readPage( pages, aggregates );
                } finally {
                    pages.close();
                }
            }
            return aggregates;
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error while reading parquet group aggregate values", e );
        }
    }


    @Override
    public void close() throws IOException {
        fileReader.close();
    }


    /**
     * Advances to the next row group if available.
     *
     * @return a {@link PageReadStore} representing next row group or null if the end of the file has been reached.
     * @throws IOException an IO reading error.
     */
    private PageReadStore nextRowGroup() throws IOException {
        return useNativeFilter
                ? fileReader.readNextFilteredRowGroup()
                : fileReader.readNextRowGroup();
    }


    /**
     * Reads a page (row group). Two ways of reading and aggregating are supported by this method:
     * 1. Only COUNT(*) with no group by fields - the fastest path.
     * 2. A fallback to a generic aggregate reader with filters support.
     *
     * @param pages a page.
     * @param aggregates a result set.
     */
    private void readPage( PageReadStore pages, Map<GroupKey, AggregateGroupState> aggregates ) {
        ColumnReadStore store = new ColumnReadStoreImpl( pages, new NoOpColumnConverter( projectionSchema ), projectionSchema, createdBy );
        ColumnDescriptor[] descriptors = projectionSchema.getColumns().toArray( ColumnDescriptor[]::new );

        if ( isNoGroupCountStar() ) {
            ParquetCountAggregatePageReader pageReader = new ParquetCountAggregatePageReader( pages, store, descriptors, primitivePredicate );
            pageReader.read( aggregates, aggregateCalls, aggregateReaderFilters, cancelFlag );
            return;
        }

        ParquetGroupedAggregatePageReader pageReader = new ParquetGroupedAggregatePageReader( pages, store, descriptors, primitivePredicate );
        pageReader.read( aggregates, aggregateCalls, aggregateReaderFilters, groupFieldCount, cancelFlag );
    }


    /**
     * Checks if there is no group by fields and only COUNT(*) aggregation function.
     *
     * @return true if the condition is satisfied and false otherwise.
     */
    private boolean isNoGroupCountStar() {
        return groupFieldCount == 0 && supportsCountStarOnly();
    }


    /**
     * Checks if there is only COUNT(*) aggregation function.
     *
     * @return true if only COUNT(*) aggregation function is provided and false otherwise.
     */
    private boolean supportsCountStarOnly() {
        for ( AggregateCallDescriptor aggregateCall : aggregateCalls ) {
            if ( aggregateCall.kind() != AggregateCallDescriptor.Kind.COUNT || aggregateCall.argumentIndex() != AggregateCallDescriptor.NO_ARGUMENT ) {
                return false;
            }
        }
        return true;
    }


}

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

package org.polypheny.db.adapter.parquet.document.execution;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.execution.aggregate.ParquetAggregateSource;
import org.polypheny.db.adapter.parquet.shared.execution.aggregate.ParquetDataAggregateExecutor;
import org.polypheny.db.adapter.parquet.shared.execution.aggregate.ParquetMetadataAggregateExecutor;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Document entry point for data aggregate execution.
 */
public class ParquetDocAggregateExecutor {

    private final ParquetAggregateSource aggregateSource;


    public ParquetDocAggregateExecutor( List<ParquetSourceFile> sourceFiles, List<ExportedColumn> columns, Map<String, List<String>> columnPaths ) {
        this.aggregateSource = new DocumentAggregateSource( sourceFiles, columns, columnPaths );
    }


    /**
     * Creates a data enumerator that should go though the parquet file.
     *
     * @param dataContext a Polypheny data context.
     * @param fields an array of field indexes.
     * @param filters a list of filters to be applied.
     * @param groupFields an array of group field indexes.
     * @param aggregateKinds an array of aggregate functions.
     * @param aggregateArgs an array of aggregate functions arguments indexes.
     * @return an enumerator
     */
    public Enumerable<PolyValue[]> createDataEnumerator( DataContext dataContext, int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        return new ParquetDataAggregateExecutor( aggregateSource, null )
                .createEnumerator( dataContext, fields, filters, groupFields, aggregateKinds, aggregateArgs );
    }


    /**
     * Creates a metadata enumerator that reads only parquet file statistics, no data is being read.
     *
     * @param dataContext a Polypheny data context.
     * @param fields an array of field indexes.
     * @param filters a list of filters to be applied.
     * @param groupFields an array of group field indexes.
     * @param aggregateKinds an array of aggregate functions.
     * @param aggregateArgs an array of aggregate functions arguments indexes.
     * @return an enumerator.
     */
    public Enumerable<PolyValue[]> createMetadataEnumerator( DataContext dataContext, int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        return new ParquetMetadataAggregateExecutor( aggregateSource )
                .createEnumerator( dataContext, fields, filters, groupFields, aggregateKinds, aggregateArgs );
    }


    /**
     * Checks whether all provided aggregate functions are supported by data aggregates. Currently only COUNT, SUM, MIN and MAX for the numeric types are supported.
     *
     * @param fields an array of field indexes.
     * @param groupSet a set of group by fields.
     * @param aggregateCalls a list of aggregation functions to validate.
     * @return {@code true} if all provided aggregation functions can be applied and {@code false} otherwise.
     */
    public boolean supportsDataAggregate( int[] fields, ImmutableBitSet groupSet, List<AggregateCall> aggregateCalls ) {
        return new ParquetDataAggregateExecutor( aggregateSource, null )
                .supportsDataAggregate( fields, groupSet, aggregateCalls );
    }


    /**
     * Checks whether all provided aggregate functions are supported by metadata aggregates. Currently only COUNT, SUM, MIN and MAX for the numeric types are supported.
     *
     * @param fields an array of field indexes.
     * @param filters a list of filters.
     * @param groupSet a set of group by fields.
     * @param aggregateCalls a list of aggregation functions to validate.
     * @return {@code true} if all provided aggregation functions can be applied and {@code false} otherwise.
     */
    public boolean supportsMetadataAggregate( int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, ImmutableBitSet groupSet, List<AggregateCall> aggregateCalls ) {
        return new ParquetMetadataAggregateExecutor( aggregateSource )
                .supportsMetadataAggregate( fields, filters, groupSet, aggregateCalls );
    }


    /**
     * Checks whether all provided aggregate functions are supported. Currently only COUNT, SUM, MIN and MAX for the numeric types are supported.
     *
     * @param fields an array of field indexes.
     * @param filters a list of filters.
     * @param groupFields an array of group field indexes.
     * @param aggregateKinds an array of aggregate functions.
     * @param aggregateArgs an array of aggregate functions argument indexes.
     * @return {@code true} if all provided aggregation functions can be applied and {@code false} otherwise.
     */
    public boolean supportsAggregate( int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        return new ParquetDataAggregateExecutor( aggregateSource, null )
                .supportsDataAggregate( fields, filters, groupFields, aggregateKinds, aggregateArgs );
    }


    /**
     * An implementation of document aggregate source on top of a Parquet file.
     */
    private static class DocumentAggregateSource implements ParquetAggregateSource {

        private final List<ParquetSourceFile> sourceFiles;
        private final List<ExportedColumn> columns;
        private final Map<Integer, ParquetColumnBinding> bindingsByField;
        private final ParquetSchemaReader schemaReader;


        private DocumentAggregateSource( List<ParquetSourceFile> sourceFiles, List<ExportedColumn> columns, Map<String, List<String>> columnPaths ) {
            this.sourceFiles = List.copyOf( sourceFiles );
            this.columns = List.copyOf( columns );
            this.bindingsByField = bindingsByField( this.sourceFiles, this.columns, columnPaths );
            this.schemaReader = new ParquetSchemaReader( this.sourceFiles.stream().map( ParquetSourceFile::asSource ).toList() );
        }


        private static Map<Integer, ParquetColumnBinding> bindingsByField( List<ParquetSourceFile> sourceFiles, List<ExportedColumn> columns, Map<String, List<String>> columnPaths ) {
            Map<Integer, ParquetColumnBinding> bindings = new LinkedHashMap<>();
            for ( ExportedColumn column : columns ) {
                List<String> sourcePath = columnPaths.get( column.name() );
                ParquetColumnRole role = sourcePath == null && isPartitionColumn( sourceFiles, column.name() )
                        ? ParquetColumnRole.PARTITION
                        : ParquetColumnRole.DATA;
                bindings.put(
                        column.physicalPosition(),
                        new ParquetColumnBinding( column.physicalPosition(), column.name(), role, sourcePath == null ? List.of() : sourcePath ) );
            }
            return bindings;
        }


        private static boolean isPartitionColumn( List<ParquetSourceFile> sourceFiles, String columnName ) {
            return sourceFiles.stream().anyMatch( sourceFile -> sourceFile.partitionValues().containsKey( columnName ) );
        }


        @Override
        public List<ParquetSourceFile> sourceFiles() {
            return sourceFiles;
        }


        @Override
        public ParquetSchemaReader schemaReader() {
            return schemaReader;
        }


        @Override
        public int fieldCount() {
            return columns.size();
        }


        @Override
        public PolyType fieldType( int field ) {
            ExportedColumn column = column( field );
            return column == null ? null : column.type();
        }


        @Override
        public ParquetColumnBinding binding( int field ) {
            return bindingsByField.get( field );
        }


        private ExportedColumn column( int field ) {
            return columns.stream().filter( column -> column.physicalPosition() == field ).findFirst().orElse( null );
        }

    }

}

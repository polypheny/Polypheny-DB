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

package org.polypheny.db.adapter.parquet.relational.schema;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import lombok.Getter;
import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelExecutorsFactory;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelMetadataAggregateExecutor;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelNestedJoinExecutor;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelProjectExecutor;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelDataAggregateExecutor;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetConvention;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelScan;
import org.polypheny.db.adapter.parquet.relational.planning.PhysicalScan;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.statistics.ParquetTableStatisticsReader;
import org.polypheny.db.adapter.statistics.AdapterStatisticsProvider;
import org.polypheny.db.adapter.statistics.ProvidedColumnStatistics;
import org.polypheny.db.adapter.statistics.ProvidedEntityStatistics;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.ImmutableBitSet;

/**
 * Physical table wrapper for the relational model.
 * Exposes the Parquet-backed table to Polypheny and ties the planner,
 * scanner, and adapter metadata together.
 */
public class ParquetRelTable extends PhysicalTable implements TranslatableEntity, AdapterStatisticsProvider {

    private final int[] fieldIndexes;
    @Getter
    private final ParquetTableBinding binding;
    private final ParquetTableStatisticsReader statisticsReader;
    private final ParquetRelExecutorsFactory executorsFactory;


    /**
     * Creates a Parquet table wrapper from a physical table definition and source binding.
     */
    public ParquetRelTable( long id, PhysicalTable table, ParquetTableBinding binding, AbstractParquetSource parquetSource ) {
        super(
                id,
                table.allocationId,
                table.logicalId,
                table.name,
                table.columns,
                table.namespaceId,
                table.namespaceName,
                table.uniqueFieldIds,
                table.adapterId );
        this.binding = binding;
        this.fieldIndexes = IntStream.range( 0, table.columns.size() ).toArray();
        ParquetSchemaReader schemaReader = new ParquetSchemaReader( binding.sourceFiles().stream().map( ParquetSourceFile::asSource ).toList() );
        this.statisticsReader = new ParquetTableStatisticsReader( schemaReader, binding );
        this.executorsFactory = new ParquetRelExecutorsFactory( this, parquetSource, fieldIndexes, schemaReader );
    }

    // Statics provider interface functions


    /**
     * Gets table statistics
     *
     * @param logicalEntityId - logical table id
     * @return statistics from parquet file
     */
    @Override
    public Optional<ProvidedEntityStatistics> getEntityStatistics( long logicalEntityId ) {
        if ( logicalId != logicalEntityId ) {
            return Optional.empty();
        }
        return statisticsReader.getEntityStatistics( isNestedTable() );
    }


    /**
     * Gets column statistics (range values for example)
     *
     * @param column - logical column
     * @param uniqueValueLimit - limit
     * @return column statistics calculated from parquet file
     */
    @Override
    public Optional<ProvidedColumnStatistics> getColumnStatistics( LogicalColumn column, int uniqueValueLimit ) {
        if ( logicalId != column.tableId ) {
            return Optional.empty();
        }
        return statisticsReader.getColumnStatistics( column, uniqueValueLimit );
    }

    // endregion


    /**
     * Returns {@link AlgNode} as part of TranslatableEntity.
     *
     * @param cluster cluster
     * @param traitSet trial set.
     * @return {@link AlgNode}
     */
    @Override
    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        ParquetConvention.INSTANCE.register( cluster.getPlanner() );
        return new ParquetRelScan( cluster, this, fieldIndexes );
    }


    /**
     * This method is called from the {@link ParquetRelScan} via reflection.
     *
     * @param dataContext data context
     * @param fields a list of fields to return.
     * @return enumerable.
     */
    public Enumerable<PolyValue[]> project( final DataContext dataContext, final int[] fields ) {
        return executorsFactory.getExecutor( ParquetRelProjectExecutor.class ).createEnumerator( dataContext, fields, List.of() );
    }


    /**
     * This method is called from the {@link ParquetRelScan} via reflection.
     *
     * @param dataContext data context
     * @param fields a list of fields to return.
     * @param filters filters to push down.
     * @return enumerable.
     */
    @SuppressWarnings("unused")
    public Enumerable<PolyValue[]> project( final DataContext dataContext, final int[] fields, final List<ParquetAdapterFilter<PolyValue>> filters ) {
        return executorsFactory.getExecutor( ParquetRelProjectExecutor.class ).createEnumerator( dataContext, fields, filters );
    }


    /**
     * Executes supported aggregates exactly from Parquet file metadata.
     */
    @SuppressWarnings("unused")
    public Enumerable<PolyValue[]> metadataAggregate( DataContext dataContext, int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        return executorsFactory.getExecutor( ParquetRelMetadataAggregateExecutor.class ).createEnumerator( dataContext, fields, filters, groupFields, aggregateKinds, aggregateArgs );
    }


    /**
     * Checks whether filters, groups and aggregate functions can be executed on metadata only.
     *
     * @param fields an array of physical field indexes of columns used in a group by statement.
     * @param filters a list of filters to be validated.
     * @param groupSet a group by set.
     * @param aggregateCalls a list of aggregate calls (aggregate functions)
     * @return {@code true} filters, groups and aggregate functions can be executed on metadata only and {@code false} otherwise.
     */
    public boolean supportsMetadataAggregate( int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, ImmutableBitSet groupSet, List<AggregateCall> aggregateCalls ) {
        return executorsFactory.getExecutor( ParquetRelMetadataAggregateExecutor.class ).supportsMetadataAggregate( fields, filters, groupSet, aggregateCalls );
    }


    /**
     * Executes supported aggregates by scanning rows inside the adapter.
     */
    @SuppressWarnings("unused")
    public Enumerable<PolyValue[]> dataAggregate( DataContext dataContext, int[] fields, List<ParquetAdapterFilter<PolyValue>> filters, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs ) {
        return executorsFactory.getExecutor( ParquetRelDataAggregateExecutor.class ).createEnumerator( dataContext, fields, filters, groupFields, aggregateKinds, aggregateArgs );
    }


    /**
     * Checks whether all provided aggregate functions are supported. Currently only COUNT, SUM, MIN and MAX for the numeric types are supported.
     *
     * @param fields an array of field indexes.
     * @param groupSet a set of group by fields.
     * @param aggregateCalls a list of aggregation functions to validate.
     * @return {@code true} if all provided aggregation functions can be applied and {@code false} otherwise.
     */
    public boolean supportsDataAggregate( int[] fields, ImmutableBitSet groupSet, List<AggregateCall> aggregateCalls ) {
        return executorsFactory.getExecutor( ParquetRelDataAggregateExecutor.class ).supportsDataAggregate( fields, groupSet, aggregateCalls );
    }


    /**
     * Executes a supported parent/child join inside the Parquet adapter.
     */
    @SuppressWarnings("unused")
    public Enumerable<PolyValue[]> nestedJoin( DataContext dataContext, PhysicalScan leftScan, PhysicalScan rightScan, boolean leftIsParent, boolean emitUnmatchedParents, List<ParquetAdapterFilter<PolyValue>> filters ) {
        return executorsFactory.getExecutor( ParquetRelNestedJoinExecutor.class ).createEnumerator( dataContext, leftScan, rightScan, leftIsParent, emitUnmatchedParents, filters );
    }


    public int columnIndexByRole( ParquetColumnRole role ) {
        for ( int i = 0; i < columns.size(); i++ ) {
            ParquetColumnBinding columnBinding = binding.getColumnBinding( columns.get( i ).id );
            if ( columnBinding != null && columnBinding.role() == role ) {
                return i;
            }
        }
        return -1;
    }


    public String getSourceUrl() {
        return binding.sourceFiles().stream().map( ParquetSourceFile::fileUrl ).sorted().toList().toString();
    }


    public int getFieldCount() {
        return columns.size();
    }


    private boolean isNestedTable() {
        return binding.parentTableName() != null;
    }

}


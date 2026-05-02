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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetNestedNonRepeatedRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetNestedRepeatedRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelFilterTranslator;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelScan;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.statistics.ParquetStatisticsReader;
import org.polypheny.db.adapter.statistics.ProvidedColumnStatistics;
import org.polypheny.db.adapter.statistics.ProvidedEntityStatistics;
import org.polypheny.db.adapter.statistics.AdapterStatisticsProvider;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.types.FilterableEntity;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

/**
 * Physical table wrapper for the relational model.
 * Exposes the Parquet-backed table to Polypheny and ties the planner,
 * scanner, and adapter metadata together.
 */
public class ParquetRelTable extends PhysicalTable implements FilterableEntity, ScannableEntity, TranslatableEntity, AdapterStatisticsProvider {

    private final Source source;
    private final int[] fieldIndexes;
    private final AbstractParquetSource parquetSource;
    private final ParquetRelFilterTranslator filterTranslator;
    private final ParquetTableBinding binding;
    private final List<PolyType> fieldTypes;
    private final ParquetSchemaReader schemaReader;
    private final ParquetStatisticsReader statisticsReader;


    /**
     * Creates a Parquet table wrapper from a physical table definition and source binding.
     */
    public ParquetRelTable( long id, Source source, PhysicalTable table, ParquetTableBinding binding, AbstractParquetSource parquetSource ) {
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
        this.source = source;
        this.binding = binding;
        this.fieldIndexes = IntStream.range( 0, table.columns.size() ).toArray();
        this.fieldTypes = columns.stream().map( c -> c.type ).toList();
        this.schemaReader = new ParquetSchemaReader( source );
        this.statisticsReader = new ParquetStatisticsReader( schemaReader, binding );
        this.parquetSource = parquetSource;
        this.filterTranslator = new ParquetRelFilterTranslator();
    }

    // Statics provider interface functions


    /**
     * Gets table statistics
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
     * Returns enumerable for FilterableEntity.
     *
     * @param dataContext data context
     * @param polyFilters polyFilters to push down.
     * @return enumerable.
     */
    @Override
    public Enumerable<PolyValue[]> scan( DataContext dataContext, List<RexNode> polyFilters ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final List<ParquetAdapterFilter> parquetAdapterFilters = new ArrayList<>();
        polyFilters.removeIf( polyFilter -> {
            var parquetFilter = filterTranslator.translate( fieldTypes, polyFilter );
            if ( parquetFilter != null ) {
                return parquetAdapterFilters.add( parquetFilter );
            }
            return false;
        } );

        // check for dynamic filters
        final List<ParquetAdapterFilter> resolvedFilters = resolveFilters( dataContext, parquetAdapterFilters );

        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                if ( isNestedTable() ) {
                    return new ParquetNestedRepeatedRelEnumerator( source, cancelFlag, binding, selectedBindings( fieldIndexes ), resolvedFilters );
                }
                if ( needsBindingScan( fieldIndexes ) ) {
                    return new ParquetNestedNonRepeatedRelEnumerator( source, cancelFlag, selectedBindings( fieldIndexes ), resolvedFilters );
                }
                return new ParquetRelEnumerator( source, cancelFlag, fieldIndexes, resolvedFilters );
            }
        };
    }


    /**
     * Returns enumerable for ScannableEntity.
     *
     * @param dataContext data context
     * @return enumerable.
     */
    @Override
    public Enumerable<PolyValue[]> scan( DataContext dataContext ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        // create parquet enumerator
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                if ( isNestedTable() ) {
                    return new ParquetNestedRepeatedRelEnumerator( source, cancelFlag, binding, selectedBindings( fieldIndexes ) );
                }
                if ( needsBindingScan( fieldIndexes ) ) {
                    return new ParquetNestedNonRepeatedRelEnumerator( source, cancelFlag, selectedBindings( fieldIndexes ) );
                }
                return new ParquetRelEnumerator( source, cancelFlag, fieldIndexes );
            }
        };
    }


    /**
     * Returns {@link AlgNode} as part of TranslatableEntity.
     *
     * @param cluster cluster
     * @param traitSet trial set.
     * @return {@link AlgNode}
     */
    @Override
    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
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
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                if ( isNestedTable() ) {
                    return new ParquetNestedRepeatedRelEnumerator( source, cancelFlag, binding, selectedBindings( fields ) );
                }
                if ( needsBindingScan( fields ) ) {
                    return new ParquetNestedNonRepeatedRelEnumerator( source, cancelFlag, selectedBindings( fields ) );
                }
                return new ParquetRelEnumerator( source, cancelFlag, fields );
            }
        };
    }


    /**
     * Support parametrized queries
     *
     * @param dataContext context
     * @param filters filters
     * @return list of parquet filters
     */
    private List<ParquetAdapterFilter> resolveFilters( DataContext dataContext, List<ParquetAdapterFilter> filters ) {
        List<ParquetAdapterFilter> resolved = new ArrayList<>( filters.size() );
        for ( ParquetAdapterFilter filter : filters ) {
            resolved.add( resolveFilter( dataContext, filter ) );
        }
        return resolved;
    }


    /**
     * Takes a ParquetAdapterFilter produced by the translator and turns it into a filter that is ready for execution against the actual Parquet file
     * Resolve dynamic parameters from dataContext
     * Use binding path elements for nested fields
     * For logical filter - recursive call
     * @param dataContext DataContext
     * @param filter Adapter level filter
     * @return Adapter level filter
     */
    private ParquetAdapterFilter resolveFilter( DataContext dataContext, ParquetAdapterFilter filter ) {
        if ( filter.isLogical() ) {
            return ParquetAdapterFilter.logical( filter.operator(), filter.operands().stream()
                    .map( operand -> resolveFilter( dataContext, operand ) )
                    .toList() );
        }

        PolyValue value = filter.dynamicParamIndex() == null
                ? filter.polyValue()
                : dataContext.getParameterValue( filter.dynamicParamIndex() );

        ParquetColumnBinding columnBinding = Objects.requireNonNull( binding.getColumnBinding( columns.get( filter.columnIndex() ).id ), "Missing parquet column binding" );
        return new ParquetAdapterFilter( filter.columnIndex(), columnBinding.sourcePathElements(), filter.operator(), value );
    }


    private boolean isNestedTable() {
        return binding.parentTableName() != null;
    }


    private List<ParquetColumnBinding> selectedBindings( int[] fields ) {
        List<ParquetColumnBinding> selected = new ArrayList<>( fields.length );
        for ( int field : fields ) {
            selected.add( Objects.requireNonNull( binding.getColumnBinding( columns.get( field ).id ), "Missing parquet column binding" ) );
        }
        return selected;
    }


    private boolean needsBindingScan( int[] fields ) {
        for ( int field : fields ) {
            ParquetColumnBinding columnBinding = binding.getColumnBinding( columns.get( field ).id );
            if ( columnBinding == null || columnBinding.sourcePathElements().isEmpty() ) {
                return true;
            }
            if ( columnBinding.sourcePathElements().size() > 1 ) {
                return true;
            }
            var schema = schemaReader.getSchema();
            if ( field >= schema.getFieldCount() || !schema.getType( field ).getName().equals( columnBinding.sourcePathElements().get( 0 ) ) ) {
                return true;
            }
            if ( !schema.getType( field ).isPrimitive() ) {
                return true;
            }
        }
        return false;
    }

}

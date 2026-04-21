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

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetBindingRelEnumerator;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetNestedRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelFilterTranslator;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelScan;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.algebra.AlgNode;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import org.apache.parquet.schema.MessageType;

/**
 * Physical table wrapper for the relational model.
 * Exposes the Parquet-backed table to Polypheny and ties the planner,
 * scanner, and adapter metadata together.
 */
public class ParquetRelTable extends PhysicalTable implements FilterableEntity, ScannableEntity, TranslatableEntity {

    protected final Source source;
    protected final int[] fieldIndexes;
    private final List<PolyType> fieldTypes;
    private final MessageType parquetSchema;
    protected final AbstractParquetSource parquetSource;
    protected final ParquetRelFilterTranslator filterTranslator;
    protected final ParquetTableBinding binding;


    /**
     * Creates a Parquet table wrapper from a physical table definition and source binding.
     */
    public ParquetRelTable(long id, Source source, PhysicalTable table, ParquetTableBinding binding, AbstractParquetSource parquetSource ) {
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
        this.parquetSchema = ParquetSourceReader.readSchema( source );
        this.parquetSource = parquetSource;
        this.filterTranslator = new ParquetRelFilterTranslator();
    }


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
        final List<ParquetAdapterFilter> resolvedFilters = resolveDynamicFilters( dataContext, parquetAdapterFilters );

        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                if ( isNestedTable() ) {
                    return new ParquetNestedRelEnumerator( source, cancelFlag, binding, selectedBindings( fieldIndexes ) );
                }
                if ( needsBindingScan( fieldIndexes ) ) {
                    return new ParquetBindingRelEnumerator( source, cancelFlag, selectedBindings( fieldIndexes ) );
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
                    return new ParquetNestedRelEnumerator( source, cancelFlag, binding, selectedBindings( fieldIndexes ) );
                }
                if ( needsBindingScan( fieldIndexes ) ) {
                    return new ParquetBindingRelEnumerator( source, cancelFlag, selectedBindings( fieldIndexes ) );
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
                    return new ParquetNestedRelEnumerator( source, cancelFlag, binding, selectedBindings( fields ) );
                }
                if ( needsBindingScan( fields ) ) {
                    return new ParquetBindingRelEnumerator( source, cancelFlag, selectedBindings( fields ) );
                }
                return new ParquetRelEnumerator( source, cancelFlag, fields );
            }
        };
    }


    /**
     * Support parametrized queries
     * @param dataContext context
     * @param filters filters
     * @return list of parquet filters
     */
    private List<ParquetAdapterFilter> resolveDynamicFilters( DataContext dataContext, List<ParquetAdapterFilter> filters ) {
        List<ParquetAdapterFilter> resolved = new ArrayList<>( filters.size() );
        for ( ParquetAdapterFilter filter : filters ) {
            if ( filter.dynamicParamIndex() == null ) {
                // regular filter
                resolved.add( filter );
                continue;
            }
            // get filter value by index
            PolyValue value = dataContext.getParameterValue( filter.dynamicParamIndex() );
            resolved.add( new ParquetAdapterFilter( filter.columnIndex(), filter.operator(), value ) );
        }
        return resolved;
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
            if ( field >= parquetSchema.getFieldCount() || !parquetSchema.getType( field ).getName().equals( columnBinding.sourcePathElements().get( 0 ) ) ) {
                return true;
            }
        }
        return false;
    }

}



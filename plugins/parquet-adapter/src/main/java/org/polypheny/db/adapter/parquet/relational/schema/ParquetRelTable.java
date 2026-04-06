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
import org.polypheny.db.adapter.parquet.shared.AbstractParquetSource;
import org.polypheny.db.adapter.parquet.shared.model.AdapterFilter;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelFilterTranslator;
import org.polypheny.db.adapter.parquet.relational.planning.ParquetRelScan;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

/**
 * Base class for Parquet physical tables.
 */
public class ParquetRelTable extends PhysicalTable implements FilterableEntity, ScannableEntity, TranslatableEntity {

    protected final Source source;
    protected final int[] fieldIndexes;
    private final List<PolyType> fieldTypes;
    protected final AbstractParquetSource parquetSource;
    protected final ParquetRelFilterTranslator filterTranslator;


    /**
     * Creates a Parquet table wrapper from a physical table definition.
     */
    public ParquetRelTable(long id, Source source, PhysicalTable table, AbstractParquetSource parquetSource ) {
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
        this.fieldIndexes = IntStream.range( 0, table.columns.size() ).toArray();
        this.fieldTypes = columns.stream().map( c -> c.type ).toList();
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
        final List<AdapterFilter> adapterFilters = new ArrayList<>();
        polyFilters.removeIf( polyFilter -> {
            var adapterFilter = filterTranslator.translate( fieldTypes, polyFilter );
            if ( adapterFilter != null ) {
                return adapterFilters.add( adapterFilter );
            }
            return false;
        } );

        // check for dynamic filters
        final List<AdapterFilter> resolvedFilters = resolveDynamicFilters( dataContext, adapterFilters );

        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
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
                return new ParquetRelEnumerator( source, cancelFlag, fields );
            }
        };
    }


    /**
     * Support parametrized queries
     * @param dataContext context
     * @param filters filters
     * @return list of adapter filters
     */
    private List<AdapterFilter> resolveDynamicFilters( DataContext dataContext, List<AdapterFilter> filters ) {
        List<AdapterFilter> resolved = new ArrayList<>( filters.size() );
        for ( AdapterFilter filter : filters ) {
            if ( filter.dynamicParamIndex() == null ) {
                // regular filter
                resolved.add( filter );
                continue;
            }
            // get filter value by index
            PolyValue value = dataContext.getParameterValue( filter.dynamicParamIndex() );
            resolved.add( new AdapterFilter( filter.columnIndex(), filter.operator(), value ) );
        }
        return resolved;
    }

}



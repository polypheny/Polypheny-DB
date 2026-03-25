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

package org.polypheny.db.adapter.parquet.schema;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.ParquetSource;
import org.polypheny.db.adapter.parquet.execution.ParquetEnumerator;
import org.polypheny.db.adapter.parquet.model.FilterInfo;
import org.polypheny.db.adapter.parquet.planning.ParquetScan;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
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

/**
 * Base class for Parquet physical tables.
 */
public class ParquetTable extends PhysicalTable implements FilterableEntity, ScannableEntity, TranslatableEntity {

    protected final Source source;
    protected List<PolyType> fieldTypes;
    protected final int[] fields;
    protected final ParquetSource parquetSource;


    /**
     * Creates a Parquet table wrapper from a physical table definition.
     */
    ParquetTable( long id, Source source, PhysicalTable table, List<PolyType> fieldTypes, int[] fields, ParquetSource parquetSource ) {
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
        this.fieldTypes = fieldTypes;
        this.fields = fields;
        this.parquetSource = parquetSource;
    }


    /**
     * Returns enumerable for FilterableEntity.
     *
     * @param dataContext data context
     * @param filters filters to push down.
     * @return enumerable.
     */
    @Override
    public Enumerable<PolyValue[]> scan( DataContext dataContext, List<RexNode> filters ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final List<FilterInfo> pushedFilters = new ArrayList<>();
        filters.removeIf( filter -> addFilter( filter, pushedFilters ) );

        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new ParquetEnumerator( source, cancelFlag, fields, pushedFilters );
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
                return new ParquetEnumerator( source, cancelFlag, fields );
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
        return new ParquetScan( cluster, this, fields );
    }


    /**
     * This method is called from the {@link ParquetScan} via reflection.
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
                return new ParquetEnumerator( source, cancelFlag, fields );
            }
        };
    }


    /**
     * Translates a Rex filter into adapter filter form when possible.
     */
    private boolean addFilter( RexNode filter, List<FilterInfo> pushedFilters ) {
        if ( !isSupportedOperator( filter.getKind() ) ) {
            return false;
        }

        RexCall call = (RexCall) filter;
        RexNode left = call.getOperands().get( 0 );
        if ( left.isA( Kind.CAST ) ) {
            left = ((RexCall) left).operands.get( 0 );
        }
        RexNode right = call.getOperands().get( 1 );

        if ( !(left instanceof RexIndexRef) || !(right instanceof RexLiteral literal) ) {
            return false;
        }

        if ( literal.getValue() == null ) {
            return false;
        }

        int index = ((RexIndexRef) left).getIndex();
        if ( index < 0 || index >= columns.size() ) {
            return false;
        }

        if ( !isPushdownSupported( index, filter.getKind(), literal ) ) {
            return false;
        }

        pushedFilters.add( new FilterInfo( index, filter.getKind(), literal.getValue() ) );
        return true;
    }


    /**
     * Checks whether the operator can be handled by the reader.
     */
    private boolean isSupportedOperator( Kind kind ) {
        return kind == Kind.EQUALS
                || kind == Kind.NOT_EQUALS
                || kind == Kind.GREATER_THAN
                || kind == Kind.GREATER_THAN_OR_EQUAL
                || kind == Kind.LESS_THAN
                || kind == Kind.LESS_THAN_OR_EQUAL;
    }


    private boolean isPushdownSupported( int index, Kind kind, RexLiteral literal ) {
        PolyType type = fieldTypes.get( index );
        return switch ( type ) {
            case BOOLEAN, VARCHAR, CHAR, TEXT -> kind == Kind.EQUALS || kind == Kind.NOT_EQUALS;
            case INTEGER, BIGINT, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP -> true;
            default -> false;
        } && literal.getValue() != null;
    }


}



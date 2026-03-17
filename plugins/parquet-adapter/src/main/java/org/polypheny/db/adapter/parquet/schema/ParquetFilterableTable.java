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
import org.polypheny.db.adapter.parquet.model.ParquetFilter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.types.FilterableEntity;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Filterable table
 */
public class ParquetFilterableTable extends ParquetTable implements FilterableEntity {

    public ParquetFilterableTable( long id, Source source, PhysicalTable table, List<PolyType> fieldTypes, int[] fields, ParquetSource csvSource ) {
        super( id, source, table, fieldTypes, fields, csvSource );
    }


    /**
     * Pushes supported filters.
     */
    @Override
    public Enumerable<PolyValue[]> scan( DataContext dataContext, List<RexNode> filters ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final List<ParquetFilter> pushedFilters = new ArrayList<>();
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
     * Translates a Rex filter into adapter filter form when possible.
     */
    private boolean addFilter( RexNode filter, List<ParquetFilter> pushedFilters ) {
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

        pushedFilters.add( new ParquetFilter( index, filter.getKind(), literal.getValue().toString() ) );
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

}


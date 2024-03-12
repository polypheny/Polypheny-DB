/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.algebra.core;

import java.util.List;
import java.util.Optional;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;

public class LaxAggregateCall {

    public final String name;
    public final AggFunction function;
    private final RexNode input;


    public LaxAggregateCall( String name, AggFunction function, RexNode input ) {
        this.name = name;
        this.function = function;
        this.input = input;
    }


    public static LaxAggregateCall create( String name, AggFunction function, RexNode input ) {
        return new LaxAggregateCall( name, function, input );
    }


    public static LaxAggregateCall from( AggregateCall call, AlgNode parent ) {
        if ( parent == null ) {
            return new LaxAggregateCall( call.name, call.getAggregation(), null );
        }

        RexNode node;
        if ( call.getArgList().isEmpty() ) {
            node = new RexIndexRef( 0, parent.getTupleType().getFields().get( 0 ).getType() );
        } else {
            node = new RexIndexRef( call.getArgList().get( 0 ), parent.getTupleType().getFields().get( call.getArgList().get( 0 ) ).getType() );
        }

        return new LaxAggregateCall( call.name, call.getAggregation(), node );
    }


    public Optional<RexNode> getInput() {
        return Optional.ofNullable( input );
    }


    public AggregateCall toAggCall( AlgDataType rowType, AlgCluster cluster ) {
        int index = rowType.getFieldNames().indexOf( name );
        if ( index < 0 && getInput().isPresent() && getInput().get().unwrap( RexIndexRef.class ).isPresent() ) {
            index = getInput().get().unwrap( RexIndexRef.class ).get().getIndex();
        }

        return AggregateCall.create( function, false, false, index < 0 ? List.of() : List.of( index ), -1, AlgCollations.EMPTY, getType( cluster ), name );
    }


    public AlgDataType getType( AlgCluster cluster ) {
        AlgDataType type = getType( cluster, function );
        if ( type == null ) {
            throw new GenericRuntimeException( "Unknown aggregate function: " + function.getKind() );
        }
        return type;
    }


    public static AlgDataType getType( AlgCluster cluster, AggFunction function ) {
        return switch ( function.getKind() ) {
            case COUNT -> cluster.getTypeFactory().createPolyType( PolyType.BIGINT );
            case SUM, AVG, MIN, MAX -> cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( PolyType.DOUBLE ), true );
            default -> null;
        };
    }


    public Optional<AlgDataType> requiresCast( AlgCluster cluster ) {
        return switch ( function.getKind() ) {
            case COUNT -> Optional.empty();
            case SUM, AVG, MAX, MIN -> Optional.ofNullable( cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( PolyType.DOUBLE ), true ) );
            default -> Optional.empty();
        };
    }

}

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
import lombok.Getter;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;

public class LaxAggregateCall {

    public final String name;
    public final AggFunction function;
    private final RexNode input;
    @Getter
    private final AlgDataType resultType;
    @Getter
    private final boolean distinct;


    public LaxAggregateCall( String name, AggFunction function, RexNode input ) {
        this( name, function, input, false, null );
    }


    private LaxAggregateCall( String name, AggFunction function, RexNode input, boolean distinct, AlgDataType resultType ) {
        this.name = name;
        this.function = function;
        this.input = input;
        this.distinct = distinct;
        this.resultType = resultType;
    }


    public static LaxAggregateCall create( String name, AggFunction function, RexNode input ) {
        return new LaxAggregateCall( name, function, input );
    }


    public static LaxAggregateCall create( String name, AggFunction function, RexNode input, boolean distinct ) {
        return new LaxAggregateCall( name, function, input, distinct, null );
    }


    public static LaxAggregateCall from( AggregateCall call, AlgNode parent ) {
        AlgDataType resultType = switch ( call.getAggregation().getKind() ) {
            case MIN, MAX, SUM, AVG, COLLECT -> call.getType();
            default -> null;
        };
        if ( parent == null ) {
            return new LaxAggregateCall( call.name, call.getAggregation(), null, call.isDistinct(), resultType );
        }

        RexNode node;
        if ( call.getArgList().isEmpty() ) {
            node = new RexIndexRef( 0, parent.getTupleType().getFields().get( 0 ).getType() );
        } else {
            node = new RexIndexRef( call.getArgList().get( 0 ), parent.getTupleType().getFields().get( call.getArgList().get( 0 ) ).getType() );
        }

        return new LaxAggregateCall( call.name, call.getAggregation(), node, call.isDistinct(), resultType );
    }


    public Optional<RexNode> getInput() {
        return Optional.ofNullable( input );
    }


    public AggregateCall toAggCall( AlgDataType rowType, AlgCluster cluster ) {
        int index = rowType.getFieldNames().indexOf( name );
        if ( index < 0 && getInput().isPresent() && getInput().get().unwrap( RexIndexRef.class ).isPresent() ) {
            index = getInput().get().unwrap( RexIndexRef.class ).get().getIndex();
        }

        return toAggCall( index < 0 ? List.of() : List.of( index ), cluster );
    }


    public AggregateCall toAggCall( List<Integer> arguments, AlgCluster cluster ) {
        return AggregateCall.create( function, distinct, false, arguments, -1, AlgCollations.EMPTY, getType( cluster ), name );
    }


    public AlgDataType getType( AlgCluster cluster ) {
        AlgDataType type = resultType != null ? resultType : getType( cluster, function );
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
        if ( resultType != null ) {
            return Optional.empty();
        }
        return switch ( function.getKind() ) {
            case COUNT -> Optional.empty();
            case SUM, AVG, MAX, MIN -> Optional.ofNullable( cluster.getTypeFactory().createTypeWithNullability( cluster.getTypeFactory().createPolyType( PolyType.DOUBLE ), true ) );
            default -> Optional.empty();
        };
    }


    public static AlgDataType inferGraphNumericType( AlgDataTypeFactory factory, Kind kind, AlgDataType operand ) {
        if ( kind == Kind.AVG ) {
            return factory.createTypeWithNullability( factory.createPolyType( PolyType.DOUBLE ), true );
        }
        PolyType sumType = switch ( operand.getPolyType() ) {
            case TINYINT, SMALLINT, INTEGER, BIGINT, NULL -> PolyType.BIGINT;
            case REAL, FLOAT, DOUBLE -> PolyType.DOUBLE;
            case DECIMAL -> PolyType.DECIMAL;
            default -> PolyType.ANY;
        };
        return factory.createPolyType( sumType );
    }

}

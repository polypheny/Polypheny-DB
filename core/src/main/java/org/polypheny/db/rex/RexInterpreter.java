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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.rex;


import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.IntPredicate;
import org.polypheny.db.nodes.TimeUnitRange;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.temporal.DateTimeUtils;
import org.polypheny.db.util.temporal.TimeUnit;


/**
 * Evaluates {@link RexNode} expressions.
 * <p>
 * Caveats:
 * <ul>
 * <li>It uses interpretation, so it is not very efficient.</li>
 * <li>It is intended for testing, so does not cover very many functions and operators. (Feel free to contribute more!)</li>
 * <li>It is not well tested.</li>
 * </ul>
 */
public class RexInterpreter implements RexVisitor<PolyValue> {

    private static final PolyNull N = PolyNull.NULL;

    private final Map<RexNode, PolyValue> environment;


    /**
     * Creates an interpreter.
     *
     * @param environment Values of certain expressions (usually {@link RexIndexRef}s)
     */
    private RexInterpreter( Map<RexNode, PolyValue> environment ) {
        this.environment = ImmutableMap.copyOf( environment );
    }


    /**
     * Evaluates an expression in an environment.
     */
    public static PolyValue evaluate( RexNode e, Map<RexNode, PolyValue> map ) {
        return e.accept( new RexInterpreter( map ) );
    }


    private IllegalArgumentException unbound( RexNode e ) {
        return new IllegalArgumentException( "unbound: " + e );
    }


    private PolyValue getOrUnbound( RexNode e ) {
        final PolyValue comparable = environment.get( e );
        if ( comparable != null ) {
            return comparable;
        }
        throw unbound( e );
    }


    @Override
    public PolyValue visitIndexRef( RexIndexRef inputRef ) {
        return getOrUnbound( inputRef );
    }


    @Override
    public PolyValue visitLocalRef( RexLocalRef localRef ) {
        throw unbound( localRef );
    }


    @Override
    public PolyValue visitLiteral( RexLiteral literal ) {
        return Util.first( literal.getValue(), N );
    }


    @Override
    public PolyValue visitOver( RexOver over ) {
        throw unbound( over );
    }


    @Override
    public PolyValue visitCorrelVariable( RexCorrelVariable correlVariable ) {
        return getOrUnbound( correlVariable );
    }


    @Override
    public PolyValue visitDynamicParam( RexDynamicParam dynamicParam ) {
        return getOrUnbound( dynamicParam );
    }


    @Override
    public PolyValue visitRangeRef( RexRangeRef rangeRef ) {
        throw unbound( rangeRef );
    }


    @Override
    public PolyValue visitFieldAccess( RexFieldAccess fieldAccess ) {
        return getOrUnbound( fieldAccess );
    }


    @Override
    public PolyValue visitSubQuery( RexSubQuery subQuery ) {
        throw unbound( subQuery );
    }


    @Override
    public PolyValue visitTableInputRef( RexTableIndexRef fieldRef ) {
        throw unbound( fieldRef );
    }


    @Override
    public PolyValue visitPatternFieldRef( RexPatternFieldRef fieldRef ) {
        throw unbound( fieldRef );
    }


    @Override
    public PolyValue visitNameRef( RexNameRef nameRef ) {
        throw unbound( nameRef );
    }


    @Override
    public PolyValue visitElementRef( RexElementRef rexElementRef ) {
        throw unbound( rexElementRef );
    }


    @Override
    public PolyValue visitCall( RexCall call ) {
        final List<PolyValue> values = new ArrayList<>( call.operands.size() );
        for ( RexNode operand : call.operands ) {
            values.add( operand.accept( this ) );
        }
        switch ( call.getKind() ) {
            case IS_NOT_DISTINCT_FROM:
                if ( containsNull( values ) ) {
                    return PolyBoolean.of( values.get( 0 ).equals( values.get( 1 ) ) );
                }
                // falls through EQUALS
            case EQUALS:
                return compare( values, c -> c == 0 );
            case IS_DISTINCT_FROM:
                if ( containsNull( values ) ) {
                    return PolyBoolean.of( !values.get( 0 ).equals( values.get( 1 ) ) );
                }
                // falls through NOT_EQUALS
            case NOT_EQUALS:
                return compare( values, c -> c != 0 );
            case GREATER_THAN:
                return compare( values, c -> c > 0 );
            case GREATER_THAN_OR_EQUAL:
                return compare( values, c -> c >= 0 );
            case LESS_THAN:
                return compare( values, c -> c < 0 );
            case LESS_THAN_OR_EQUAL:
                return compare( values, c -> c <= 0 );
            case AND:
                return values.stream().map( Truthy::of ).min( Comparator.naturalOrder() ).orElseThrow().toPolyValue();
            case OR:
                return values.stream().map( Truthy::of ).max( Comparator.naturalOrder() ).orElseThrow().toPolyValue();
            case NOT:
                return not( values.get( 0 ) );
            case CASE:
                return case_( values );
            case IS_TRUE:
                return PolyBoolean.of( values.get( 0 ).isNotNull() && values.get( 0 ).asBoolean().value );
            case IS_NOT_TRUE:
                return PolyBoolean.of( values.get( 0 ).isNull() || !values.get( 0 ).asBoolean().value );
            case IS_NULL:
                return PolyBoolean.of( values.get( 0 ).equals( N ) );
            case IS_NOT_NULL:
                return PolyBoolean.of( !values.get( 0 ).equals( N ) );
            case IS_FALSE:
                return PolyBoolean.of( values.get( 0 ).isNull() || !values.get( 0 ).asBoolean().value );
            case IS_NOT_FALSE:
                return PolyBoolean.of( values.get( 0 ).isNotNull() && values.get( 0 ).asBoolean().value );
            case PLUS_PREFIX:
                return values.get( 0 );
            case MINUS_PREFIX:
                return containsNull( values ) ? N : values.get( 0 ).asNumber().negate();
            case PLUS:
                return containsNull( values ) ? N : values.get( 0 ).asNumber().plus( values.get( 1 ).asNumber() );
            case MINUS:
                return containsNull( values ) ? N : values.get( 0 ).asNumber().subtract( values.get( 1 ).asNumber() );
            case TIMES:
                return containsNull( values ) ? N : values.get( 0 ).asNumber().multiply( values.get( 1 ).asNumber() );
            case DIVIDE:
                return containsNull( values ) ? N : values.get( 0 ).asNumber().divide( values.get( 1 ).asNumber() );
            case CAST:
                return cast( call, values );
            case COALESCE:
                return coalesce( call, values );
            case CEIL:
            case FLOOR:
                return ceil( call, values );
            case EXTRACT:
                return extract( call, values );
            default:
                throw unbound( call );
        }
    }


    private PolyValue extract( RexCall call, List<PolyValue> values ) {
        final PolyValue v = values.get( 1 );
        if ( v == N ) {
            return N;
        }
        final TimeUnitRange timeUnitRange = values.get( 0 ).asSymbol().asEnum( TimeUnitRange.class );
        final long v2;
        if ( v.isTimestamp() ) {
            // TIMESTAMP
            v2 = (v.asTimestamp().getMillisSinceEpoch() / TimeUnit.DAY.multiplier.longValue());
        } else {
            // DATE
            v2 = v.asDate().asDate().getDaysSinceEpoch();
        }
        return PolyTimestamp.of( DateTimeUtils.unixDateExtract( timeUnitRange, v2 ) );
    }


    private PolyValue coalesce( RexCall call, List<PolyValue> values ) {
        for ( PolyValue value : values ) {
            if ( value != N ) {
                return value;
            }
        }
        return N;
    }


    private PolyValue ceil( RexCall call, List<PolyValue> values ) {
        if ( values.get( 0 ) == N ) {
            return N;
        }
        final Long v = values.get( 0 ).asNumber().LongValue();
        final TimeUnitRange unit = values.get( 1 ).asSymbol().asEnum( TimeUnitRange.class );
        switch ( unit ) {
            case YEAR:
            case MONTH:
                return switch ( call.getKind() ) {
                    case FLOOR -> PolyTimestamp.of( DateTimeUtils.unixTimestampFloor( unit, v ) );
                    default -> PolyTimestamp.of( DateTimeUtils.unixTimestampCeil( unit, v ) );
                };
        }
        final TimeUnitRange subUnit = subUnit( unit );
        for ( long v2 = v; ; ) {
            final int e = DateTimeUtils.unixTimestampExtract( subUnit, v2 );
            if ( e == 0 ) {
                return PolyTimestamp.of( v2 );
            }
            v2 -= unit.startUnit.multiplier.longValue();
        }
    }


    private TimeUnitRange subUnit( TimeUnitRange unit ) {
        return switch ( unit ) {
            case QUARTER -> TimeUnitRange.MONTH;
            default -> TimeUnitRange.DAY;
        };
    }


    private PolyValue cast( RexCall call, List<PolyValue> values ) {
        if ( values.get( 0 ) == N ) {
            return N;
        }
        return values.get( 0 );
    }


    private PolyValue not( PolyValue value ) {
        if ( value.isBoolean() && value.isNotNull() && value.asBoolean().value.equals( true ) ) {
            return PolyBoolean.FALSE;
        } else if ( value.isBoolean() && value.isNotNull() && value.asBoolean().value.equals( false ) ) {
            return PolyBoolean.TRUE;
        } else {
            return N;
        }
    }


    private PolyValue case_( List<PolyValue> values ) {
        final int size;
        final PolyValue elseValue;
        if ( values.size() % 2 == 0 ) {
            size = values.size();
            elseValue = N;
        } else {
            size = values.size() - 1;
            elseValue = Util.last( values );
        }
        for ( int i = 0; i < size; i += 2 ) {
            if ( values.get( i ).isBoolean() && !values.get( i ).isNull() && values.get( i ).asBoolean().value.equals( true ) ) {
                return values.get( i + 1 );
            }
        }
        return elseValue;
    }


    private BigDecimal number( Comparable<?> comparable ) {
        return comparable instanceof BigDecimal
                ? (BigDecimal) comparable
                : comparable instanceof BigInteger
                        ? new BigDecimal( (BigInteger) comparable )
                        : comparable instanceof Long || comparable instanceof Integer || comparable instanceof Short
                                ? new BigDecimal( ((Number) comparable).longValue() )
                                : BigDecimal.valueOf( ((Number) comparable).doubleValue() );
    }


    private PolyValue compare( List<PolyValue> values, IntPredicate p ) {
        if ( containsNull( values ) ) {
            return N;
        }
        PolyValue v0 = values.get( 0 );
        PolyValue v1 = values.get( 1 );

        final int c = v0.compareTo( v1 );
        return PolyBoolean.of( p.test( c ) );
    }


    private boolean containsNull( List<PolyValue> values ) {
        for ( PolyValue value : values ) {
            if ( value == N ) {
                return true;
            }
        }
        return false;
    }


    /**
     * An enum that wraps boolean and unknown values and makes them comparable.
     */
    enum Truthy {
        // Order is important; AND returns the min, OR returns the max
        FALSE, UNKNOWN, TRUE;


        static Truthy of( PolyValue c ) {
            return c.isNull() || !c.isBoolean() ? UNKNOWN : (c.asBoolean().value ? TRUE : FALSE);
        }


        PolyValue toPolyValue() {
            return switch ( this ) {
                case TRUE -> PolyBoolean.TRUE;
                case FALSE -> PolyBoolean.FALSE;
                case UNKNOWN -> N;
            };
        }
    }

}


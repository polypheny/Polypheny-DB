/*
 * Copyright 2019-2022 The Polypheny Project
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


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.avatica.util.TimeUnit;
import org.bson.BsonValue;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Function.FunctionType;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.SpecialOperator;
import org.polypheny.db.runtime.FlatLists;
import org.polypheny.db.runtime.PolyCollections.PolyList;
import org.polypheny.db.runtime.PolyCollections.PolyMap;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.MapPolyType;
import org.polypheny.db.type.MultisetPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;
import org.polypheny.db.util.Util;


/**
 * Factory for row expressions.
 *
 * Some common literal values (NULL, TRUE, FALSE, 0, 1, '') are cached.
 */
public class RexBuilder {

    /**
     * Special operator that accesses an unadvertised field of an input record.
     * This operator cannot be used in SQL queries; it is introduced temporarily during sql-to-rel translation, then replaced during the process that trims unwanted fields.
     */
    public static final Operator GET_OPERATOR = new SpecialOperator( "_get", Kind.OTHER_FUNCTION );


    /**
     * The smallest valid {@code int} value, as a {@link BigDecimal}.
     */
    private static final BigDecimal INT_MIN = BigDecimal.valueOf( Integer.MIN_VALUE );

    /**
     * The largest valid {@code int} value, as a {@link BigDecimal}.
     */
    private static final BigDecimal INT_MAX = BigDecimal.valueOf( Integer.MAX_VALUE );


    protected final AlgDataTypeFactory typeFactory;
    private final RexLiteral booleanTrue;
    private final RexLiteral booleanFalse;
    private final RexLiteral charEmpty;
    private final RexLiteral constantNull;


    /**
     * Creates a RexBuilder.
     *
     * @param typeFactory Type factory
     */
    public RexBuilder( AlgDataTypeFactory typeFactory ) {
        this.typeFactory = typeFactory;
        this.booleanTrue =
                makeLiteral(
                        Boolean.TRUE,
                        typeFactory.createPolyType( PolyType.BOOLEAN ),
                        PolyType.BOOLEAN );
        this.booleanFalse =
                makeLiteral(
                        Boolean.FALSE,
                        typeFactory.createPolyType( PolyType.BOOLEAN ),
                        PolyType.BOOLEAN );
        this.charEmpty =
                makeLiteral(
                        new NlsString( "", null, null ),
                        typeFactory.createPolyType( PolyType.CHAR, 0 ),
                        PolyType.CHAR );
        this.constantNull =
                makeLiteral(
                        null,
                        typeFactory.createPolyType( PolyType.NULL ),
                        PolyType.NULL );
    }


    /**
     * Creates a list of {@link RexInputRef} expressions, projecting the fields of a given record type.
     */
    public List<? extends RexNode> identityProjects( final AlgDataType rowType ) {
        return Lists.transform( rowType.getFieldList(), input -> new RexInputRef( input.getIndex(), input.getType() ) );
    }


    /**
     * Returns this RexBuilder's type factory
     *
     * @return type factory
     */
    public AlgDataTypeFactory getTypeFactory() {
        return typeFactory;
    }


    /**
     * Creates an expression accessing a given named field from a record.
     *
     * NOTE: Be careful choosing the value of {@code caseSensitive}. If the field name was supplied by an end-user (e.g. as a column alias in SQL), use your session's case-sensitivity setting.
     * Only hard-code {@code true} if you are sure that the field name is internally generated. Hard-coding {@code false} is almost certainly wrong.</p>
     *
     * @param expr Expression yielding a record
     * @param fieldName Name of field in record
     * @param caseSensitive Whether match is case-sensitive
     * @return Expression accessing a given named field
     */
    public RexNode makeFieldAccess( RexNode expr, String fieldName, boolean caseSensitive ) {
        final AlgDataType type = expr.getType();
        final AlgDataTypeField field = type.getField( fieldName, caseSensitive, false );
        if ( field == null ) {
            throw new AssertionError( "Type '" + type + "' has no field '" + fieldName + "'" );
        }
        return makeFieldAccessInternal( expr, field );
    }


    /**
     * Creates an expression accessing a field with a given ordinal from a record.
     *
     * @param expr Expression yielding a record
     * @param i Ordinal of field
     * @return Expression accessing given field
     */
    public RexNode makeFieldAccess( RexNode expr, int i ) {
        final AlgDataType type = expr.getType();
        final List<AlgDataTypeField> fields = type.getFieldList();
        if ( (i < 0) || (i >= fields.size()) ) {
            throw new AssertionError( "Field ordinal " + i + " is invalid for type '" + type + "'" );
        }
        return makeFieldAccessInternal( expr, fields.get( i ) );
    }


    /**
     * Creates an expression accessing a given field from a record.
     *
     * @param expr Expression yielding a record
     * @param field Field
     * @return Expression accessing given field
     */
    private RexNode makeFieldAccessInternal( RexNode expr, final AlgDataTypeField field ) {
        if ( expr instanceof RexRangeRef ) {
            RexRangeRef range = (RexRangeRef) expr;
            if ( field.getIndex() < 0 ) {
                return makeCall(
                        field.getType(),
                        GET_OPERATOR,
                        ImmutableList.of( expr, makeLiteral( field.getName() ) ) );
            }
            return new RexInputRef(
                    range.getOffset() + field.getIndex(),
                    field.getType() );
        }
        return new RexFieldAccess( expr, field );
    }


    /**
     * Creates a call with a list of arguments and a predetermined type.
     */
    public RexNode makeCall( AlgDataType returnType, Operator op, List<RexNode> exprs ) {
        return new RexCall( returnType, op, exprs );
    }


    /**
     * Creates a call with an array of arguments.
     * <p>
     * If you already know the return type of the call, then {@link #makeCall(AlgDataType, Operator, java.util.List)} is preferred.
     */
    public RexNode makeCall( Operator op, List<? extends RexNode> exprs ) {
        final AlgDataType type = deriveReturnType( op, exprs );
        return new RexCall( type, op, exprs );
    }


    /**
     * Creates a call with a list of arguments.
     *
     * Equivalent to <code>makeCall(op, exprList.toArray(new RexNode[exprList.size()]))</code>.
     */
    public final RexNode makeCall( Operator op, RexNode... exprs ) {
        return makeCall( op, ImmutableList.copyOf( exprs ) );
    }


    /**
     * Derives the return type of a call to an operator.
     *
     * @param op the operator being called
     * @param exprs actual operands
     * @return derived type
     */
    public AlgDataType deriveReturnType( Operator op, List<? extends RexNode> exprs ) {
        return op.inferReturnType( new RexCallBinding( typeFactory, op, exprs, ImmutableList.of() ) );
    }


    /**
     * Creates a reference to an aggregate call, checking for repeated calls.
     *
     * Argument types help to optimize for repeated aggregates.
     * For instance count(42) is equivalent to count(*).
     *
     * @param aggCall aggregate call to be added
     * @param groupCount number of groups in the aggregate relation
     * @param indicator Whether the Aggregate has indicator (GROUPING) columns
     * @param aggCalls destination list of aggregate calls
     * @param aggCallMapping the dictionary of already added calls
     * @param aggArgTypes Argument types, not null
     * @return Rex expression for the given aggregate call
     */
    public RexNode addAggCall( AggregateCall aggCall, int groupCount, boolean indicator, List<AggregateCall> aggCalls, Map<AggregateCall, RexNode> aggCallMapping, final List<AlgDataType> aggArgTypes ) {
        if ( aggCall.getAggregation().getFunctionType() == FunctionType.COUNT && !aggCall.isDistinct() ) {
            final List<Integer> args = aggCall.getArgList();
            final List<Integer> nullableArgs = nullableArgs( args, aggArgTypes );
            if ( !nullableArgs.equals( args ) ) {
                aggCall = aggCall.copy( nullableArgs, aggCall.filterArg, aggCall.collation );
            }
        }
        RexNode rex = aggCallMapping.get( aggCall );
        if ( rex == null ) {
            int index = aggCalls.size() + groupCount * (indicator ? 2 : 1);
            aggCalls.add( aggCall );
            rex = makeInputRef( aggCall.getType(), index );
            aggCallMapping.put( aggCall, rex );
        }
        return rex;
    }


    private static List<Integer> nullableArgs( List<Integer> list0, List<AlgDataType> types ) {
        final List<Integer> list = new ArrayList<>();
        for ( Pair<Integer, AlgDataType> pair : Pair.zip( list0, types ) ) {
            if ( pair.right.isNullable() ) {
                list.add( pair.left );
            }
        }
        return list;
    }


    /**
     * Creates a call to a windowed agg.
     */
    public RexNode makeOver( AlgDataType type, AggFunction operator, List<RexNode> exprs, List<RexNode> partitionKeys, ImmutableList<RexFieldCollation> orderKeys, RexWindowBound lowerBound, RexWindowBound upperBound, boolean physical, boolean allowPartial, boolean nullWhenCountZero, boolean distinct ) {
        assert operator != null;
        assert exprs != null;
        assert partitionKeys != null;
        assert orderKeys != null;
        final RexWindow window = makeWindow( partitionKeys, orderKeys, lowerBound, upperBound, physical );
        final RexOver over = new RexOver( type, operator, exprs, window, distinct );
        RexNode result = over;

        // This should be correct but need time to go over test results.
        // Also want to look at combing with section below.
        if ( nullWhenCountZero ) {
            final AlgDataType bigintType = getTypeFactory().createPolyType( PolyType.BIGINT );
            result = makeCall(
                    OperatorRegistry.get( OperatorName.CASE ),
                    makeCall(
                            OperatorRegistry.get( OperatorName.GREATER_THAN ),
                            new RexOver(
                                    bigintType,
                                    OperatorRegistry.get( OperatorName.COUNT ),
                                    exprs,
                                    window,
                                    distinct ),
                            makeLiteral(
                                    BigDecimal.ZERO,
                                    bigintType,
                                    PolyType.DECIMAL ) ),
                    ensureType(
                            type, // SUM0 is non-nullable, thus need a cast
                            new RexOver(
                                    typeFactory.createTypeWithNullability( type, false ),
                                    operator,
                                    exprs,
                                    window,
                                    distinct ),
                            false ),
                    makeCast( type, constantNull() ) );
        }
        if ( !allowPartial ) {
            Preconditions.checkArgument( physical, "DISALLOW PARTIAL over RANGE" );
            final AlgDataType bigintType = getTypeFactory().createPolyType( PolyType.BIGINT );
            // todo: read bound
            result =
                    makeCall(
                            OperatorRegistry.get( OperatorName.CASE ),
                            makeCall(
                                    OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ),
                                    new RexOver(
                                            bigintType,
                                            OperatorRegistry.get( OperatorName.COUNT ),
                                            ImmutableList.of(),
                                            window,
                                            distinct ),
                                    makeLiteral(
                                            BigDecimal.valueOf( 2 ),
                                            bigintType,
                                            PolyType.DECIMAL ) ),
                            result,
                            constantNull );
        }
        return result;
    }


    /**
     * Creates a window specification.
     *
     * @param partitionKeys Partition keys
     * @param orderKeys Order keys
     * @param lowerBound Lower bound
     * @param upperBound Upper bound
     * @param isRows Whether physical. True if row-based, false if range-based
     * @return window specification
     */
    public RexWindow makeWindow( List<RexNode> partitionKeys, ImmutableList<RexFieldCollation> orderKeys, RexWindowBound lowerBound, RexWindowBound upperBound, boolean isRows ) {
        return new RexWindow( partitionKeys, orderKeys, lowerBound, upperBound, isRows );
    }


    /**
     * Creates a constant for the SQL <code>NULL</code> value.
     */
    public RexLiteral constantNull() {
        return constantNull;
    }


    /**
     * Creates an expression referencing a correlation variable.
     *
     * @param id Name of variable
     * @param type Type of variable
     * @return Correlation variable
     */
    public RexNode makeCorrel( AlgDataType type, CorrelationId id ) {
        return new RexCorrelVariable( id, type );
    }


    /**
     * Creates an invocation of the NEW operator.
     *
     * @param type Type to be instantiated
     * @param exprs Arguments to NEW operator
     * @return Expression invoking NEW operator
     */
    public RexNode makeNewInvocation( AlgDataType type, List<RexNode> exprs ) {
        return new RexCall( type, OperatorRegistry.get( OperatorName.NEW ), exprs );
    }


    /**
     * Creates a call to the CAST operator.
     *
     * @param type Type to cast to
     * @param exp Expression being cast
     * @return Call to CAST operator
     */
    public RexNode makeCast( AlgDataType type, RexNode exp ) {
        return makeCast( type, exp, false );
    }


    /**
     * Creates a call to the CAST operator, expanding if possible, and optionally also preserving nullability.
     *
     * Tries to expand the cast, and therefore the result may be something other than a {@link RexCall} to the CAST operator, such as a {@link RexLiteral}.
     *
     * @param type Type to cast to
     * @param exp Expression being cast
     * @param matchNullability Whether to ensure the result has the same nullability as {@code type}
     * @return Call to CAST operator
     */
    public RexNode makeCast( AlgDataType type, RexNode exp, boolean matchNullability ) {
        // MV: This might be a bad idea. It would be better to implement cast support for array columns
        if ( exp.getType().getPolyType() == PolyType.ARRAY ) {
            return exp;
        }

        final PolyType sqlType = type.getPolyType();
        if ( exp instanceof RexLiteral ) {
            RexLiteral literal = (RexLiteral) exp;
            Comparable value = literal.getValueAs( Comparable.class );
            PolyType typeName = literal.getTypeName();
            if ( canRemoveCastFromLiteral( type, value, typeName ) ) {
                switch ( typeName ) {
                    case INTERVAL_YEAR:
                    case INTERVAL_YEAR_MONTH:
                    case INTERVAL_MONTH:
                    case INTERVAL_DAY:
                    case INTERVAL_DAY_HOUR:
                    case INTERVAL_DAY_MINUTE:
                    case INTERVAL_DAY_SECOND:
                    case INTERVAL_HOUR:
                    case INTERVAL_HOUR_MINUTE:
                    case INTERVAL_HOUR_SECOND:
                    case INTERVAL_MINUTE:
                    case INTERVAL_MINUTE_SECOND:
                    case INTERVAL_SECOND:
                        assert value instanceof BigDecimal;
                        typeName = type.getPolyType();
                        switch ( typeName ) {
                            case BIGINT:
                            case INTEGER:
                            case SMALLINT:
                            case TINYINT:
                            case FLOAT:
                            case REAL:
                            case DECIMAL:
                                BigDecimal value2 = (BigDecimal) value;
                                final BigDecimal multiplier = baseUnit( literal.getTypeName() ).multiplier;
                                final BigDecimal divider = literal.getTypeName().getEndUnit().multiplier;
                                value = value2.multiply( multiplier ).divide( divider, 0, RoundingMode.HALF_DOWN );
                        }

                        // Not all types are allowed for literals
                        switch ( typeName ) {
                            case INTEGER:
                                typeName = PolyType.BIGINT;
                        }
                }
                final RexLiteral literal2 = makeLiteral( value, type, typeName );
                if ( type.isNullable()
                        && !literal2.getType().isNullable()
                        && matchNullability ) {
                    return makeAbstractCast( type, literal2 );
                }
                return literal2;
            }
        } else if ( PolyTypeUtil.isExactNumeric( type ) && PolyTypeUtil.isInterval( exp.getType() ) ) {
            return makeCastIntervalToExact( type, exp );
        } else if ( sqlType == PolyType.BOOLEAN && PolyTypeUtil.isExactNumeric( exp.getType() ) ) {
            return makeCastExactToBoolean( type, exp );
        } else if ( exp.getType().getPolyType() == PolyType.BOOLEAN && PolyTypeUtil.isExactNumeric( type ) ) {
            return makeCastBooleanToExact( type, exp );
        }
        return makeAbstractCast( type, exp );
    }


    /**
     * Returns the lowest granularity unit for the given unit.
     * YEAR and MONTH intervals are stored as months; HOUR, MINUTE, SECOND intervals are stored as milliseconds.
     */
    protected static TimeUnit baseUnit( PolyType unit ) {
        if ( unit.isYearMonth() ) {
            return TimeUnit.MONTH;
        } else {
            return TimeUnit.MILLISECOND;
        }
    }


    boolean canRemoveCastFromLiteral( AlgDataType toType, Comparable value, PolyType fromTypeName ) {
        final PolyType sqlType = toType.getPolyType();
        if ( !RexLiteral.valueMatchesType( value, sqlType, false ) ) {
            return false;
        }
        if ( toType.getPolyType() != fromTypeName && PolyTypeFamily.DATETIME.getTypeNames().contains( fromTypeName ) ) {
            return false;
        }
        if ( value instanceof NlsString ) {
            final int length = ((NlsString) value).getValue().length();
            switch ( toType.getPolyType() ) {
                case CHAR:
                    return PolyTypeUtil.comparePrecision( toType.getPrecision(), length ) == 0;
                case JSON:
                case VARCHAR:
                    return PolyTypeUtil.comparePrecision( toType.getPrecision(), length ) >= 0;
                default:
                    throw new AssertionError( toType );
            }
        }
        if ( value instanceof ByteString ) {
            final int length = ((ByteString) value).length();
            switch ( toType.getPolyType() ) {
                case BINARY:
                    return PolyTypeUtil.comparePrecision( toType.getPrecision(), length ) == 0;
                case VARBINARY:
                    return PolyTypeUtil.comparePrecision( toType.getPrecision(), length ) >= 0;
                default:
                    throw new AssertionError( toType );
            }
        }
        return true;
    }


    private RexNode makeCastExactToBoolean( AlgDataType toType, RexNode exp ) {
        return makeCall( toType, OperatorRegistry.get( OperatorName.NOT_EQUALS ), ImmutableList.of( exp, makeZeroLiteral( exp.getType() ) ) );
    }


    private RexNode makeCastBooleanToExact( AlgDataType toType, RexNode exp ) {
        final RexNode casted =
                makeCall(
                        OperatorRegistry.get( OperatorName.CASE ),
                        exp,
                        makeExactLiteral( BigDecimal.ONE, toType ),
                        makeZeroLiteral( toType ) );
        if ( !exp.getType().isNullable() ) {
            return casted;
        }
        return makeCall(
                toType,
                OperatorRegistry.get( OperatorName.CASE ),
                ImmutableList.of(
                        makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), exp ),
                        casted,
                        makeNullLiteral( toType ) ) );
    }


    private RexNode makeCastIntervalToExact( AlgDataType toType, RexNode exp ) {
        final TimeUnit endUnit = exp.getType().getPolyType().getEndUnit();
        final TimeUnit baseUnit = baseUnit( exp.getType().getPolyType() );
        final BigDecimal multiplier = baseUnit.multiplier;
        final int scale = 0;
        BigDecimal divider = endUnit.multiplier.scaleByPowerOfTen( -scale );
        RexNode value = multiplyDivide( decodeIntervalOrDecimal( exp ), multiplier, divider );
        if ( scale > 0 ) {
            AlgDataType decimalType =
                    getTypeFactory().createPolyType(
                            PolyType.DECIMAL,
                            scale + exp.getType().getPrecision(),
                            scale );
            value = encodeIntervalOrDecimal( value, decimalType, false );
        }
        return ensureType( toType, value, false );
    }


    public RexNode multiplyDivide( RexNode e, BigDecimal multiplier, BigDecimal divider ) {
        assert multiplier.signum() > 0;
        assert divider.signum() > 0;
        switch ( multiplier.compareTo( divider ) ) {
            case 0:
                return e;
            case 1:
                // E.g. multiplyDivide(e, 1000, 10) ==> e * 100
                return makeCall( OperatorRegistry.get( OperatorName.MULTIPLY ), e, makeExactLiteral( multiplier.divide( divider, RoundingMode.UNNECESSARY ) ) );
            case -1:
                // E.g. multiplyDivide(e, 10, 1000) ==> e / 100
                return makeCall( OperatorRegistry.get( OperatorName.DIVIDE_INTEGER ), e, makeExactLiteral( divider.divide( multiplier, RoundingMode.UNNECESSARY ) ) );
            default:
                throw new AssertionError( multiplier + "/" + divider );
        }
    }


    /**
     * Casts a decimal's integer representation to a decimal node. If the expression is not the expected integer type, then it is casted first.
     *
     * An overflow check may be requested to ensure the internal value does not exceed the maximum value of the decimal type.
     *
     * @param value integer representation of decimal
     * @param type type integer will be reinterpreted as
     * @param checkOverflow indicates whether an overflow check is required when reinterpreting this particular value as the decimal type. A check usually not required for arithmetic, but is often required for rounding and explicit casts.
     * @return the integer reinterpreted as an opaque decimal type
     */
    public RexNode encodeIntervalOrDecimal( RexNode value, AlgDataType type, boolean checkOverflow ) {
        AlgDataType bigintType = typeFactory.createPolyType( PolyType.BIGINT );
        RexNode cast = ensureType( bigintType, value, true );
        return makeReinterpretCast( type, cast, makeLiteral( checkOverflow ) );
    }


    /**
     * Retrieves an interval or decimal node's integer representation
     *
     * @param node the interval or decimal value as an opaque type
     * @return an integer representation of the decimal value
     */
    public RexNode decodeIntervalOrDecimal( RexNode node ) {
        assert PolyTypeUtil.isDecimal( node.getType() ) || PolyTypeUtil.isInterval( node.getType() );
        AlgDataType bigintType = typeFactory.createPolyType( PolyType.BIGINT );
        return makeReinterpretCast( matchNullability( bigintType, node ), node, makeLiteral( false ) );
    }


    /**
     * Creates a call to the CAST operator.
     *
     * @param type Type to cast to
     * @param exp Expression being cast
     * @return Call to CAST operator
     */
    public RexNode makeAbstractCast( AlgDataType type, RexNode exp ) {
        return new RexCall( type, OperatorRegistry.get( OperatorName.CAST ), ImmutableList.of( exp ) );
    }


    /**
     * Makes a reinterpret cast.
     *
     * @param type type returned by the cast
     * @param exp expression to be casted
     * @param checkOverflow whether an overflow check is required
     * @return a RexCall with two operands and a special return type
     */
    public RexNode makeReinterpretCast( AlgDataType type, RexNode exp, RexNode checkOverflow ) {
        List<RexNode> args;
        if ( (checkOverflow != null) && checkOverflow.isAlwaysTrue() ) {
            args = ImmutableList.of( exp, checkOverflow );
        } else {
            args = ImmutableList.of( exp );
        }
        return new RexCall( type, OperatorRegistry.get( OperatorName.REINTERPRET ), args );
    }


    /**
     * Makes a cast of a value to NOT NULL; no-op if the type already has NOT NULL.
     */
    public RexNode makeNotNull( RexNode exp ) {
        final AlgDataType type = exp.getType();
        if ( !type.isNullable() ) {
            return exp;
        }
        final AlgDataType notNullType = typeFactory.createTypeWithNullability( type, false );
        return makeAbstractCast( notNullType, exp );
    }


    /**
     * Creates a reference to all the fields in the row. That is, the whole row as a single record object.
     *
     * @param input Input relational expression
     */
    public RexNode makeRangeReference( AlgNode input ) {
        return new RexRangeRef( input.getRowType(), 0 );
    }


    /**
     * Creates a reference to all the fields in the row.
     *
     * For example, if the input row has type <code>T{f0,f1,f2,f3,f4}</code> then <code>makeRangeReference(T{f0,f1,f2,f3,f4}, S{f3,f4}, 3)</code> is an expression which yields the last 2 fields.
     *
     * @param type Type of the resulting range record.
     * @param offset Index of first field.
     * @param nullable Whether the record is nullable.
     */
    public RexRangeRef makeRangeReference( AlgDataType type, int offset, boolean nullable ) {
        if ( nullable && !type.isNullable() ) {
            type = typeFactory.createTypeWithNullability( type, nullable );
        }
        return new RexRangeRef( type, offset );
    }


    /**
     * Creates a reference to a given field of the input record.
     *
     * @param type Type of field
     * @param i Ordinal of field
     * @return Reference to field
     */
    public RexInputRef makeInputRef( AlgDataType type, int i ) {
        type = PolyTypeUtil.addCharsetAndCollation( type, typeFactory );
        return new RexInputRef( i, type );
    }


    /**
     * Creates a reference to a given field of the input relational expression.
     *
     * @param input Input relational expression
     * @param i Ordinal of field
     * @return Reference to field
     * @see #identityProjects(AlgDataType)
     */
    public RexInputRef makeInputRef( AlgNode input, int i ) {
        return makeInputRef( input.getRowType().getFieldList().get( i ).getType(), i );
    }


    /**
     * Creates a reference to a given field of the pattern.
     *
     * @param alpha the pattern name
     * @param type Type of field
     * @param i Ordinal of field
     * @return Reference to field of pattern
     */
    public RexPatternFieldRef makePatternFieldRef( String alpha, AlgDataType type, int i ) {
        type = PolyTypeUtil.addCharsetAndCollation( type, typeFactory );
        return new RexPatternFieldRef( alpha, i, type );
    }


    /**
     * Creates a literal representing a flag.
     *
     * @param flag Flag value
     */
    public RexLiteral makeFlag( Enum flag ) {
        assert flag != null;
        return makeLiteral( flag, typeFactory.createPolyType( PolyType.SYMBOL ), PolyType.SYMBOL );
    }


    /**
     * Internal method to create a call to a literal. Code outside this package should call one of the type-specific methods such as {@link #makeDateLiteral(DateString)}, {@link #makeLiteral(boolean)},
     * {@link #makeLiteral(String)}.
     *
     * @param o Value of literal, must be appropriate for the type
     * @param type Type of literal
     * @param typeName SQL type of literal
     * @return Literal
     */
    protected RexLiteral makeLiteral( Comparable o, AlgDataType type, PolyType typeName ) {
        // All literals except NULL have NOT NULL types.
        type = typeFactory.createTypeWithNullability( type, o == null );
        int p;
        switch ( typeName ) {
            case CHAR:
                // Character literals must have a charset and collation. Populate from the type if necessary.
                assert o instanceof NlsString;
                NlsString nlsString = (NlsString) o;
                if ( (nlsString.getCollation() == null) || (nlsString.getCharset() == null) ) {
                    assert type.getPolyType() == PolyType.CHAR;
                    assert type.getCharset().name() != null;
                    assert type.getCollation() != null;
                    o = new NlsString( nlsString.getValue(), type.getCharset().name(), type.getCollation() );
                }
                break;
            case TIME:
                assert o instanceof TimeString;
                p = type.getPrecision();
                if ( p == AlgDataType.PRECISION_NOT_SPECIFIED ) {
                    p = 0;
                }
                o = ((TimeString) o).round( p );
                break;
            case TIME_WITH_LOCAL_TIME_ZONE:
                assert o instanceof TimeString;
                p = type.getPrecision();
                if ( p == AlgDataType.PRECISION_NOT_SPECIFIED ) {
                    p = 0;
                }
                o = ((TimeString) o).round( p );
                break;
            case TIMESTAMP:
                assert o instanceof TimestampString;
                p = type.getPrecision();
                if ( p == AlgDataType.PRECISION_NOT_SPECIFIED ) {
                    p = 0;
                }
                o = ((TimestampString) o).round( p );
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                assert o instanceof TimestampString;
                p = type.getPrecision();
                if ( p == AlgDataType.PRECISION_NOT_SPECIFIED ) {
                    p = 0;
                }
                o = ((TimestampString) o).round( p );
        }
        return new RexLiteral( o, type, typeName );
    }


    /**
     * Creates a boolean literal.
     */
    public RexLiteral makeLiteral( boolean b ) {
        return b ? booleanTrue : booleanFalse;
    }


    /**
     * Creates a numeric literal.
     */
    public RexLiteral makeExactLiteral( BigDecimal bd ) {
        AlgDataType algType;
        int scale = bd.scale();
        assert scale >= 0;
        assert scale <= typeFactory.getTypeSystem().getMaxNumericScale() : scale;
        if ( scale == 0 ) {
            if ( bd.compareTo( INT_MIN ) >= 0 && bd.compareTo( INT_MAX ) <= 0 ) {
                algType = typeFactory.createPolyType( PolyType.INTEGER );
            } else {
                algType = typeFactory.createPolyType( PolyType.BIGINT );
            }
        } else {
            int precision = bd.unscaledValue().abs().toString().length();
            if ( precision > scale ) {
                // bd is greater than or equal to 1
                algType = typeFactory.createPolyType( PolyType.DECIMAL, precision, scale );
            } else {
                // bd is less than 1
                algType = typeFactory.createPolyType( PolyType.DECIMAL, scale + 1, scale );
            }
        }
        return makeExactLiteral( bd, algType );
    }


    /**
     * Creates a BIGINT literal.
     */
    public RexLiteral makeBigintLiteral( BigDecimal bd ) {
        AlgDataType bigintType = typeFactory.createPolyType( PolyType.BIGINT );
        return makeLiteral( bd, bigintType, PolyType.DECIMAL );
    }


    /**
     * Creates a numeric literal.
     */
    public RexLiteral makeExactLiteral( BigDecimal bd, AlgDataType type ) {
        return makeLiteral( bd, type, PolyType.DECIMAL );
    }


    /**
     * Creates a byte array literal.
     */
    public RexLiteral makeBinaryLiteral( ByteString byteString ) {
        return makeLiteral(
                byteString,
                typeFactory.createPolyType( PolyType.BINARY, byteString.length() ),
                PolyType.BINARY );
    }


    public RexLiteral makeFileLiteral( byte[] bytes ) {
        ByteString byteString = new ByteString( bytes );
        return makeLiteral(
                new ByteString( bytes ),
                typeFactory.createPolyType( PolyType.BINARY, byteString.length() ),
                PolyType.BINARY );
    }


    /**
     * Creates a double-precision literal.
     */
    public RexLiteral makeApproxLiteral( BigDecimal bd ) {
        // Validator should catch if underflow is allowed.
        // If underflow is allowed, let underflow become zero
        if ( bd.doubleValue() == 0 ) {
            bd = BigDecimal.ZERO;
        }
        return makeApproxLiteral( bd, typeFactory.createPolyType( PolyType.DOUBLE ) );
    }


    /**
     * Creates an approximate numeric literal (double or float).
     *
     * @param bd literal value
     * @param type approximate numeric type
     * @return new literal
     */
    public RexLiteral makeApproxLiteral( BigDecimal bd, AlgDataType type ) {
        assert PolyTypeFamily.APPROXIMATE_NUMERIC.getTypeNames().contains( type.getPolyType() );
        return makeLiteral( bd, type, PolyType.DOUBLE );
    }


    /**
     * Creates a character string literal.
     */
    public RexLiteral makeLiteral( String s ) {
        assert s != null;
        return makePreciseStringLiteral( s );
    }


    /**
     * Creates a character string literal with type CHAR and default charset and collation.
     *
     * @param s String value
     * @return Character string literal
     */
    protected RexLiteral makePreciseStringLiteral( String s ) {
        assert s != null;
        if ( s.equals( "" ) ) {
            return charEmpty;
        }
        return makeCharLiteral( new NlsString( s, null, null ) );
    }


    /**
     * Creates a character string literal with type CHAR.
     *
     * @param value String value in bytes
     * @param charsetName SQL-level charset name
     * @param collation Sql collation
     * @return String     literal
     */
    protected RexLiteral makePreciseStringLiteral( ByteString value, String charsetName, Collation collation ) {
        return makeCharLiteral( new NlsString( value, charsetName, collation ) );
    }


    /**
     * Ensures expression is interpreted as a specified type. The returned expression may be wrapped with a cast.
     *
     * @param type desired type
     * @param node expression
     * @param matchNullability whether to correct nullability of specified type to match the expression; this usually should be true, except for explicit casts which can override default nullability
     * @return a casted expression or the original expression
     */
    public RexNode ensureType( AlgDataType type, RexNode node, boolean matchNullability ) {
        AlgDataType targetType = type;
        if ( matchNullability ) {
            targetType = matchNullability( type, node );
        }

        if ( targetType.getPolyType() == PolyType.ANY && (!matchNullability || targetType.isNullable() == node.getType().isNullable()) ) {
            return node;
        }

        // Special handling for arrays
        if ( node instanceof RexCall && ((RexCall) node).op.getKind() == Kind.ARRAY_VALUE_CONSTRUCTOR ) {
            ArrayType arrayType = (ArrayType) node.getType();
            /*List<RexNode> newRexNodes = new ArrayList<>( ((RexCall) node).operands.size() );
            for ( RexNode n : ((RexCall) node).operands ) {
                newRexNodes.add( makeCast( targetType.getComponentType(), n ) );
            }*/
            return new RexLiteral( PolyList.ofArray( ((RexCall) node).operands ), arrayType, arrayType.getPolyType() );
            //return new RexCall( ((RexCall) node).type, ((RexCall) node).op, newRexNodes );
        } else if ( !node.getType().equals( targetType ) ) {
            return makeCast( targetType, node );
        }
        return node;
    }


    /**
     * Ensures that a type's nullability matches a value's nullability.
     */
    public AlgDataType matchNullability( AlgDataType type, RexNode value ) {
        boolean typeNullability = type.isNullable();
        boolean valueNullability = value.getType().isNullable();
        if ( typeNullability != valueNullability ) {
            return getTypeFactory().createTypeWithNullability(
                    type,
                    valueNullability );
        }
        return type;
    }


    /**
     * Creates a character string literal from an {@link NlsString}.
     *
     * If the string's charset and collation are not set, uses the system defaults.
     */
    public RexLiteral makeCharLiteral( NlsString str ) {
        assert str != null;
        AlgDataType type = CoreUtil.createNlsStringType( typeFactory, str );
        return makeLiteral( str, type, PolyType.CHAR );
    }


    /**
     * Creates a Date literal.
     */
    public RexLiteral makeDateLiteral( DateString date ) {
        return makeLiteral( Objects.requireNonNull( date ), typeFactory.createPolyType( PolyType.DATE ), PolyType.DATE );
    }


    /**
     * Creates a Time literal.
     */
    public RexLiteral makeTimeLiteral( TimeString time, int precision ) {
        return makeLiteral(
                Objects.requireNonNull( time ),
                typeFactory.createPolyType( PolyType.TIME, precision ),
                PolyType.TIME );
    }


    /**
     * Creates a Time with local time-zone literal.
     */
    public RexLiteral makeTimeWithLocalTimeZoneLiteral( TimeString time, int precision ) {
        return makeLiteral(
                Objects.requireNonNull( time ),
                typeFactory.createPolyType( PolyType.TIME_WITH_LOCAL_TIME_ZONE, precision ),
                PolyType.TIME_WITH_LOCAL_TIME_ZONE );
    }


    /**
     * Creates a Timestamp literal.
     */
    public RexLiteral makeTimestampLiteral( TimestampString timestamp, int precision ) {
        return makeLiteral(
                Objects.requireNonNull( timestamp ),
                typeFactory.createPolyType( PolyType.TIMESTAMP, precision ),
                PolyType.TIMESTAMP );
    }


    /**
     * Creates a Timestamp with local time-zone literal.
     */
    public RexLiteral makeTimestampWithLocalTimeZoneLiteral( TimestampString timestamp, int precision ) {
        return makeLiteral(
                Objects.requireNonNull( timestamp ),
                typeFactory.createPolyType( PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE, precision ),
                PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE );
    }


    /**
     * Creates a literal representing an interval type, for example {@code YEAR TO MONTH} or {@code DOW}.
     */
    public RexLiteral makeIntervalLiteral( IntervalQualifier intervalQualifier ) {
        assert intervalQualifier != null;
        return makeFlag( intervalQualifier.getTimeUnitRange() );
    }


    /**
     * Creates a literal representing an interval value, for example {@code INTERVAL '3-7' YEAR TO MONTH}.
     */
    public RexLiteral makeIntervalLiteral( BigDecimal v, IntervalQualifier intervalQualifier ) {
        return makeLiteral(
                v,
                typeFactory.createSqlIntervalType( intervalQualifier ),
                intervalQualifier.typeName() );
    }


    /**
     * Creates a reference to a dynamic parameter
     *
     * @param type Type of dynamic parameter
     * @param index Index of dynamic parameter
     * @return Expression referencing dynamic parameter
     */
    public RexDynamicParam makeDynamicParam( AlgDataType type, int index ) {
        return new RexDynamicParam( type, index );
    }


    /**
     * Creates a literal whose value is NULL, with a particular type.
     *
     * The typing is necessary because RexNodes are strictly typed. For example, in the Rex world the <code>NULL</code> parameter to <code>SUBSTRING(NULL FROM 2 FOR 4)</code> must have a valid VARCHAR type so
     * that the result type can be determined.
     *
     * @param type Type to cast NULL to
     * @return NULL literal of given type
     */
    public RexLiteral makeNullLiteral( AlgDataType type ) {
        if ( !type.isNullable() ) {
            type = typeFactory.createTypeWithNullability( type, true );
        }
        return (RexLiteral) makeCast( type, constantNull() );
    }


    /**
     * Creates a copy of an expression, which may have been created using a different RexBuilder and/or {@link AlgDataTypeFactory}, using this RexBuilder.
     *
     * @param expr Expression
     * @return Copy of expression
     * @see AlgDataTypeFactory#copyType(AlgDataType)
     */
    public RexNode copy( RexNode expr ) {
        return expr.accept( new RexCopier( this ) );
    }


    /**
     * Creates a literal of the default value for the given type.
     *
     * This value is:
     *
     * <ul>
     * <li>0 for numeric types;</li>
     * <li>FALSE for BOOLEAN;</li>
     * <li>The epoch for TIMESTAMP and DATE;</li>
     * <li>Midnight for TIME;</li>
     * <li>The empty string for string types (CHAR, BINARY, VARCHAR, VARBINARY).</li>
     * </ul>
     *
     * @param type Type
     * @return Simple literal, or cast simple literal
     */
    public RexNode makeZeroLiteral( AlgDataType type ) {
        return makeLiteral( zeroValue( type ), type, false );
    }


    private static Comparable zeroValue( AlgDataType type ) {
        switch ( type.getPolyType() ) {
            case CHAR:
                return new NlsString( Spaces.of( type.getPrecision() ), null, null );
            case JSON:
            case VARCHAR:
                return new NlsString( "", null, null );
            case BINARY:
                return new ByteString( new byte[type.getPrecision()] );
            case VARBINARY:
                return ByteString.EMPTY;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case FLOAT:
            case REAL:
            case DOUBLE:
                return BigDecimal.ZERO;
            case BOOLEAN:
                return false;
            case TIME:
            case DATE:
            case TIMESTAMP:
                return DateTimeUtils.ZERO_CALENDAR;
            case TIME_WITH_LOCAL_TIME_ZONE:
                return new TimeString( 0, 0, 0 );
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new TimestampString( 0, 0, 0, 0, 0, 0 );
            default:
                throw Util.unexpected( type.getPolyType() );
        }
    }


    /**
     * Creates a literal of a given type. The value is assumed to be compatible with the type.
     *
     * @param value Value
     * @param type Type
     * @param allowCast Whether to allow a cast. If false, value is always a {@link RexLiteral} but may not be the exact type
     * @return Simple literal, or cast simple literal
     */
    public RexNode makeLiteral( Object value, AlgDataType type, boolean allowCast ) {
        if ( value == null ) {
            return makeCast( type, constantNull );
        }
        if ( type.isNullable() ) {
            final AlgDataType typeNotNull = typeFactory.createTypeWithNullability( type, false );
            RexNode literalNotNull = makeLiteral( value, typeNotNull, allowCast );
            return makeAbstractCast( type, literalNotNull );
        }
        value = clean( value, type );
        RexLiteral literal;
        final List<RexNode> operands;
        switch ( type.getPolyType() ) {
            case CHAR:
                return makeCharLiteral( padRight( (NlsString) value, type.getPrecision() ) );
            case VARCHAR:
                literal = makeCharLiteral( (NlsString) value );
                if ( allowCast ) {
                    return makeCast( type, literal );
                } else {
                    return literal;
                }
            case BINARY:
                return makeBinaryLiteral( padRight( (ByteString) value, type.getPrecision() ) );
            case VARBINARY:
                literal = makeBinaryLiteral( (ByteString) value );
                if ( allowCast ) {
                    return makeCast( type, literal );
                } else {
                    return literal;
                }
            case FILE:
                return makeFileLiteral( (byte[]) value );
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
                return makeExactLiteral( (BigDecimal) value, type );
            case FLOAT:
            case REAL:
            case DOUBLE:
                return makeApproxLiteral( (BigDecimal) value, type );
            case BOOLEAN:
                return (Boolean) value ? booleanTrue : booleanFalse;
            case TIME:
                return makeTimeLiteral( (TimeString) value, type.getPrecision() );
            case TIME_WITH_LOCAL_TIME_ZONE:
                return makeTimeWithLocalTimeZoneLiteral( (TimeString) value, type.getPrecision() );
            case DATE:
                return makeDateLiteral( (DateString) value );
            case TIMESTAMP:
                return makeTimestampLiteral( (TimestampString) value, type.getPrecision() );
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return makeTimestampWithLocalTimeZoneLiteral( (TimestampString) value, type.getPrecision() );
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                return makeIntervalLiteral( (BigDecimal) value, type.getIntervalQualifier() );
            case MAP:
                return makeMap( (Map<Object, Object>) value, type, allowCast );
            case ARRAY:
                return makeArray( (List<Object>) value, type, allowCast );
            //return makeCall( OperatorRegistry.get( OperatorName.ARRAY_VALUE_CONSTRUCTOR ), operands );
            case MULTISET:
                final MultisetPolyType multisetType = (MultisetPolyType) type;
                operands = new ArrayList<>();
                for ( Object entry : (List<?>) value ) {
                    final RexNode e =
                            entry instanceof RexLiteral
                                    ? (RexNode) entry
                                    : makeLiteral( entry, multisetType.getComponentType(), allowCast );
                    operands.add( e );
                }
                if ( allowCast ) {
                    return makeCall( OperatorRegistry.get( OperatorName.MULTISET_VALUE ), operands );
                } else {
                    return new RexLiteral( (Comparable<?>) FlatLists.of( operands ), type, type.getPolyType() );
                }
            case ROW:
                operands = new ArrayList<>();
                //noinspection unchecked
                for ( Pair<AlgDataTypeField, Object> pair : Pair.zip( type.getFieldList(), (List<Object>) value ) ) {
                    final RexNode e =
                            pair.right instanceof RexLiteral
                                    ? (RexNode) pair.right
                                    : makeLiteral( pair.right, pair.left.getType(), allowCast );
                    operands.add( e );
                }
                return new RexLiteral( (Comparable<?>) FlatLists.of( operands ), type, type.getPolyType() );
            case NODE:
            case EDGE:
                return new RexLiteral( (Comparable<?>) value, type, type.getPolyType() );
            case ANY:
                return makeLiteral( value, guessType( value ), allowCast );
            default:
                throw Util.unexpected( type.getPolyType() );
        }
    }


    private RexLiteral makeMap( Map<Object, Object> value, AlgDataType type, boolean allowCast ) {
        final MapPolyType mapType = (MapPolyType) type;

        final Map<RexNode, RexNode> map = value
                .entrySet()
                .stream()
                .collect( Collectors.toMap( e -> makeLiteral( e.getKey(), mapType.getKeyType(), allowCast ), e -> makeLiteral( e.getValue(), mapType.getValueType(), allowCast ) ) );

        return makeMap( type, map );
    }


    private RexLiteral makeArray( List<Object> value, AlgDataType type, boolean allowCast ) {
        if ( value.stream().allMatch( v -> v instanceof RexLiteral ) ) {
            return makeArray( type, value.stream().map( v -> (RexLiteral) v ).collect( Collectors.toList() ) );
        }

        final List<RexNode> operands;
        final ArrayType arrayType = (ArrayType) type;
        @SuppressWarnings("unchecked") final List<Object> listValue = value;
        operands = new ArrayList<>();
        for ( Object entry : listValue ) {
            operands.add( makeLiteral( entry, arrayType.getComponentType(), allowCast ) );
        }
        return makeArray( type, operands );
    }


    /**
     * Converts the type of a value to comply with {@link RexLiteral#valueMatchesType}.
     */
    private static Object clean( Object o, AlgDataType type ) {
        if ( o == null ) {
            return null;
        }
        switch ( type.getPolyType() ) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                if ( o instanceof BigDecimal ) {
                    return o;
                }
                return new BigDecimal( ((Number) o).longValue() );
            case FLOAT:
                if ( o instanceof BigDecimal ) {
                    return o;
                }
                return new BigDecimal( ((Number) o).doubleValue(), MathContext.DECIMAL32 ).stripTrailingZeros();
            case REAL:
            case DOUBLE:
                if ( o instanceof BigDecimal ) {
                    return o;
                }
                return new BigDecimal( ((Number) o).doubleValue(), MathContext.DECIMAL64 ).stripTrailingZeros();
            case CHAR:
            case VARCHAR:
                if ( o instanceof NlsString ) {
                    return o;
                }
                return new NlsString( (String) o, type.getCharset().name(), type.getCollation() );
            case TIME:
                if ( o instanceof TimeString ) {
                    return o;
                } else if ( o instanceof Calendar ) {
                    if ( !((Calendar) o).getTimeZone().equals( DateTimeUtils.UTC_ZONE ) ) {
                        throw new AssertionError();
                    }
                    return TimeString.fromCalendarFields( (Calendar) o );
                } else {
                    return TimeString.fromMillisOfDay( (Integer) o );
                }
            case TIME_WITH_LOCAL_TIME_ZONE:
                if ( o instanceof TimeString ) {
                    return o;
                } else {
                    return TimeString.fromMillisOfDay( (Integer) o );
                }
            case DATE:
                if ( o instanceof DateString ) {
                    return o;
                } else if ( o instanceof Calendar ) {
                    if ( !((Calendar) o).getTimeZone().equals( DateTimeUtils.UTC_ZONE ) ) {
                        throw new AssertionError();
                    }
                    return DateString.fromCalendarFields( (Calendar) o );
                } else {
                    return DateString.fromDaysSinceEpoch( (Integer) o );
                }
            case TIMESTAMP:
                if ( o instanceof TimestampString ) {
                    return o;
                } else if ( o instanceof Calendar ) {
                    if ( !((Calendar) o).getTimeZone().equals( DateTimeUtils.UTC_ZONE ) ) {
                        throw new AssertionError();
                    }
                    return TimestampString.fromCalendarFields( (Calendar) o );
                } else {
                    return TimestampString.fromMillisSinceEpoch( (Long) o );
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if ( o instanceof TimestampString ) {
                    return o;
                } else {
                    return TimestampString.fromMillisSinceEpoch( (Long) o );
                }
            default:
                return o;
        }
    }


    private AlgDataType guessType( Object value ) {
        if ( value == null ) {
            return typeFactory.createPolyType( PolyType.NULL );
        }
        if ( value instanceof Float || value instanceof Double ) {
            return typeFactory.createPolyType( PolyType.DOUBLE );
        }
        if ( value instanceof Number ) {
            return typeFactory.createPolyType( PolyType.BIGINT );
        }
        if ( value instanceof Boolean ) {
            return typeFactory.createPolyType( PolyType.BOOLEAN );
        }
        if ( value instanceof String ) {
            return typeFactory.createPolyType( PolyType.CHAR, ((String) value).length() );
        }
        if ( value instanceof ByteString ) {
            return typeFactory.createPolyType( PolyType.BINARY, ((ByteString) value).length() );
        }
        throw new AssertionError( "unknown type " + value.getClass() );
    }


    /**
     * Returns an {@link NlsString} with spaces to make it at least a given length.
     */
    private static NlsString padRight( NlsString s, int length ) {
        if ( s.getValue().length() >= length ) {
            return s;
        }
        return s.copy( padRight( s.getValue(), length ) );
    }


    /**
     * Returns a string padded with spaces to make it at least a given length.
     */
    private static String padRight( String s, int length ) {
        if ( s.length() >= length ) {
            return s;
        }
        return new StringBuilder()
                .append( s )
                .append( Spaces.MAX, s.length(), length )
                .toString();
    }


    /**
     * Returns a byte-string padded with zero bytes to make it at least a given length,
     */
    private static ByteString padRight( ByteString s, int length ) {
        if ( s.length() >= length ) {
            return s;
        }
        return new ByteString( Arrays.copyOf( s.getBytes(), length ) );
    }


    public RexLiteral makeArray( AlgDataType type, List<RexNode> operands ) {
        return new RexLiteral( PolyList.ofArray( operands ), type, type.getPolyType() );
    }


    public RexLiteral makeMap( AlgDataType type, Map<RexNode, RexNode> operands ) {
        return new RexLiteral( PolyMap.ofMap( operands ), type, type.getPolyType() );
    }


    public RexLiteral makeMapFromBson( AlgDataType type, Map<String, BsonValue> bson ) {
        @SuppressWarnings("RedundantCast") // seems necessary
        PolyMap<RexLiteral, RexLiteral> map = PolyMap.of( (Map<RexLiteral, RexLiteral>) bson.entrySet().stream().collect( Collectors.toMap( e -> makeLiteral( e.getKey() ), e -> BsonUtil.getAsLiteral( e.getValue(), this ) ) ) );
        return new RexLiteral( map, type, PolyType.CHAR );
    }


    public RexCall makeLpgExtract( String key ) {
        return new RexCall(
                typeFactory.createPolyType( PolyType.VARCHAR, 255 ),
                OperatorRegistry.get( QueryLanguage.CYPHER, OperatorName.CYPHER_EXTRACT_PROPERTY ),
                List.of( makeInputRef( typeFactory.createPolyType( PolyType.NODE ), 0 ), makeLiteral( key ) ) );
    }


    public RexCall makeLpgGetId() {
        return new RexCall(
                typeFactory.createPolyType( PolyType.VARCHAR, 255 ),
                OperatorRegistry.get( QueryLanguage.CYPHER, OperatorName.CYPHER_EXTRACT_ID ),
                List.of( makeInputRef( typeFactory.createPolyType( PolyType.NODE ), 0 ) ) );
    }


    public RexCall makeLpgPropertiesExtract() {
        return new RexCall(
                typeFactory.createPolyType( PolyType.VARCHAR, 255 ),
                OperatorRegistry.get( QueryLanguage.CYPHER, OperatorName.CYPHER_EXTRACT_PROPERTIES ),
                List.of( makeInputRef( typeFactory.createPolyType( PolyType.NODE ), 0 ) ) );
    }


    public RexCall makeLpgLabels() {
        return new RexCall(
                typeFactory.createArrayType( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), -1 ),
                OperatorRegistry.get( QueryLanguage.CYPHER, OperatorName.CYPHER_EXTRACT_LABELS ),
                List.of( makeInputRef( typeFactory.createPolyType( PolyType.NODE ), 0 ) ) );
    }

}


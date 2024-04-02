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

package org.polypheny.db.algebra.enumerable;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.linq4j.tree.UnaryExpression;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.functions.Functions;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexElementRef;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.category.PolyTemporal;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.ControlFlowException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Translates {@link RexNode REX expressions} to
 * {@link Expression linq4j expressions}.
 */
@Slf4j
public class RexToLixTranslator {

    public static final Map<Method, Operator> JAVA_TO_SQL_METHOD_MAP =
            Util.mapOf(
                    findMethod( String.class, "toUpperCase" ), OperatorRegistry.get( OperatorName.UPPER ),
                    findMethod( Functions.class, "substring", PolyString.class, PolyNumber.class, PolyNumber.class ), OperatorRegistry.get( OperatorName.SUBSTRING ),
                    findMethod( Functions.class, "charLength", PolyString.class ), OperatorRegistry.get( OperatorName.CHARACTER_LENGTH ),
                    findMethod( Functions.class, "charLength", PolyString.class ), OperatorRegistry.get( OperatorName.CHAR_LENGTH ),
                    findMethod( Functions.class, "translate3", String.class, String.class, String.class ), OperatorRegistry.get( OperatorName.ORACLE_TRANSLATE3 ) );

    final JavaTypeFactory typeFactory;
    final RexBuilder builder;
    private final RexProgram program;
    final Conformance conformance;
    @Getter
    private final Expression root;
    private final RexToLixTranslator.InputGetter inputGetter;
    @Getter
    private final BlockBuilder list;
    private final Map<? extends RexNode, Boolean> exprNullableMap;
    private final RexToLixTranslator parent;
    private final Function1<String, InputGetter> correlates;
    @Setter
    private boolean doSubstitute;
    @Getter
    private final Map<RexNode, Expression> replace;


    public RexToLixTranslator(
            RexProgram program,
            JavaTypeFactory typeFactory,
            Expression root,
            InputGetter inputGetter,
            BlockBuilder list,
            Map<? extends RexNode, Boolean> nullable,
            RexBuilder builder,
            Conformance conformance,
            RexToLixTranslator parent,
            Function1<String, InputGetter> correlates ) {
        this( program, typeFactory, root, inputGetter, list, nullable, builder, conformance, parent, correlates, new HashMap<>() );
    }


    private static Method findMethod( Class<?> clazz, String name, Class<?>... parameterTypes ) {
        try {
            return clazz.getMethod( name, parameterTypes );
        } catch ( NoSuchMethodException e ) {
            throw new RuntimeException( e );
        }
    }


    private RexToLixTranslator(
            @Nullable RexProgram program,
            JavaTypeFactory typeFactory,
            Expression root,
            InputGetter inputGetter,
            BlockBuilder list,
            Map<? extends RexNode, Boolean> exprNullableMap,
            RexBuilder builder,
            Conformance conformance,
            @Nullable RexToLixTranslator parent,
            @Nullable Function1<String, InputGetter> correlates,
            Map<RexNode, Expression> replace ) {
        this.program = program;
        this.typeFactory = Objects.requireNonNull( typeFactory );
        this.conformance = Objects.requireNonNull( conformance );
        this.root = Objects.requireNonNull( root );
        this.inputGetter = inputGetter;
        this.list = Objects.requireNonNull( list );
        this.exprNullableMap = Objects.requireNonNull( exprNullableMap );
        this.builder = Objects.requireNonNull( builder );
        this.parent = parent;
        this.correlates = correlates;
        this.replace = replace;
        this.doSubstitute = !replace.isEmpty();
    }


    /**
     * Translates a {@link RexProgram} to a sequence of expressions and declarations.
     *
     * @param program Program to be translated
     * @param typeFactory Type factory
     * @param conformance SQL conformance
     * @param list List of statements, populated with declarations
     * @param outputPhysType Output type, or null
     * @param root Root expression
     * @param inputGetter Generates expressions for inputs
     * @param correlates Provider of references to the values of correlated variables
     * @return Sequence of expressions, optional condition
     */
    public static List<Expression> translateProjects(
            RexProgram program, JavaTypeFactory typeFactory, Conformance conformance, BlockBuilder list, PhysType outputPhysType, Expression root,
            InputGetter inputGetter, Function1<String, InputGetter> correlates ) {
        List<Type> storageTypes = null;
        if ( outputPhysType != null ) {
            final AlgDataType rowType = outputPhysType.getTupleType();
            storageTypes = new ArrayList<>( rowType.getFieldCount() );
            for ( int i = 0; i < rowType.getFieldCount(); i++ ) {
                storageTypes.add( outputPhysType.getJavaFieldType( i ) );
            }
        }
        return new RexToLixTranslator( program, typeFactory, root, inputGetter, list, Collections.emptyMap(), new RexBuilder( typeFactory ), conformance, null, correlates )
                .translateList( program.getProjectList(), storageTypes );
    }


    /**
     * Creates a translator for translating aggregate functions.
     */
    public static RexToLixTranslator forAggregation( JavaTypeFactory typeFactory, BlockBuilder list, InputGetter inputGetter, Conformance conformance ) {
        final ParameterExpression root = DataContext.ROOT;
        return new RexToLixTranslator( null, typeFactory, root, inputGetter, list, Collections.emptyMap(), new RexBuilder( typeFactory ), conformance, null, null );
    }


    Expression translate( RexNode expr ) {
        final RexImpTable.NullAs nullAs = RexImpTable.NullAs.of( isNullable( expr ) );
        return translate( expr, nullAs );
    }


    Expression translate( RexNode expr, RexImpTable.NullAs nullAs ) {
        return translate( expr, nullAs, null );
    }


    Expression translate( RexNode expr, Type storageType ) {
        final RexImpTable.NullAs nullAs = RexImpTable.NullAs.of( isNullable( expr ) );
        return translate( expr, nullAs, storageType );
    }


    Expression translate( RexNode expr, RexImpTable.NullAs nullAs, Type storageType ) {
        Expression expression = translate0( expr, nullAs, storageType );
        expression = EnumUtils.enforce( storageType, expression );
        assert expression != null;
        return list.append( "v", expression );
    }


    Expression translateCast( AlgDataType sourceType, AlgDataType targetType, Expression operand ) {
        Expression convert = null;
        convert = switch ( targetType.getPolyType() ) {
            case ANY -> operand;
            case DATE -> switch ( sourceType.getPolyType() ) {
                case CHAR, VARCHAR -> Expressions.call( BuiltInMethod.STRING_TO_DATE.method, operand );
                case TIMESTAMP -> Expressions.call( PolyDate.class, "of", Expressions.call( operand, "LongValue" ) );
                default -> convert;
            };
            case TIME -> switch ( sourceType.getPolyType() ) {
                case CHAR, VARCHAR -> Expressions.call( BuiltInMethod.STRING_TO_TIME.method, operand );
                case TIMESTAMP -> Expressions.call( PolyTime.class, "of",
                        Expressions.call(
                                BuiltInMethod.FLOOR_MOD.method,
                                Expressions.call( operand, BuiltInMethod.MILLIS_SINCE_EPOCH_POLY.method ),
                                PolyLong.of( DateTimeUtils.MILLIS_PER_DAY ).asExpression() ) );
                default -> convert;
            };
            case TIMESTAMP -> switch ( sourceType.getPolyType() ) {
                case CHAR, VARCHAR -> Expressions.call( BuiltInMethod.STRING_TO_TIMESTAMP.method, operand );
                case DATE -> Expressions.call( PolyTimestamp.class, "of", Expressions.multiply(
                        Expressions.call( operand, BuiltInMethod.MILLIS_SINCE_EPOCH_POLY.method ),
                        PolyTemporal.MILLIS_OF_DAY ) );
                case TIME -> Expressions.call( PolyTimestamp.class, "of", Expressions.add(
                        Expressions.multiply(
                                Expressions.convert_( Expressions.call( BuiltInMethod.CURRENT_DATE.method, root ), long.class ),
                                PolyTemporal.MILLIS_OF_DAY ),
                        Expressions.call( operand, BuiltInMethod.MILLIS_SINCE_EPOCH_POLY.method ) ) );
                default -> convert;
            };
            case BOOLEAN -> switch ( sourceType.getPolyType() ) {
                case CHAR, VARCHAR -> Expressions.call(
                        BuiltInMethod.STRING_TO_BOOLEAN.method,
                        operand );
                default -> convert;
            };
            case CHAR, VARCHAR -> {
                final IntervalQualifier interval = sourceType.getIntervalQualifier();
                yield switch ( sourceType.getPolyType() ) {
                    case DATE -> RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                    BuiltInMethod.UNIX_DATE_TO_STRING.method,
                                    operand ) );
                    case TIME -> RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                    BuiltInMethod.UNIX_TIME_TO_STRING.method,
                                    operand ) );
                    case TIMESTAMP -> RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                    BuiltInMethod.UNIX_TIMESTAMP_TO_STRING.method,
                                    operand ) );
                    case INTERVAL -> RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                    BuiltInMethod.INTERVAL_YEAR_MONTH_TO_STRING.method,
                                    operand,
                                    Expressions.constant( interval.getTimeUnitRange() ) ) );
                    case BOOLEAN -> RexImpTable.optimize2(
                            operand,
                            Expressions.call(
                                    BuiltInMethod.BOOLEAN_TO_STRING.method,
                                    operand ) );
                    default -> convert;
                };
            }
            default -> null;
        };
        if ( convert == null ) {
            convert = convert( operand, typeFactory.getJavaClass( targetType ) );
        }
        // Going from anything to CHAR(n) or VARCHAR(n), make sure value is no longer than n.
        boolean pad = false;
        boolean truncate = true;
        switch ( targetType.getPolyType() ) {
            case CHAR:
            case BINARY:
                pad = true;
                // fall through
            case VARCHAR:
            case VARBINARY:
                final int targetPrecision = targetType.getPrecision();
                if ( targetPrecision >= 0 ) {
                    switch ( sourceType.getPolyType() ) {
                        case CHAR:
                        case VARCHAR:
                        case BINARY:
                        case VARBINARY:
                            // If this is a widening cast, no need to truncate.
                            final int sourcePrecision = sourceType.getPrecision();
                            if ( PolyTypeUtil.comparePrecision( sourcePrecision, targetPrecision ) <= 0 ) {
                                truncate = false;
                            }
                            // If this is a widening cast, no need to pad.
                            if ( PolyTypeUtil.comparePrecision( sourcePrecision, targetPrecision ) >= 0 ) {
                                pad = false;
                            }
                            // fall through
                        default:
                            if ( truncate || pad ) {
                                convert = Expressions.call(
                                        pad ? BuiltInMethod.TRUNCATE_OR_PAD.method : BuiltInMethod.TRUNCATE.method,
                                        convert,
                                        Expressions.constant( targetPrecision ) );
                            }
                    }
                }
                break;
            case TIMESTAMP:
                int targetScale = targetType.getScale();
                if ( targetScale == AlgDataType.SCALE_NOT_SPECIFIED ) {
                    targetScale = 0;
                }
                if ( targetScale < sourceType.getScale() ) {
                    convert = Expressions.call(
                            BuiltInMethod.ROUND_LONG.method,
                            convert,
                            Expressions.constant( (long) Math.pow( 10, 3 - targetScale ) ) );
                }
                break;
            case INTERVAL:
                if ( Objects.requireNonNull( sourceType.getPolyType().getFamily() ) == PolyTypeFamily.NUMERIC ) {
                    final BigDecimal multiplier = targetType.getPolyType().getEndUnit().multiplier;
                    final BigDecimal divider = BigDecimal.ONE;
                    convert = RexImpTable.multiplyDivide( convert, multiplier, divider );
                }
        }
        return scaleIntervalToNumber( sourceType, targetType, convert );
    }


    private Expression handleNullUnboxingIfNecessary( Expression input, RexImpTable.NullAs nullAs, Type storageType ) {
        if ( RexImpTable.NullAs.NOT_POSSIBLE == nullAs && input.type.equals( storageType ) ) {
            // When we asked for not null input that would be stored as box, avoid unboxing which may occur in the handleNull method below.
            return input;
        }
        return handleNull( input, nullAs );
    }


    /**
     * Adapts an expression with "normal" result to one that adheres to this particular policy. Wraps the result expression into a new parameter if need be.
     *
     * @param input Expression
     * @param nullAs If false, if expression is definitely not null at runtime. Therefore we can optimize. For example, we can cast to int using x.intValue().
     * @return Translated expression
     */
    public Expression handleNull( Expression input, RexImpTable.NullAs nullAs ) {
        final Expression nullHandled = nullAs.handle( input );

        // If we get ConstantExpression, just return it (i.e. primitive false)
        if ( nullHandled instanceof ConstantExpression ) {
            return nullHandled;
        }

        // if nullHandled expression is the same as "input", then we can just reuse it
        if ( nullHandled == input ) {
            return input;
        }

        // If nullHandled is different, then it might be unsafe to compute early (i.e. unbox of null value should not happen _before_ ternary).
        // Thus we wrap it into brand-new ParameterExpression, and we are guaranteed that ParameterExpression will not be shared
        String unboxVarName = "v_unboxed";
        if ( input instanceof ParameterExpression ) {
            unboxVarName = ((ParameterExpression) input).name + "_unboxed";
        }
        ParameterExpression unboxed = Expressions.parameter( nullHandled.getType(), list.newName( unboxVarName ) );
        list.add( Expressions.declare( Modifier.FINAL, unboxed, nullHandled ) );

        return unboxed;
    }


    /**
     * Translates an expression that is not in the cache.
     *
     * @param expr Expression
     * @param nullAs If false, if expression is definitely not null at runtime. Therefore we can optimize. For example, we can cast to int using x.intValue().
     * @return Translated expression
     */
    private Expression translate0( RexNode expr, RexImpTable.NullAs nullAs, Type storageType ) {
        if ( nullAs == RexImpTable.NullAs.NULL && !expr.getType().isNullable() ) {
            nullAs = RexImpTable.NullAs.NOT_POSSIBLE;
        }
        switch ( expr.getKind() ) {
            case INPUT_REF: {
                final int index = ((RexIndexRef) expr).getIndex();
                Expression x = inputGetter.field( list, index, storageType );
                Expression input = list.append( "inp" + index + "_", x ); // safe to share
                return handleNullUnboxingIfNecessary( input, nullAs, storageType );
            }
            case LOCAL_REF:
                if ( doSubstitute && replace.containsKey( expr ) ) {
                    return replace.get( expr );
                }
                return translate( deref( expr ), nullAs, storageType );
            case LITERAL:
                return translateLiteral(
                        (RexLiteral) expr,
                        nullifyType( expr.getType(), isNullable( expr ) && nullAs != RexImpTable.NullAs.NOT_POSSIBLE ),
                        typeFactory,
                        nullAs );
            case DYNAMIC_PARAM:
                return translateParameter( (RexDynamicParam) expr, nullAs, storageType );
            case CORREL_VARIABLE:
                throw new RuntimeException( "Cannot translate " + expr + ". Correlated variables should always be referenced by field access" );
            case FIELD_ACCESS: {
                RexFieldAccess fieldAccess = (RexFieldAccess) expr;
                RexNode target = deref( fieldAccess.getReferenceExpr() );
                int fieldIndex = fieldAccess.getField().getIndex();
                String fieldName = fieldAccess.getField().getName();
                if ( Objects.requireNonNull( target.getKind() ) == Kind.CORREL_VARIABLE ) {
                    if ( correlates == null ) {
                        throw new GenericRuntimeException( "Cannot translate " + expr + " since correlate variables resolver is not defined" );
                    }

                    if ( target.getType().getPolyType() == PolyType.DOCUMENT ) {
                        return translate( builder.makeCall( target.getType(), OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_QUERY_VALUE ), RexIndexRef.of( 0, target.getType() ),
                                builder.makeArray( builder.getTypeFactory().createArrayType( builder.getTypeFactory().createPolyType( PolyType.CHAR, 255 ), -1 ),
                                        PolyList.copyOf( Arrays.stream( fieldName.split( "\\." ) ).map( PolyString::of ).collect( Collectors.toList() ) ) ) ) );
                    }

                    InputGetter getter = correlates.apply( ((RexCorrelVariable) target).getName() );
                    Expression y = getter.field( list, fieldIndex, storageType );
                    Expression input = list.append( "corInp" + fieldIndex + "_", y ); // safe to share
                    return handleNullUnboxingIfNecessary( input, nullAs, storageType );
                }
                RexNode rxIndex = builder.makeLiteral( fieldIndex, typeFactory.createType( int.class ), true );
                RexNode rxName = builder.makeLiteral( fieldName, typeFactory.createType( String.class ), true );
                RexCall accessCall = (RexCall) builder.makeCall( fieldAccess.getType(), OperatorRegistry.get( OperatorName.STRUCT_ACCESS ), ImmutableList.of( target, rxIndex, rxName ) );
                return translateCall( accessCall, nullAs );
            }
            case ELEMENT_REF:
                RexElementRef element = expr.unwrap( RexElementRef.class ).orElseThrow();
                return element.getExpression();

            default:
                if ( expr instanceof RexCall ) {
                    return translateCall( (RexCall) expr, nullAs );
                }
                throw new GenericRuntimeException( "cannot translate expression " + expr );
        }
    }


    /**
     * Dereferences an expression if it is a {@link RexLocalRef}.
     */
    public RexNode deref( RexNode expr ) {
        if ( expr instanceof RexLocalRef ref ) {
            final RexNode e2 = program.getExprList().get( ref.getIndex() );
            assert ref.getType().equals( e2.getType() );
            return e2;
        } else {
            return expr;
        }
    }


    /**
     * Translates a call to an operator or function.
     */
    private Expression translateCall( RexCall call, RexImpTable.NullAs nullAs ) {
        final Operator operator = call.getOperator();
        CallImplementor implementor = RexImpTable.INSTANCE.get( operator );
        if ( implementor == null ) {
            throw new GenericRuntimeException( "cannot translate call " + call );
        }
        return implementor.implement( this, call, nullAs );
    }


    /**
     * Translates a parameter.
     */
    private Expression translateParameter( RexDynamicParam expr, RexImpTable.NullAs nullAs, Type storageType ) {
        if ( storageType == null ) {
            storageType = typeFactory.getJavaClass( expr.getType() );
        }
        return nullAs.handle(
                convert(
                        Expressions.call(
                                root,
                                BuiltInMethod.DATA_CONTEXT_GET_PARAMETER_VALUE.method,
                                Expressions.constant( expr.getIndex() ) ),
                        storageType ) );
    }


    /**
     * Translates a literal.
     *
     * @throws AlwaysNull if literal is null but {@code nullAs} is {@link RexImpTable.NullAs#NOT_POSSIBLE}.
     */
    public static Expression translateLiteral( RexLiteral literal, AlgDataType type, AlgDataTypeFactory typeFactory, RexImpTable.NullAs nullAs ) {
        if ( literal.isNull() ) {
            return switch ( nullAs ) {
                case TRUE, IS_NULL -> RexImpTable.TRUE_EXPR;
                case FALSE, IS_NOT_NULL -> RexImpTable.FALSE_EXPR;
                case NOT_POSSIBLE -> throw AlwaysNull.INSTANCE;
                default -> RexImpTable.NULL_EXPR;
            };
        } else {
            switch ( nullAs ) {
                case IS_NOT_NULL:
                    return RexImpTable.TRUE_EXPR;
                case IS_NULL:
                    return RexImpTable.FALSE_EXPR;
            }
        }
        return literal.value.asExpression();
    }


    public List<Expression> translateList( List<RexNode> operandList, RexImpTable.NullAs nullAs ) {
        // we use default storage type in this case
        return translateList( operandList, nullAs, operandList.stream().map( o -> (Type) null ).toList() );
    }


    public List<Expression> translateList( List<RexNode> operandList, RexImpTable.NullAs nullAs, List<? extends Type> storageTypes ) {
        final List<Expression> list = new ArrayList<>();
        for ( Pair<RexNode, ? extends Type> e : Pair.zip( operandList, storageTypes ) ) {
            list.add( translate( e.left, nullAs, e.right ) );
        }
        return list;
    }


    /**
     * Translates the list of {@code RexNode}, using the default output types. This might be suboptimal in terms of additional box-unbox when you use the translation later.
     * If you know the java class that will be used to store the results, use {@link RexToLixTranslator#translateList(java.util.List, java.util.List)} version.
     *
     * @param operandList list of RexNodes to translate
     * @return translated expressions
     */
    public List<Expression> translateList( List<? extends RexNode> operandList ) {
        // we use default storage type in this case
        return translateList( operandList, operandList.stream().map( o -> (Type) null ).toList() );
    }


    /**
     * Translates the list of {@code RexNode}, while optimizing for output storage. For instance, if the result of translation is going to be stored in {@code Object[]}, and the input is {@code Object[]} as well,
     * then translator will avoid casting, boxing, etc.
     *
     * @param operandList list of RexNodes to translate
     * @param storageTypes hints of the java classes that will be used to store translation results. Use null to use default storage type
     * @return translated expressions
     */
    public List<Expression> translateList( List<? extends RexNode> operandList, List<? extends Type> storageTypes ) {
        final List<Expression> list = new ArrayList<>( operandList.size() );

        for ( int i = 0; i < operandList.size(); i++ ) {
            RexNode rex = operandList.get( i );
            Type desiredType = null;
            if ( storageTypes != null ) {
                desiredType = storageTypes.get( i );
            }
            final Expression translate = translate( rex, desiredType );
            list.add( translate );
            // desiredType is still a hint, thus we might get any kind of output (boxed or not) when hint was provided.
            // It is favourable to get the type matching desired type
            assert desiredType != null || isNullable( rex ) || !Primitive.isBox( translate.getType() ) : "Not-null boxed primitive should come back as primitive: " + rex + ", " + translate.getType();
        }
        return list;
    }


    public static Expression translateCondition( RexProgram program, JavaTypeFactory typeFactory, BlockBuilder list, InputGetter inputGetter, Function1<String, InputGetter> correlates, Conformance conformance ) {
        if ( program.getCondition() == null ) {
            return RexImpTable.TRUE_EXPR;
        }
        final ParameterExpression root = DataContext.ROOT;
        RexToLixTranslator translator = new RexToLixTranslator( program, typeFactory, root, inputGetter, list, Collections.emptyMap(), new RexBuilder( typeFactory ), conformance, null, correlates );
        return translator.translate( program.getCondition(), RexImpTable.NullAs.FALSE );
    }


    public static Expression convert( Expression operand, Type toType ) {
        final Type fromType = operand.getType();
        return convert( operand, fromType, toType );
    }


    public static Expression convert( Expression operand, Type fromType, Type toType ) {
        if ( fromType.equals( toType ) ) {
            return operand;
        }
        // E.g. from "Short" to "int".
        // Generate "x.intValue()".
        final Primitive toPrimitive = Primitive.of( toType );
        final Primitive toBox = Primitive.ofBox( toType );
        final Primitive fromBox = Primitive.ofBox( fromType );
        final Primitive fromPrimitive = Primitive.of( fromType );
        final boolean fromNumber = fromType instanceof Class && Number.class.isAssignableFrom( (Class<?>) fromType );
        if ( fromType == String.class ) {
            if ( toPrimitive != null ) {
                return switch ( toPrimitive ) {
                    case CHAR, SHORT, INT, LONG, FLOAT, DOUBLE ->
                        // Generate "SqlFunctions.toShort(x)".
                            Expressions.call(
                                    Functions.class,
                                    "to" + Functions.initcap( toPrimitive.primitiveName ),
                                    operand );
                    default ->
                        // Generate "Short.parseShort(x)".
                            Expressions.call(
                                    toPrimitive.boxClass,
                                    "parse" + Functions.initcap( toPrimitive.primitiveName ),
                                    operand );
                };
            }
            if ( toBox != null ) {
                if ( toBox == Primitive.CHAR ) {// Generate "SqlFunctions.toCharBoxed(x)".
                    return Expressions.call(
                            Functions.class,
                            "to" + Functions.initcap( toBox.primitiveName ) + "Boxed",
                            operand );
                }// Generate "Short.valueOf(x)".
                return Expressions.call(
                        toBox.boxClass,
                        "valueOf",
                        operand );
            }
        }
        if ( toPrimitive != null ) {
            if ( fromPrimitive != null ) {
                // E.g. from "float" to "double"
                return Expressions.convert_( operand, toPrimitive.primitiveClass );
            }
            if ( fromNumber || fromBox == Primitive.CHAR ) {
                // Generate "x.shortValue()".
                return Expressions.unbox( operand, toPrimitive );
            } else {
                // E.g. from "Object" to "short".
                // Generate "SqlFunctions.toShort(x)"
                return Expressions.call(
                        Functions.class,
                        "to" + Functions.initcap( toPrimitive.primitiveName ),
                        operand );
            }
        } else if ( fromNumber && toBox != null ) {
            // E.g. from "Short" to "Integer"
            // Generate "x == null ? null : Integer.valueOf(x.intValue())"
            return EnumUtils.condition(
                    Expressions.equal( operand, RexImpTable.NULL_EXPR ),
                    RexImpTable.NULL_EXPR,
                    Expressions.box( Expressions.unbox( operand, toBox ), toBox ) );
        } else if ( fromPrimitive != null && toBox != null ) {
            // E.g. from "int" to "Long".
            // Generate Long.valueOf(x)
            // Eliminate primitive casts like Long.valueOf((long) x)
            if ( operand instanceof UnaryExpression una ) {
                if ( una.nodeType == ExpressionType.Convert || Primitive.of( una.getType() ) == toBox ) {
                    return Expressions.box( una.expression, toBox );
                }
            }
            return Expressions.box( operand, toBox );
        } else if ( fromType == java.sql.Date.class ) {
            if ( toBox == Primitive.INT ) {
                return Expressions.call( BuiltInMethod.DATE_TO_LONG.method, operand );
            } else {
                return Expressions.convert_( operand, toType );
            }
        } else if ( toType == java.sql.Date.class ) {
            // E.g. from "int" or "Integer" to "java.sql.Date",
            // generate "SqlFunctions.internalToDate".
            if ( isA( fromType, Primitive.INT ) ) {
                return Expressions.call( BuiltInMethod.INTERNAL_TO_DATE.method, operand );
            } else {
                return Expressions.convert_( operand, java.sql.Date.class );
            }
        } else if ( toType == java.sql.Time.class ) {
            // E.g. from "int" or "Integer" to "java.sql.Time",
            // generate "SqlFunctions.internalToTime".
            if ( isA( fromType, Primitive.INT ) ) {
                return Expressions.call( BuiltInMethod.INTERNAL_TO_TIME.method, operand );
            } else {
                return Expressions.convert_( operand, java.sql.Time.class );
            }
        } else if ( toType == java.sql.Timestamp.class ) {
            // E.g. from "long" or "Long" to "java.sql.Timestamp",
            // generate "SqlFunctions.internalToTimestamp".
            if ( isA( fromType, Primitive.LONG ) ) {
                return Expressions.call( BuiltInMethod.INTERNAL_TO_TIMESTAMP.method, operand );
            } else {
                return Expressions.convert_( operand, java.sql.Timestamp.class );
            }
        } else if ( toType == BigDecimal.class ) {
            if ( fromBox != null ) {
                // E.g. from "Integer" to "BigDecimal".
                // Generate "x == null ? null : new BigDecimal(x.intValue())"
                return EnumUtils.condition(
                        Expressions.equal( operand, RexImpTable.NULL_EXPR ),
                        RexImpTable.NULL_EXPR,
                        Expressions.new_( BigDecimal.class, Expressions.unbox( operand, fromBox ) ) );
            }
            if ( fromPrimitive != null ) {
                // E.g. from "int" to "BigDecimal".
                // Generate "new BigDecimal(x)"
                return Expressions.new_( BigDecimal.class, operand );
            }
            // E.g. from "Object" to "BigDecimal".
            // Generate "x == null ? null : SqlFunctions.toBigDecimal(x)"
            return EnumUtils.condition(
                    Expressions.equal( operand, RexImpTable.NULL_EXPR ),
                    RexImpTable.NULL_EXPR,
                    Expressions.call( Functions.class, "toBigDecimal", operand ) );
        } else if ( toType == String.class ) {
            if ( fromPrimitive != null ) {
                return switch ( fromPrimitive ) {
                    case DOUBLE, FLOAT ->
                        // E.g. from "double" to "String"
                        // Generate "SqlFunctions.toString(x)"
                            Expressions.call(
                                    Functions.class,
                                    "toString",
                                    operand );
                    default ->
                        // E.g. from "int" to "String"
                        // Generate "Integer.toString(x)"
                            Expressions.call(
                                    fromPrimitive.boxClass,
                                    "toString",
                                    operand );
                };
            } else if ( fromType == BigDecimal.class ) {
                // E.g. from "BigDecimal" to "String"
                // Generate "x.toString()"
                return EnumUtils.condition(
                        Expressions.equal( operand, RexImpTable.NULL_EXPR ),
                        RexImpTable.NULL_EXPR,
                        Expressions.call(
                                Functions.class,
                                "toString",
                                operand ) );
            } else {
                // E.g. from "BigDecimal" to "String"
                // Generate "x == null ? null : x.toString()"
                return EnumUtils.condition(
                        Expressions.equal( operand, RexImpTable.NULL_EXPR ),
                        RexImpTable.NULL_EXPR,
                        Expressions.call( operand, "toString" ) );
            }
        }
        if ( Types.isAssignableFrom( PolyValue.class, toType ) ) {
            operand = Expressions.convert_( operand, PolyValue.class );
            if ( toType == PolyNumber.class && !Types.isAssignableFrom( toType, operand.type ) ) {
                return Expressions.call( PolyBigDecimal.class, "convert", operand );
            } else if ( toType == PolyString.class ) {
                return Expressions.call( PolyString.class, "convert", operand );
            } else if ( toType == PolyBoolean.class ) {
                return Expressions.call( PolyBoolean.class, "convert", operand );
            } else if ( toType == PolyList.class ) {
                return Expressions.call( PolyList.class, "convert", operand );
            } else if ( toType == PolyDocument.class ) {
                return Expressions.call( PolyDocument.class, "convert", operand );
            } else if ( toType == PolyValue.class ) {
                return Expressions.convert_( operand, toType ); // document
            } else if ( toType == PolyNumber.class ) {
                return Expressions.convert_( operand, toType ); // number
            } else if ( toType == PolyDate.class ) {
                return Expressions.call( PolyDate.class, "convert", operand );
            } else if ( toType == PolyTimestamp.class ) {
                return Expressions.call( PolyTimestamp.class, "convert", operand );
            } else if ( toType == PolyTime.class ) {
                return Expressions.call( PolyTime.class, "convert", operand );
            }
            log.debug( "Converter missing " + toType );
        }

        return Expressions.convert_( operand, toType );
    }


    static boolean isA( Type fromType, Primitive primitive ) {
        return Primitive.of( fromType ) == primitive || Primitive.ofBox( fromType ) == primitive;
    }


    public Expression translateConstructor( List<RexNode> operandList, Kind kind ) {
        switch ( kind ) {
            case MAP_VALUE_CONSTRUCTOR:
                Expression map =
                        list.append(
                                "map",
                                Expressions.new_( LinkedHashMap.class ),
                                false );
                for ( int i = 0; i < operandList.size(); i++ ) {
                    RexNode key = operandList.get( i++ );
                    RexNode value = operandList.get( i );
                    list.add(
                            Expressions.statement(
                                    Expressions.call(
                                            map,
                                            BuiltInMethod.MAP_PUT.method,
                                            Expressions.box( translate( key ) ),
                                            Expressions.box( translate( value ) ) ) ) );
                }
                return map;
            case ARRAY_VALUE_CONSTRUCTOR:
                Expression lyst =
                        list.append(
                                "list",
                                Expressions.new_( ArrayList.class ),
                                false );
                for ( RexNode value : operandList ) {
                    list.add(
                            Expressions.statement(
                                    Expressions.call(
                                            lyst,
                                            BuiltInMethod.COLLECTION_ADD.method,
                                            Expressions.box( translate( value ) ) ) ) );
                }
                return lyst;
            default:
                throw new AssertionError( "unexpected: " + kind );
        }
    }


    /**
     * Returns whether an expression is nullable. Even if its type says it is nullable, if we have previously generated a check to make sure that it is not null, we will say so.
     *
     * For example, {@code WHERE a == b} translates to {@code a != null && b != null && a.equals(b)}. When translating the 3rd part of the disjunction, we already know a and b are not null.</p>
     *
     * @param e Expression
     * @return Whether expression is nullable in the current translation context
     */
    public boolean isNullable( RexNode e ) {
        if ( !e.getType().isNullable() ) {
            return false;
        }
        final Boolean b = isKnownNullable( e );
        return b == null || b;
    }


    /**
     * Walks parent translator chain and verifies if the expression is nullable.
     *
     * @param node RexNode to check if it is nullable or not
     * @return null when nullability is not known, true or false otherwise
     */
    protected Boolean isKnownNullable( RexNode node ) {
        if ( !exprNullableMap.isEmpty() ) {
            Boolean nullable = exprNullableMap.get( node );
            if ( nullable != null ) {
                return nullable;
            }
        }
        return parent == null ? null : parent.isKnownNullable( node );
    }


    /**
     * Creates a read-only copy of this translator that records that a given expression is nullable.
     */
    public RexToLixTranslator setNullable( RexNode e, boolean nullable ) {
        return setNullable( Collections.singletonMap( e, nullable ) );
    }


    /**
     * Creates a read-only copy of this translator that records that a given expression is nullable.
     */
    public RexToLixTranslator setNullable( Map<? extends RexNode, Boolean> nullable ) {
        if ( nullable == null || nullable.isEmpty() ) {
            return this;
        }
        return new RexToLixTranslator( program, typeFactory, root, inputGetter, list, nullable, builder, conformance, this, correlates, replace );
    }


    public RexToLixTranslator setBlock( BlockBuilder block ) {
        if ( block == list ) {
            return this;
        }
        return new RexToLixTranslator( program, typeFactory, root, inputGetter, block, ImmutableMap.of(), builder, conformance, this, correlates, replace );
    }


    public RexToLixTranslator setCorrelates( Function1<String, InputGetter> correlates ) {
        if ( this.correlates == correlates ) {
            return this;
        }
        return new RexToLixTranslator( program, typeFactory, root, inputGetter, list, Collections.emptyMap(), builder, conformance, this, correlates, replace );
    }


    private RexToLixTranslator withConformance( Conformance conformance ) {
        if ( conformance == this.conformance ) {
            return this;
        }
        return new RexToLixTranslator( program, typeFactory, root, inputGetter, list, Collections.emptyMap(), builder, conformance, this, correlates, replace );
    }


    public AlgDataType nullifyType( AlgDataType type, boolean nullable ) {
        if ( !nullable ) {
            final Primitive primitive = javaPrimitive( type );
            if ( primitive != null ) {
                return typeFactory.createJavaType( primitive.primitiveClass );
            }
        }
        return typeFactory.createTypeWithNullability( type, nullable );
    }


    private Primitive javaPrimitive( AlgDataType type ) {
        if ( type instanceof AlgDataTypeFactoryImpl.JavaType ) {
            return Primitive.ofBox( ((AlgDataTypeFactoryImpl.JavaType) type).getJavaClass() );
        }
        return null;
    }


    private static Expression scaleIntervalToNumber( AlgDataType sourceType, AlgDataType targetType, Expression operand ) {
        if ( Objects.requireNonNull( targetType.getPolyType().getFamily() ) == PolyTypeFamily.NUMERIC ) {
            if ( Objects.requireNonNull( sourceType.getPolyType() ) == PolyType.INTERVAL ) {// Scale to the given field.
                final BigDecimal multiplier = BigDecimal.ONE;
                final BigDecimal divider = sourceType.getPolyType().getEndUnit().multiplier;
                return RexImpTable.multiplyDivide( operand, multiplier, divider );
            }
        }
        return operand;
    }


    /**
     * Translates a field of an input to an expression.
     */
    public interface InputGetter {

        Expression field( BlockBuilder list, int index, Type storageType );

    }


    /**
     * Implementation of {@link InputGetter} that calls {@link PhysType#fieldReference}.
     */
    public static class InputGetterImpl implements InputGetter {

        private final List<Pair<Expression, PhysType>> inputs;


        public InputGetterImpl( List<Pair<Expression, PhysType>> inputs ) {
            this.inputs = inputs;
        }


        @Override
        public Expression field( BlockBuilder list, int index, Type storageType ) {
            int offset = 0;
            for ( Pair<Expression, PhysType> input : inputs ) {
                final PhysType physType = input.right;
                int fieldCount = physType.getTupleType().getFieldCount();
                if ( index >= offset + fieldCount ) {
                    offset += fieldCount;
                    continue;
                }
                final Expression left = list.append( "current", input.left );
                return physType.fieldReference( left, index - offset, storageType );
            }
            throw new IllegalArgumentException( "Unable to find field #" + index );
        }

    }


    /**
     * Thrown in the unusual (but not erroneous) situation where the expression we are translating is the null literal but we have already checked that it is not null. It is easier to throw (and caller will always handle)
     * than to check exhaustively beforehand.
     */
    static class AlwaysNull extends ControlFlowException {

        public static final AlwaysNull INSTANCE = new AlwaysNull();


        private AlwaysNull() {
        }

    }

}


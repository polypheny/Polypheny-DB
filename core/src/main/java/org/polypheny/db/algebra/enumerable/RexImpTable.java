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


import static org.apache.calcite.linq4j.tree.ExpressionType.Add;
import static org.apache.calcite.linq4j.tree.ExpressionType.AndAlso;
import static org.apache.calcite.linq4j.tree.ExpressionType.Divide;
import static org.apache.calcite.linq4j.tree.ExpressionType.GreaterThan;
import static org.apache.calcite.linq4j.tree.ExpressionType.GreaterThanOrEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.LessThanOrEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.Multiply;
import static org.apache.calcite.linq4j.tree.ExpressionType.Negate;
import static org.apache.calcite.linq4j.tree.ExpressionType.Not;
import static org.apache.calcite.linq4j.tree.ExpressionType.NotEqual;
import static org.apache.calcite.linq4j.tree.ExpressionType.OrElse;
import static org.apache.calcite.linq4j.tree.ExpressionType.Subtract;
import static org.apache.calcite.linq4j.tree.ExpressionType.UnaryPlus;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ConditionalStatement;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.OptimizeShuttle;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.fun.TrimFunction.Flag;
import org.polypheny.db.algebra.fun.TrimFunction.TrimFlagHolder;
import org.polypheny.db.algebra.fun.UserDefined;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.functions.Functions;
import org.polypheny.db.functions.MqlFunctions;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.BinaryOperator;
import org.polypheny.db.nodes.JsonAgg;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.TimeUnitRange;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.ImplementableAggFunction;
import org.polypheny.db.schema.ImplementableFunction;
import org.polypheny.db.schema.impl.AggregateFunctionImpl;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.temporal.DateTimeUtils;
import org.polypheny.db.util.temporal.TimeUnit;


/**
 * Contains implementations of Rex operators as Java code.
 */
@Slf4j
public class RexImpTable {

    public static final ConstantExpression NULL_EXPR = Expressions.constant( null );
    public static final Expression FALSE_EXPR = PolyBoolean.FALSE.asExpression();
    public static final Expression TRUE_EXPR = PolyBoolean.TRUE.asExpression();
    public static final Expression BOXED_FALSE_EXPR = PolyBoolean.FALSE.asExpression();
    public static final Expression BOXED_TRUE_EXPR = PolyBoolean.TRUE.asExpression();

    private final Map<Operator, CallImplementor> map = new HashMap<>();
    private final Map<AggFunction, Supplier<? extends AggImplementor>> aggMap = new HashMap<>();
    private final Map<AggFunction, Supplier<? extends WinAggImplementor>> winAggMap = new HashMap<>();


    RexImpTable() {
        defineMethod( OperatorRegistry.get( OperatorName.ROW ), BuiltInMethod.ARRAY.method, NullPolicy.ANY );
        defineMethod( OperatorRegistry.get( OperatorName.UPPER ), BuiltInMethod.UPPER.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.LOWER ), BuiltInMethod.LOWER.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.INITCAP ), BuiltInMethod.INITCAP.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.SUBSTRING ), BuiltInMethod.SUBSTRING.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.REPLACE ), BuiltInMethod.REPLACE.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.ORACLE_TRANSLATE3 ), BuiltInMethod.TRANSLATE3.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.CHARACTER_LENGTH ), BuiltInMethod.CHAR_LENGTH.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.CHAR_LENGTH ), BuiltInMethod.CHAR_LENGTH.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.CONCAT ), BuiltInMethod.STRING_CONCAT.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.OVERLAY ), BuiltInMethod.OVERLAY.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.POSITION ), BuiltInMethod.POSITION.method, NullPolicy.STRICT );

        final TrimImplementor trimImplementor = new TrimImplementor();
        defineImplementor( OperatorRegistry.get( OperatorName.TRIM ), NullPolicy.STRICT, trimImplementor, false );

        // logical
        defineBinary( OperatorRegistry.get( OperatorName.AND ), AndAlso, NullPolicy.AND, null );
        defineBinary( OperatorRegistry.get( OperatorName.OR ), OrElse, NullPolicy.OR, null );
        defineUnary( OperatorRegistry.get( OperatorName.NOT ), Not, NullPolicy.NOT );

        // comparisons
        defineMethod( OperatorRegistry.get( OperatorName.LESS_THAN ), "lt", NullPolicy.STRICT );
        defineBinary( OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ), LessThanOrEqual, NullPolicy.STRICT, "le" );
        defineBinary( OperatorRegistry.get( OperatorName.GREATER_THAN ), GreaterThan, NullPolicy.STRICT, "gt" );
        defineBinary( OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ), GreaterThanOrEqual, NullPolicy.STRICT, "ge" );
        defineMethod( OperatorRegistry.get( OperatorName.EQUALS ), "eq", NullPolicy.STRICT );
        defineBinary( OperatorRegistry.get( OperatorName.NOT_EQUALS ), NotEqual, NullPolicy.STRICT, "ne" );

        // arithmetic
        defineBinary( OperatorRegistry.get( OperatorName.PLUS ), Add, NullPolicy.STRICT, "plus" );
        defineBinary( OperatorRegistry.get( OperatorName.MINUS ), Subtract, NullPolicy.STRICT, "minus" );
        defineBinary( OperatorRegistry.get( OperatorName.MULTIPLY ), Multiply, NullPolicy.STRICT, "multiply" );
        defineBinary( OperatorRegistry.get( OperatorName.DIVIDE ), Divide, NullPolicy.STRICT, "divide" );
        defineBinary( OperatorRegistry.get( OperatorName.DIVIDE_INTEGER ), Divide, NullPolicy.STRICT, "divide" );
        defineUnary( OperatorRegistry.get( OperatorName.UNARY_MINUS ), Negate, NullPolicy.STRICT );
        defineUnary( OperatorRegistry.get( OperatorName.UNARY_PLUS ), UnaryPlus, NullPolicy.STRICT );

        defineMethod( OperatorRegistry.get( OperatorName.MOD ), "mod", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.EXP ), "exp", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.POWER ), "power", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.LN ), "ln", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.LOG10 ), "log10", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.ABS ), "abs", NullPolicy.STRICT );

        defineImplementor( OperatorRegistry.get( OperatorName.RAND ), NullPolicy.STRICT,
                new NotNullImplementor() {
                    final NotNullImplementor[] implementors = {
                            new ReflectiveCallNotNullImplementor( BuiltInMethod.RAND.method ),
                            new ReflectiveCallNotNullImplementor( BuiltInMethod.RAND_SEED.method )
                    };


                    @Override
                    public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
                        return implementors[call.getOperands().size()].implement( translator, call, translatedOperands );
                    }
                }, false );
        defineImplementor( OperatorRegistry.get( OperatorName.RAND_INTEGER ), NullPolicy.STRICT,
                new NotNullImplementor() {
                    final NotNullImplementor[] implementors = {
                            null,
                            new ReflectiveCallNotNullImplementor( BuiltInMethod.RAND_INTEGER.method ),
                            new ReflectiveCallNotNullImplementor( BuiltInMethod.RAND_INTEGER_SEED.method )
                    };


                    @Override
                    public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
                        return implementors[call.getOperands().size()].implement( translator, call, translatedOperands );
                    }
                }, false );

        defineMethod( OperatorRegistry.get( OperatorName.ACOS ), "acos", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.ASIN ), "asin", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.ATAN ), "atan", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.ATAN2 ), "atan2", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.COS ), "cos", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.COT ), "cot", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.DEGREES ), "degrees", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.RADIANS ), "radians", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.ROUND ), "sround", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.SIGN ), "sign", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.SIN ), "sin", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.TAN ), "tan", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.TRUNCATE ), "struncate", NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.DISTANCE ), "distance", NullPolicy.ANY );
        defineMethod( OperatorRegistry.get( OperatorName.META ), "meta", NullPolicy.ANY );

        map.put( OperatorRegistry.get( OperatorName.PI ), ( translator, call, nullAs ) -> PolyDouble.of( Math.PI ).asExpression() );

        // datetime
        defineImplementor( OperatorRegistry.get( OperatorName.DATETIME_PLUS ), NullPolicy.STRICT, new DatetimeArithmeticImplementor(), false );
        defineImplementor( OperatorRegistry.get( OperatorName.MINUS_DATE ), NullPolicy.STRICT, new DatetimeArithmeticImplementor(), false );
        defineImplementor( OperatorRegistry.get( OperatorName.EXTRACT ), NullPolicy.STRICT, new ExtractImplementor(), false );
        defineImplementor( OperatorRegistry.get( OperatorName.FLOOR ), NullPolicy.STRICT,
                new FloorImplementor(
                        BuiltInMethod.FLOOR.method.getName(),
                        BuiltInMethod.UNIX_TIMESTAMP_FLOOR.method,
                        BuiltInMethod.UNIX_DATE_FLOOR.method ),
                false );
        defineImplementor( OperatorRegistry.get( OperatorName.CEIL ), NullPolicy.STRICT,
                new FloorImplementor(
                        BuiltInMethod.CEIL.method.getName(),
                        BuiltInMethod.UNIX_TIMESTAMP_CEIL.method,
                        BuiltInMethod.UNIX_DATE_CEIL.method ),
                false );

        map.put( OperatorRegistry.get( OperatorName.IS_NULL ), new IsXxxImplementor( null, false ) );
        map.put( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), new IsXxxImplementor( null, true ) );
        map.put( OperatorRegistry.get( OperatorName.IS_TRUE ), new IsXxxImplementor( true, false ) );
        map.put( OperatorRegistry.get( OperatorName.IS_NOT_TRUE ), new IsXxxImplementor( true, true ) );
        map.put( OperatorRegistry.get( OperatorName.IS_FALSE ), new IsXxxImplementor( false, false ) );
        map.put( OperatorRegistry.get( OperatorName.IS_NOT_FALSE ), new IsXxxImplementor( false, true ) );

        // LIKE and SIMILAR
        final MethodImplementor likeImplementor = new MethodImplementor( BuiltInMethod.LIKE.method );
        defineImplementor( OperatorRegistry.get( OperatorName.LIKE ), NullPolicy.STRICT, likeImplementor, false );
        defineImplementor( OperatorRegistry.get( OperatorName.NOT_LIKE ), NullPolicy.STRICT, NotImplementor.of( likeImplementor ), false );
        final MethodImplementor similarImplementor = new MethodImplementor( BuiltInMethod.SIMILAR.method );
        defineImplementor( OperatorRegistry.get( OperatorName.SIMILAR_TO ), NullPolicy.STRICT, similarImplementor, false );
        defineImplementor( OperatorRegistry.get( OperatorName.NOT_SIMILAR_TO ), NullPolicy.STRICT, NotImplementor.of( similarImplementor ), false );

        // Multisets & arrays
        defineMethod( OperatorRegistry.get( OperatorName.CARDINALITY ), BuiltInMethod.COLLECTION_SIZE.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.SLICE ), BuiltInMethod.SLICE.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( OperatorName.ELEMENT ), BuiltInMethod.ELEMENT.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.STRUCT_ACCESS ), BuiltInMethod.STRUCT_ACCESS.method, NullPolicy.ANY );
        defineMethod( OperatorRegistry.get( OperatorName.MEMBER_OF ), BuiltInMethod.MEMBER_OF.method, NullPolicy.NONE );
        final MethodImplementor isEmptyImplementor = new MethodImplementor( BuiltInMethod.IS_EMPTY.method );
        defineImplementor( OperatorRegistry.get( OperatorName.IS_EMPTY ), NullPolicy.NONE, isEmptyImplementor, false );
        defineImplementor( OperatorRegistry.get( OperatorName.IS_NOT_EMPTY ), NullPolicy.NONE, NotImplementor.of( isEmptyImplementor ), false );
        final MethodImplementor isASetImplementor = new MethodImplementor( BuiltInMethod.IS_A_SET.method );
        defineImplementor( OperatorRegistry.get( OperatorName.IS_A_SET ), NullPolicy.NONE, isASetImplementor, false );
        defineImplementor( OperatorRegistry.get( OperatorName.IS_NOT_A_SET ), NullPolicy.NONE, NotImplementor.of( isASetImplementor ), false );
        defineMethod( OperatorRegistry.get( OperatorName.MULTISET_INTERSECT_DISTINCT ), BuiltInMethod.MULTISET_INTERSECT_DISTINCT.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( OperatorName.MULTISET_INTERSECT ), BuiltInMethod.MULTISET_INTERSECT_ALL.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( OperatorName.MULTISET_EXCEPT_DISTINCT ), BuiltInMethod.MULTISET_EXCEPT_DISTINCT.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( OperatorName.MULTISET_EXCEPT ), BuiltInMethod.MULTISET_EXCEPT_ALL.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( OperatorName.MULTISET_UNION_DISTINCT ), BuiltInMethod.MULTISET_UNION_DISTINCT.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( OperatorName.MULTISET_UNION ), BuiltInMethod.MULTISET_UNION_ALL.method, NullPolicy.NONE );
        final MethodImplementor subMultisetImplementor = new MethodImplementor( BuiltInMethod.SUBMULTISET_OF.method );
        defineImplementor( OperatorRegistry.get( OperatorName.SUBMULTISET_OF ), NullPolicy.NONE, subMultisetImplementor, false );
        defineImplementor( OperatorRegistry.get( OperatorName.NOT_SUBMULTISET_OF ), NullPolicy.NONE, NotImplementor.of( subMultisetImplementor ), false );

        map.put( OperatorRegistry.get( OperatorName.CASE ), new CaseImplementor() );
        map.put( OperatorRegistry.get( OperatorName.COALESCE ), new CoalesceImplementor() );
        map.put( OperatorRegistry.get( OperatorName.CAST ), new CastOptimizedImplementor() );

        defineImplementor( OperatorRegistry.get( OperatorName.REINTERPRET ), NullPolicy.STRICT, new ReinterpretImplementor(), false );

        final CallImplementor value = new ValueConstructorImplementor();
        map.put( OperatorRegistry.get( OperatorName.MAP_VALUE_CONSTRUCTOR ), value );
        map.put( OperatorRegistry.get( OperatorName.ARRAY_VALUE_CONSTRUCTOR ), value );
        map.put( OperatorRegistry.get( OperatorName.ITEM ), new ItemImplementor() );

        map.put( OperatorRegistry.get( OperatorName.DEFAULT ), ( translator, call, nullAs ) -> Expressions.constant( null ) );

        // Sequences
        defineMethod( OperatorRegistry.get( OperatorName.CURRENT_VALUE ), BuiltInMethod.SEQUENCE_CURRENT_VALUE.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.NEXT_VALUE ), BuiltInMethod.SEQUENCE_NEXT_VALUE.method, NullPolicy.STRICT );

        // Json Operators
        defineMethod( OperatorRegistry.get( OperatorName.JSON_VALUE_EXPRESSION ), BuiltInMethod.JSON_VALUE_EXPRESSION.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.JSON_VALUE_EXPRESSION_EXCLUDED ), BuiltInMethod.JSON_VALUE_EXPRESSION_EXCLUDE.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.JSON_STRUCTURED_VALUE_EXPRESSION ), BuiltInMethod.JSON_STRUCTURED_VALUE_EXPRESSION.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( OperatorName.JSON_API_COMMON_SYNTAX ), BuiltInMethod.JSON_API_COMMON_SYNTAX.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( OperatorName.JSON_EXISTS ), BuiltInMethod.JSON_EXISTS.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( OperatorName.JSON_VALUE_ANY ), BuiltInMethod.JSON_VALUE_ANY.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( OperatorName.JSON_QUERY ), BuiltInMethod.JSON_QUERY.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( OperatorName.JSON_OBJECT ), BuiltInMethod.JSON_OBJECT.method, NullPolicy.NONE );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.JSON_OBJECTAGG ), JsonObjectAggImplementor.supplierFor( BuiltInMethod.JSON_OBJECTAGG_ADD.method ) );
        defineMethod( OperatorRegistry.get( OperatorName.JSON_ARRAY ), BuiltInMethod.JSON_ARRAY.method, NullPolicy.NONE );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.JSON_ARRAYAGG ), JsonArrayAggImplementor.supplierFor( BuiltInMethod.JSON_ARRAYAGG_ADD.method ) );
        defineImplementor( OperatorRegistry.get( OperatorName.IS_JSON_VALUE ), NullPolicy.NONE, new MethodImplementor( BuiltInMethod.IS_JSON_VALUE.method ), false );
        defineImplementor( OperatorRegistry.get( OperatorName.IS_JSON_OBJECT ), NullPolicy.NONE, new MethodImplementor( BuiltInMethod.IS_JSON_OBJECT.method ), false );
        defineImplementor( OperatorRegistry.get( OperatorName.IS_JSON_ARRAY ), NullPolicy.NONE, new MethodImplementor( BuiltInMethod.IS_JSON_ARRAY.method ), false );
        defineImplementor( OperatorRegistry.get( OperatorName.IS_JSON_SCALAR ), NullPolicy.NONE, new MethodImplementor( BuiltInMethod.IS_JSON_SCALAR.method ), false );
        defineImplementor( OperatorRegistry.get( OperatorName.IS_NOT_JSON_VALUE ), NullPolicy.NONE, NotImplementor.of( new MethodImplementor( BuiltInMethod.IS_JSON_VALUE.method ) ), false );
        defineImplementor( OperatorRegistry.get( OperatorName.IS_NOT_JSON_OBJECT ), NullPolicy.NONE, NotImplementor.of( new MethodImplementor( BuiltInMethod.IS_JSON_OBJECT.method ) ), false );
        defineImplementor( OperatorRegistry.get( OperatorName.IS_NOT_JSON_ARRAY ), NullPolicy.NONE, NotImplementor.of( new MethodImplementor( BuiltInMethod.IS_JSON_ARRAY.method ) ), false );
        defineImplementor( OperatorRegistry.get( OperatorName.IS_NOT_JSON_SCALAR ), NullPolicy.NONE, NotImplementor.of( new MethodImplementor( BuiltInMethod.IS_JSON_SCALAR.method ) ), false );

        // Mongo functions
        if ( QueryLanguage.containsLanguage( "mongo" ) ) {
            defineMongoMethods();
        }

        // Cypher functions
        if ( QueryLanguage.containsLanguage( "cypher" ) ) {
            defineCypherMethods();
        }

        // Cross Model Sql
        defineMethod( OperatorRegistry.get( OperatorName.CROSS_MODEL_ITEM ), BuiltInMethod.X_MODEL_ITEM.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( OperatorName.TO_JSON ), BuiltInMethod.TO_JSON.method, NullPolicy.NONE );

        // System functions
        final SystemFunctionImplementor systemFunctionImplementor = new SystemFunctionImplementor();
        map.put( OperatorRegistry.get( OperatorName.USER ), systemFunctionImplementor );
        map.put( OperatorRegistry.get( OperatorName.CURRENT_USER ), systemFunctionImplementor );
        map.put( OperatorRegistry.get( OperatorName.SESSION_USER ), systemFunctionImplementor );
        map.put( OperatorRegistry.get( OperatorName.SYSTEM_USER ), systemFunctionImplementor );
        map.put( OperatorRegistry.get( OperatorName.CURRENT_PATH ), systemFunctionImplementor );
        map.put( OperatorRegistry.get( OperatorName.CURRENT_ROLE ), systemFunctionImplementor );
        map.put( OperatorRegistry.get( OperatorName.CURRENT_CATALOG ), systemFunctionImplementor );

        // Current time functions
        map.put( OperatorRegistry.get( OperatorName.CURRENT_TIME ), systemFunctionImplementor );
        map.put( OperatorRegistry.get( OperatorName.CURRENT_TIMESTAMP ), systemFunctionImplementor );
        map.put( OperatorRegistry.get( OperatorName.CURRENT_DATE ), systemFunctionImplementor );
        map.put( OperatorRegistry.get( OperatorName.LOCALTIME ), systemFunctionImplementor );
        map.put( OperatorRegistry.get( OperatorName.LOCALTIMESTAMP ), systemFunctionImplementor );

        aggMap.put( OperatorRegistry.getAgg( OperatorName.COUNT ), constructorSupplier( CountImplementor.class ) );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.REGR_COUNT ), constructorSupplier( CountImplementor.class ) );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.SUM0 ), constructorSupplier( SumImplementor.class ) );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.SUM ), constructorSupplier( SumImplementor.class ) );
        Supplier<MinMaxImplementor> minMax = constructorSupplier( MinMaxImplementor.class );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.MIN ), minMax );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.MAX ), minMax );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.ANY_VALUE ), minMax );
        final Supplier<BitOpImplementor> bitop = constructorSupplier( BitOpImplementor.class );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.BIT_AND ), bitop );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.BIT_OR ), bitop );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.SINGLE_VALUE ), constructorSupplier( SingleValueImplementor.class ) );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.COLLECT ), constructorSupplier( CollectImplementor.class ) );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.FUSION ), constructorSupplier( FusionImplementor.class ) );
        final Supplier<GroupingImplementor> grouping = constructorSupplier( GroupingImplementor.class );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.GROUPING ), grouping );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.GROUP_ID ), grouping );
        aggMap.put( OperatorRegistry.getAgg( OperatorName.GROUPING_ID ), grouping );
        winAggMap.put( OperatorRegistry.getAgg( OperatorName.RANK ), constructorSupplier( RankImplementor.class ) );
        winAggMap.put( OperatorRegistry.getAgg( OperatorName.DENSE_RANK ), constructorSupplier( DenseRankImplementor.class ) );
        winAggMap.put( OperatorRegistry.getAgg( OperatorName.ROW_NUMBER ), constructorSupplier( RowNumberImplementor.class ) );
        winAggMap.put( OperatorRegistry.getAgg( OperatorName.FIRST_VALUE ), constructorSupplier( FirstValueImplementor.class ) );
        winAggMap.put( OperatorRegistry.getAgg( OperatorName.NTH_VALUE ), constructorSupplier( NthValueImplementor.class ) );
        winAggMap.put( OperatorRegistry.getAgg( OperatorName.LAST_VALUE ), constructorSupplier( LastValueImplementor.class ) );
        winAggMap.put( OperatorRegistry.getAgg( OperatorName.LEAD ), constructorSupplier( LeadImplementor.class ) );
        winAggMap.put( OperatorRegistry.getAgg( OperatorName.LAG ), constructorSupplier( LagImplementor.class ) );
        winAggMap.put( OperatorRegistry.getAgg( OperatorName.NTILE ), constructorSupplier( NtileImplementor.class ) );
        winAggMap.put( OperatorRegistry.getAgg( OperatorName.COUNT ), constructorSupplier( CountWinImplementor.class ) );
        winAggMap.put( OperatorRegistry.getAgg( OperatorName.REGR_COUNT ), constructorSupplier( CountWinImplementor.class ) );
    }


    private void defineCypherMethods() {
        CypherImplementor implementor = new CypherImplementor();
        QueryLanguage cypher = QueryLanguage.from( "cypher" );
        map.put( OperatorRegistry.get( cypher, OperatorName.CYPHER_ALL_MATCH ), implementor );
        map.put( OperatorRegistry.get( cypher, OperatorName.CYPHER_ANY_MATCH ), implementor );
        map.put( OperatorRegistry.get( cypher, OperatorName.CYPHER_SINGLE_MATCH ), implementor );
        map.put( OperatorRegistry.get( cypher, OperatorName.CYPHER_NONE_MATCH ), implementor );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_LIKE ), BuiltInMethod.CYPHER_LIKE.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_PATH_MATCH ), BuiltInMethod.CYPHER_PATH_MATCH.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_NODE_EXTRACT ), BuiltInMethod.CYPHER_NODE_EXTRACT.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_EXTRACT_FROM_PATH ), BuiltInMethod.CYPHER_EXTRACT_FROM_PATH.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_NODE_MATCH ), BuiltInMethod.CYPHER_NODE_MATCH.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_HAS_LABEL ), BuiltInMethod.CYPHER_HAS_LABEL.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_HAS_PROPERTY ), BuiltInMethod.CYPHER_HAS_PROPERTY.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_EXTRACT_PROPERTY ), BuiltInMethod.CYPHER_EXTRACT_PROPERTY.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_EXTRACT_PROPERTIES ), BuiltInMethod.CYPHER_EXTRACT_PROPERTIES.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_EXTRACT_ID ), BuiltInMethod.CYPHER_EXTRACT_ID.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_EXTRACT_LABELS ), BuiltInMethod.CYPHER_EXTRACT_LABELS.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_EXTRACT_LABEL ), BuiltInMethod.CYPHER_EXTRACT_LABEL.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_TO_LIST ), BuiltInMethod.CYPHER_TO_LIST.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_ADJUST_EDGE ), BuiltInMethod.CYPHER_ADJUST_EDGE.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_SET_PROPERTY ), BuiltInMethod.CYPHER_SET_PROPERTY.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_SET_PROPERTIES ), BuiltInMethod.CYPHER_SET_PROPERTIES.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_SET_LABELS ), BuiltInMethod.CYPHER_SET_LABELS.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_REMOVE_LABELS ), BuiltInMethod.CYPHER_REMOVE_LABELS.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_REMOVE_PROPERTY ), BuiltInMethod.CYPHER_REMOVE_PROPERTY.method, NullPolicy.NONE );
        defineMethod( OperatorRegistry.get( cypher, OperatorName.CYPHER_GRAPH_ONLY_LABEL ), BuiltInMethod.X_MODEL_GRAPH_ONLY_LABEL.method, NullPolicy.NONE );
    }


    private void defineMongoMethods() {
        QueryLanguage mongo = QueryLanguage.from( "mongo" );
        defineBinary( OperatorRegistry.get( mongo, OperatorName.MQL_ITEM ), ExpressionType.Parameter, NullPolicy.STRICT, "docItem" );
        defineImplementor( OperatorRegistry.get( mongo, OperatorName.MQL_EQUALS ), NullPolicy.NONE, new MethodImplementor( BuiltInMethod.MQL_EQ.method ), false );
        defineImplementor( OperatorRegistry.get( mongo, OperatorName.MQL_GT ), NullPolicy.NONE, new MethodImplementor( BuiltInMethod.MQL_GT.method ), false );
        defineImplementor( OperatorRegistry.get( mongo, OperatorName.MQL_GTE ), NullPolicy.NONE, new MethodImplementor( BuiltInMethod.MQL_GTE.method ), false );
        defineImplementor( OperatorRegistry.get( mongo, OperatorName.MQL_LT ), NullPolicy.NONE, new MethodImplementor( BuiltInMethod.MQL_LT.method ), false );
        defineImplementor( OperatorRegistry.get( mongo, OperatorName.MQL_LTE ), NullPolicy.NONE, new MethodImplementor( BuiltInMethod.MQL_LTE.method ), false );
        defineImplementor( OperatorRegistry.get( mongo, OperatorName.MQL_SIZE_MATCH ), NullPolicy.NONE, new MethodImplementor( BuiltInMethod.MQL_SIZE_MATCH.method ), false );
        defineImplementor( OperatorRegistry.get( mongo, OperatorName.MQL_REGEX_MATCH ), NullPolicy.NONE, new MethodImplementor( BuiltInMethod.MQL_REGEX_MATCH.method ), false );
        defineImplementor( OperatorRegistry.get( mongo, OperatorName.MQL_JSON_MATCH ), NullPolicy.NONE, new MethodImplementor( BuiltInMethod.MQL_JSON_MATCH.method ), false );
        defineImplementor( OperatorRegistry.get( mongo, OperatorName.MQL_TYPE_MATCH ), NullPolicy.NONE, new MethodImplementor( BuiltInMethod.MQL_TYPE_MATCH.method ), false );
        defineMethod( OperatorRegistry.get( mongo, OperatorName.MQL_SLICE ), BuiltInMethod.MQL_SLICE.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( mongo, OperatorName.MQL_QUERY_VALUE ), BuiltInMethod.MQL_QUERY_VALUE.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( mongo, OperatorName.MQL_ADD_FIELDS ), BuiltInMethod.MQL_ADD_FIELDS.method, NullPolicy.STRICT );

        defineMethod( OperatorRegistry.get( mongo, OperatorName.MQL_UPDATE_MIN ), BuiltInMethod.MQL_UPDATE_MIN.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( mongo, OperatorName.MQL_UPDATE_MAX ), BuiltInMethod.MQL_UPDATE_MAX.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( mongo, OperatorName.MQL_UPDATE_ADD_TO_SET ), BuiltInMethod.MQL_UPDATE_ADD_TO_SET.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( mongo, OperatorName.MQL_UPDATE_RENAME ), BuiltInMethod.MQL_UPDATE_RENAME.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( mongo, OperatorName.MQL_UPDATE_REPLACE ), BuiltInMethod.MQL_UPDATE_REPLACE.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( mongo, OperatorName.MQL_REMOVE ), BuiltInMethod.MQL_REMOVE.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( mongo, OperatorName.MQL_EXISTS ), BuiltInMethod.MQL_EXISTS.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( mongo, OperatorName.MQL_MERGE ), BuiltInMethod.MQL_MERGE.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( mongo, OperatorName.MQL_PROJECT_INCLUDES ), BuiltInMethod.MQL_PROJECT_INCLUDES.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( mongo, OperatorName.MQL_REPLACE_ROOT ), BuiltInMethod.MQL_REPLACE_ROOT.method, NullPolicy.STRICT );
        defineMethod( OperatorRegistry.get( mongo, OperatorName.MQL_NOT_UNSET ), BuiltInMethod.MQL_NOT_UNSET.method, NullPolicy.STRICT );

        defineMqlMethod( OperatorName.PLUS, "plus", NullPolicy.STRICT );
        defineMqlMethod( OperatorName.MINUS, "minus", NullPolicy.STRICT );
        defineMqlMethod( OperatorName.MULTIPLY, "multiply", NullPolicy.STRICT );
        defineMqlMethod( OperatorName.DIVIDE, "divide", NullPolicy.STRICT );

        map.put( OperatorRegistry.get( mongo, OperatorName.MQL_ELEM_MATCH ), new ElemMatchImplementor() );
    }


    private <T> Supplier<T> constructorSupplier( Class<T> klass ) {
        final Constructor<T> constructor;
        try {
            constructor = klass.getDeclaredConstructor();
        } catch ( NoSuchMethodException e ) {
            throw new IllegalArgumentException( klass + " should implement zero arguments constructor" );
        }
        return () -> {
            try {
                return constructor.newInstance();
            } catch ( InstantiationException | IllegalAccessException | InvocationTargetException e ) {
                throw new IllegalStateException( "Error while creating aggregate implementor " + constructor, e );
            }
        };
    }


    private void defineImplementor( Operator operator, NullPolicy nullPolicy, NotNullImplementor implementor, boolean harmonize ) {
        CallImplementor callImplementor = createImplementor( implementor, nullPolicy, harmonize );
        map.put( operator, callImplementor );
    }


    private static RexCall call2( boolean harmonize, RexToLixTranslator translator, RexCall call ) {
        if ( !harmonize ) {
            return call;
        }
        final List<RexNode> operands2 = harmonize( translator, call.getOperands() );
        if ( operands2.equals( call.getOperands() ) ) {
            return call;
        }
        return call.clone( call.getType(), operands2 );
    }


    public static CallImplementor createImplementor( final NotNullImplementor implementor, final NullPolicy nullPolicy, final boolean harmonize ) {
        return switch ( nullPolicy ) {
            case ANY, STRICT, SEMI_STRICT -> ( translator, call, nullAs ) -> implementNullSemantics0( translator, call, nullAs, nullPolicy, harmonize, implementor );
            case AND ->
                /* TODO:
                if (nullAs == NullAs.FALSE) {
                    nullPolicy2 = NullPolicy.ANY;
                }
                */
                // If any of the arguments are false, result is false;
                // else if any arguments are null, result is null;
                // else true.
                //
                // b0 == null ? (b1 == null || b1 ? null : Boolean.FALSE)
                //   : b0 ? b1
                //   : Boolean.FALSE;
                    ( translator, call, nullAs ) -> {
                        assert call.getOperator().getOperatorName() == OperatorName.AND : "AND null semantics is supported only for AND operator. Actual operator is " + call.getOperator();
                        final RexCall call2 = call2( false, translator, call );
                        switch ( nullAs ) {
                            case NOT_POSSIBLE:
                                // This doesn't mean that none of the arguments might be null, ex: (s and s is not null)
                                nullAs = NullAs.TRUE;
                                // fallthru
                            case TRUE:
                                // AND call should return false iff has FALSEs, thus if we convert nulls to true then no harm is made
                            case FALSE:
                                // AND call should return false iff has FALSEs or has NULLs, thus if we convert nulls to false, no harm is made
                                final List<Expression> expressions = translator.translateList( call2.getOperands(), nullAs );
                                return EnumUtils.foldAnd( expressions );
                            case NULL:
                            case IS_NULL:
                            case IS_NOT_NULL:
                                final List<Expression> nullAsTrue = translator.translateList( call2.getOperands(), NullAs.TRUE );
                                final List<Expression> nullAsIsNull = translator.translateList( call2.getOperands(), NullAs.IS_NULL );
                                Expression hasFalse = EnumUtils.not( EnumUtils.foldAnd( nullAsTrue ) );
                                Expression hasNull = EnumUtils.foldOr( nullAsIsNull );
                                return nullAs.handle( EnumUtils.condition( hasFalse, BOXED_FALSE_EXPR, EnumUtils.condition( hasNull, NULL_EXPR, BOXED_TRUE_EXPR ) ) );
                            default:
                                throw new IllegalArgumentException( "Unknown nullAs when implementing AND: " + nullAs );
                        }
                    };
            case OR ->
                // If any of the arguments are true, result is true;
                // else if any arguments are null, result is null;
                // else false.
                //
                // b0 == null ? (b1 == null || !b1 ? null : Boolean.TRUE)
                //   : !b0 ? b1
                //   : Boolean.TRUE;
                    ( translator, call, nullAs ) -> {
                        assert call.getOperator().getOperatorName() == OperatorName.OR : "OR null semantics is supported only for OR operator. Actual operator is " + call.getOperator();
                        final RexCall call2 = call2( harmonize, translator, call );
                        switch ( nullAs ) {
                            case NOT_POSSIBLE:
                                // This doesn't mean that none of the arguments might be null, ex: (s or s is null)
                                nullAs = NullAs.FALSE;
                                // fallthru
                            case TRUE:
                                // This should return false iff all arguments are FALSE, thus we convert nulls to TRUE and foldOr
                            case FALSE:
                                // This should return true iff has TRUE arguments, thus we convert nulls to FALSE and foldOr
                                final List<Expression> expressions = translator.translateList( call2.getOperands(), nullAs );
                                return EnumUtils.foldOr( expressions );
                            case NULL:
                            case IS_NULL:
                            case IS_NOT_NULL:
                                final List<Expression> nullAsFalse = translator.translateList( call2.getOperands(), NullAs.FALSE );
                                final List<Expression> nullAsIsNull = translator.translateList( call2.getOperands(), NullAs.IS_NULL );
                                Expression hasTrue = EnumUtils.foldOr( nullAsFalse );
                                Expression hasNull = EnumUtils.foldOr( nullAsIsNull );
                                return nullAs.handle( EnumUtils.condition( hasTrue, BOXED_TRUE_EXPR, EnumUtils.condition( hasNull, NULL_EXPR, BOXED_FALSE_EXPR ) ) );
                            default:
                                throw new IllegalArgumentException( "Unknown nullAs when implementing OR: " + nullAs );
                        }
                    };
            case NOT ->
                // If any of the arguments are false, result is true;
                // else if any arguments are null, result is null;
                // else false.
                    new CallImplementor() {
                        @Override
                        public Expression implement( RexToLixTranslator translator, RexCall call, NullAs nullAs ) {
                            if ( Objects.requireNonNull( nullAs ) == NullAs.NULL ) {
                                return Expressions.call( BuiltInMethod.NOT.method, translator.translateList( call.getOperands(), nullAs ) );
                            }
                            return Expressions.not( translator.translate( call.getOperands().get( 0 ), negate( nullAs ) ) );
                        }


                        private NullAs negate( NullAs nullAs ) {
                            return switch ( nullAs ) {
                                case FALSE -> NullAs.TRUE;
                                case TRUE -> NullAs.FALSE;
                                default -> nullAs;
                            };
                        }
                    };
            case NONE -> ( translator, call, nullAs ) -> {
                final RexCall call2 = call2( false, translator, call );
                return implementCall( translator, call2, implementor, nullAs );
            };
        };
    }


    private void defineMethod( Operator operator, String functionName, NullPolicy nullPolicy ) {
        defineImplementor( operator, nullPolicy, new MethodNameImplementor( functionName ), false );
    }


    private void defineMqlMethod( OperatorName operator, String functionName, NullPolicy nullPolicy ) {
        defineImplementor( OperatorRegistry.get( QueryLanguage.from( "mongo" ), operator ), nullPolicy, new MqlMethodNameImplementor( functionName ), false );
    }


    private void defineMethod( Operator operator, Method method, NullPolicy nullPolicy ) {
        defineImplementor( operator, nullPolicy, new MethodImplementor( method ), false );
    }


    private void defineMethodReflective( Operator operator, Method method, NullPolicy nullPolicy ) {
        defineImplementor( operator, nullPolicy, new ReflectiveCallNotNullImplementor( method ), false );
    }


    private void defineUnary( Operator operator, ExpressionType expressionType, NullPolicy nullPolicy ) {
        defineImplementor( operator, nullPolicy, new UnaryImplementor( expressionType ), false );
    }


    private void defineBinary( Operator operator, ExpressionType expressionType, NullPolicy nullPolicy, String backupMethodName ) {
        defineImplementor( operator, nullPolicy, new BinaryImplementor( expressionType, backupMethodName ), true );
    }


    public static final RexImpTable INSTANCE = new RexImpTable();


    public CallImplementor get( final Operator operator ) {
        if ( operator instanceof UserDefined ) {
            Function udf = ((UserDefined) operator).getFunction();
            if ( !(udf instanceof ImplementableFunction) ) {
                throw new IllegalStateException( "User defined function " + operator + " must implement ImplementableFunction" );
            }
            return ((ImplementableFunction) udf).getImplementor();
        }
        return map.get( operator );
    }


    public AggImplementor get( final AggFunction aggregation, boolean forWindowAggregate ) {
        if ( aggregation instanceof UserDefined udaf ) {
            if ( !(udaf.getFunction() instanceof ImplementableAggFunction) ) {
                throw new IllegalStateException( "User defined aggregation " + aggregation + " must implement ImplementableAggFunction" );
            }
            return ((ImplementableAggFunction) udaf.getFunction()).getImplementor( forWindowAggregate );
        }
        if ( forWindowAggregate ) {
            Supplier<? extends WinAggImplementor> winAgg = winAggMap.get( aggregation );
            if ( winAgg != null ) {
                return winAgg.get();
            }
            // Regular aggregates can be used in window context as well
        }

        Supplier<? extends AggImplementor> aggSupplier = aggMap.get( aggregation );
        if ( aggSupplier == null ) {
            return null;
        }

        return aggSupplier.get();
    }


    static Expression maybeNegate( boolean negate, Expression expression ) {
        if ( !negate ) {
            return expression;
        } else {
            return EnumUtils.not( expression );
        }
    }


    static Expression optimize( Expression expression ) {
        return expression.accept( new OptimizeShuttle() );
    }


    static Expression optimize2( Expression operand, Expression expression ) {
        if ( Primitive.is( operand.getType() ) ) {
            // Primitive values cannot be null
            return optimize( expression );
        } else {
            return optimize( EnumUtils.condition( Expressions.equal( operand, NULL_EXPR ), NULL_EXPR, expression ) );
        }
    }


    private static boolean nullable( RexCall call, int i ) {
        return call.getOperands().get( i ).getType().isNullable();
    }


    /**
     * Ensures that operands have identical type.
     */
    private static List<RexNode> harmonize( final RexToLixTranslator translator, final List<RexNode> operands ) {
        int nullCount = 0;
        final List<AlgDataType> types = new ArrayList<>();
        final AlgDataTypeFactory typeFactory = translator.builder.getTypeFactory();
        for ( RexNode operand : operands ) {
            AlgDataType type = operand.getType();
            type = toSql( typeFactory, type );
            if ( translator.isNullable( operand ) ) {
                ++nullCount;
            } else {
                type = typeFactory.createTypeWithNullability( type, false );
            }
            types.add( type );
        }
        if ( allSame( types ) ) {
            // Operands have the same nullability and type. Return them unchanged.
            return operands;
        }
        final AlgDataType type = typeFactory.leastRestrictive( types );
        if ( type == null ) {
            // There is no common type. Presumably this is a binary operator with asymmetric arguments (e.g. interval / integer) which is not intended to be harmonized.
            return operands;
        }
        assert (nullCount > 0) == type.isNullable();
        final List<RexNode> list = new ArrayList<>();
        for ( RexNode operand : operands ) {
            list.add( translator.builder.ensureType( type, operand, false ) );
        }
        return list;
    }


    private static AlgDataType toSql( AlgDataTypeFactory typeFactory, AlgDataType type ) {
        if ( type instanceof AlgDataTypeFactoryImpl.JavaType ) {
            final PolyType typeName = type.getPolyType();
            if ( typeName != null && typeName != PolyType.OTHER ) {
                return typeFactory.createTypeWithNullability( typeFactory.createPolyType( typeName ), type.isNullable() );
            }
        }
        return type;
    }


    private static <E> boolean allSame( List<E> list ) {
        E prev = null;
        for ( E e : list ) {
            if ( prev != null && !prev.equals( e ) ) {
                return false;
            }
            prev = e;
        }
        return true;
    }


    private static Expression implementNullSemantics0( RexToLixTranslator translator, RexCall call, NullAs nullAs, NullPolicy nullPolicy, boolean harmonize, NotNullImplementor implementor ) {
        switch ( nullAs ) {
            case IS_NOT_NULL:
                // If "f" is strict, then "f(a0, a1) IS NOT NULL" is equivalent to "a0 IS NOT NULL AND a1 IS NOT NULL".
                if ( Objects.requireNonNull( nullPolicy ) == NullPolicy.STRICT ) {
                    return EnumUtils.foldAnd( translator.translateList( call.getOperands(), nullAs ) );
                }
                break;
            case IS_NULL:
                // If "f" is strict, then "f(a0, a1) IS NULL" is equivalent to "a0 IS NULL OR a1 IS NULL".
                if ( Objects.requireNonNull( nullPolicy ) == NullPolicy.STRICT ) {
                    return EnumUtils.foldOr( translator.translateList( call.getOperands(), nullAs ) );
                }
                break;
        }
        final RexCall call2 = call2( harmonize, translator, call );
        try {
            return implementNullSemantics( translator, call2, nullAs, nullPolicy, implementor );
        } catch ( RexToLixTranslator.AlwaysNull e ) {
            return switch ( nullAs ) {
                case NOT_POSSIBLE -> throw e;
                case FALSE -> FALSE_EXPR;
                case TRUE -> TRUE_EXPR;
                default -> NULL_EXPR;
            };
        }
    }


    private static Expression implementNullSemantics( RexToLixTranslator translator, RexCall call, NullAs nullAs, NullPolicy nullPolicy, NotNullImplementor implementor ) {
        final List<Expression> list = new ArrayList<>();
        switch ( nullAs ) {
            case NULL:
                // v0 == null || v1 == null ? null : f(v0, v1)
                for ( Ord<RexNode> operand : Ord.zip( call.getOperands() ) ) {
                    if ( translator.isNullable( operand.e ) ) {
                        list.add( translator.translate( operand.e, NullAs.IS_NULL ) );
                        translator = translator.setNullable( operand.e, false );
                    }
                }
                final Expression box = Expressions.box( implementCall( translator, call, implementor, nullAs ) );
                return optimize( EnumUtils.condition( EnumUtils.foldOr( list ), Types.castIfNecessary( box.getType(), NULL_EXPR ), box ) );
            case FALSE:
                // v0 != null && v1 != null && f(v0, v1)
                for ( Ord<RexNode> operand : Ord.zip( call.getOperands() ) ) {
                    if ( translator.isNullable( operand.e ) ) {
                        list.add( translator.translate( operand.e, NullAs.IS_NOT_NULL ) );
                        translator = translator.setNullable( operand.e, false );
                    }
                }
                list.add( implementCall( translator, call, implementor, nullAs ) );
                return EnumUtils.foldAnd( list );
            case TRUE:
                // v0 == null || v1 == null || f(v0, v1)
                for ( Ord<RexNode> operand : Ord.zip( call.getOperands() ) ) {
                    if ( translator.isNullable( operand.e ) ) {
                        list.add( translator.translate( operand.e, NullAs.IS_NULL ) );
                        translator = translator.setNullable( operand.e, false );
                    }
                }
                list.add( implementCall( translator, call, implementor, nullAs ) );
                return EnumUtils.foldOr( list );
            case NOT_POSSIBLE:
                // Need to transmit to the implementor the fact that call cannot return null. In particular, it should return a primitive (e.g. int) rather than a box type (Integer).
                // The cases with setNullable above might not help since the same RexNode can be referred via multiple ways: RexNode itself, RexLocalRef, and may be others.
                final Map<RexNode, Boolean> nullable = new HashMap<>();
                if ( Objects.requireNonNull( nullPolicy ) == NullPolicy.STRICT ) {// The arguments should be not nullable if STRICT operator is computed in nulls NOT_POSSIBLE mode
                    for ( RexNode arg : call.getOperands() ) {
                        if ( translator.isNullable( arg ) && !nullable.containsKey( arg ) ) {
                            nullable.put( arg, false );
                        }
                    }
                }
                nullable.put( call, false );
                translator = translator.setNullable( nullable );
                // fall through
            default:
                return implementCall( translator, call, implementor, nullAs );
        }
    }


    private static Expression implementCall( final RexToLixTranslator translator, RexCall call, NotNullImplementor implementor, final NullAs nullAs ) {
        List<Expression> translatedOperands = translator.translateList( call.getOperands() );
        // Make sure the operands marked not null in the translator have all been handled for nulls before being passed to the NotNullImplementor.
        if ( nullAs == NullAs.NOT_POSSIBLE ) {
            List<Expression> nullHandled = translatedOperands;
            for ( int i = 0; i < translatedOperands.size(); i++ ) {
                RexNode arg = call.getOperands().get( i );
                Expression e = translatedOperands.get( i );
                if ( !translator.isNullable( arg ) ) {
                    if ( nullHandled == translatedOperands ) {
                        nullHandled = new ArrayList<>( translatedOperands.subList( 0, i ) );
                    }
                    nullHandled.add( translator.handleNull( e, nullAs ) );
                } else if ( nullHandled != translatedOperands ) {
                    nullHandled.add( e );
                }
            }
            translatedOperands = nullHandled;
        }
        Expression result = implementor.implement( translator, call, translatedOperands );
        return translator.handleNull( result, nullAs );
    }


    /**
     * Strategy what an operator should return if one of its arguments is null.
     */
    public enum NullAs {
        /**
         * The most common policy among the SQL built-in operators. If one of the arguments is null, returns null.
         */
        NULL,

        /**
         * If one of the arguments is null, the function returns false. Example: {@code IS NOT NULL}.
         */
        FALSE,

        /**
         * If one of the arguments is null, the function returns true. Example: {@code IS NULL}.
         */
        TRUE,

        /**
         * It is not possible for any of the arguments to be null. If the argument type is nullable, the enclosing code will already have performed a not-null check. This may allow the operator
         * implementor to generate a more efficient implementation, for example, by avoiding boxing or unboxing.
         */
        NOT_POSSIBLE,

        /**
         * Return false if result is not null, true if result is null.
         */
        IS_NULL,

        /**
         * Return true if result is not null, false if result is null.
         */
        IS_NOT_NULL;


        public static NullAs of( boolean nullable ) {
            return nullable ? NULL : NOT_POSSIBLE;
        }


        /**
         * Adapts an expression with "normal" result to one that adheres to this particular policy.
         */
        public Expression handle( Expression x ) {
            switch ( Primitive.flavor( x.getType() ) ) {
                case PRIMITIVE:
                    // Expression cannot be null. We can skip any runtime checks.
                    return switch ( this ) {
                        case NULL, NOT_POSSIBLE, FALSE, TRUE -> x;
                        case IS_NULL -> FALSE_EXPR;
                        case IS_NOT_NULL -> TRUE_EXPR;
                    };
                case BOX:
                    if ( this == NullAs.NOT_POSSIBLE ) {
                        return RexToLixTranslator.convert( x, Objects.requireNonNull( Primitive.ofBox( x.getType() ) ).primitiveClass );
                    }
                    // fall through
                default:
                    if ( (this == FALSE || this == TRUE) && Types.isAssignableFrom( PolyBoolean.class, x.type ) ) {
                        return x;
                    }
                    // fall through
            }
            return switch ( this ) {
                case NULL, NOT_POSSIBLE -> x;
                case FALSE -> Expressions.call( BuiltInMethod.IS_TRUE.method, x );
                case TRUE -> Expressions.call( BuiltInMethod.IS_NOT_FALSE.method, x );
                case IS_NULL -> Expressions.new_( PolyBoolean.class, Expressions.equal( x, NULL_EXPR ) );
                case IS_NOT_NULL -> Expressions.new_( PolyBoolean.class, Expressions.notEqual( x, NULL_EXPR ) );
            };
        }
    }


    static Expression getDefaultValue( Type type ) {
        if ( Primitive.is( type ) ) {
            Primitive p = Primitive.of( type );
            assert p != null;
            return Expressions.constant( p.defaultValue, type );
        }
        if ( Types.isAssignableFrom( PolyValue.class, type ) && PolyValue.getInitial( type ) != null ) {
            return PolyValue.getInitial( type ).asExpression();
        }
        return Expressions.constant( null, type );
    }


    /**
     * Multiplies an expression by a constant and divides by another constant, optimizing appropriately.
     * <p>
     * For example, {@code multiplyDivide(e, 10, 1000)} returns {@code e / 100}.
     */
    public static Expression multiplyDivide( Expression e, BigDecimal multiplier, BigDecimal divider ) {
        if ( multiplier.equals( BigDecimal.ONE ) ) {
            if ( divider.equals( BigDecimal.ONE ) ) {
                return e;
            }
            return Expressions.divide( e, Expressions.constant( divider.intValueExact() ) );
        }
        final BigDecimal x = multiplier.divide( divider, RoundingMode.UNNECESSARY );
        return switch ( x.compareTo( BigDecimal.ONE ) ) {
            case 0 -> e;
            case 1 -> EnumUtils.wrapPolyValue( e.type, Expressions.multiply(
                    EnumUtils.unwrapPolyValue( e, "longValue" ),
                    Expressions.constant( x.intValueExact() ) ) );
            case -1 -> multiplyDivide( e, BigDecimal.ONE, x );
            default -> throw new AssertionError();
        };
    }


    /**
     * Implementor for the {@code COUNT} aggregate function.
     */
    static class CountImplementor extends StrictAggImplementor {

        @Override
        public void implementNotNullAdd( AggContext info, AggAddContext add ) {
            add.currentBlock().add( Expressions.statement( Expressions.assign( add.accumulator().get( 0 ), Expressions.call( add.accumulator().get( 0 ), "increment" ) ) ) );
        }

    }


    /**
     * Implementor for the {@code COUNT} windowed aggregate function.
     */
    static class CountWinImplementor extends StrictWinAggImplementor {

        boolean justFrameRowCount;


        @Override
        public List<Type> getNotNullState( WinAggContext info ) {
            boolean hasNullable = false;
            for ( AlgDataType type : info.parameterAlgTypes() ) {
                if ( type.isNullable() ) {
                    hasNullable = true;
                    break;
                }
            }
            if ( !hasNullable ) {
                justFrameRowCount = true;
                return Collections.emptyList();
            }
            return super.getNotNullState( info );
        }


        @Override
        public void implementNotNullAdd( WinAggContext info, WinAggAddContext add ) {
            if ( justFrameRowCount ) {
                return;
            }
            add.currentBlock().add( Expressions.statement( Expressions.postIncrementAssign( add.accumulator().get( 0 ) ) ) );
        }


        @Override
        protected Expression implementNotNullResult( WinAggContext info, WinAggResultContext result ) {
            if ( justFrameRowCount ) {
                return result.getFrameRowCount();
            }
            return super.implementNotNullResult( info, result );
        }

    }


    /**
     * Implementor for the {@code SUM} windowed aggregate function.
     */
    static class SumImplementor extends StrictAggImplementor {

        @Override
        protected void implementNotNullReset( AggContext info, AggResetContext reset ) {
            Expression start = PolyValue.getInitialExpression( info.returnType() );//info.returnType() == BigDecimal.class
            //? Expressions.constant( BigDecimal.ZERO )
            //: Expressions.constant( 0 );

            reset.currentBlock().add( Expressions.statement( Expressions.assign( reset.accumulator().get( 0 ), start ) ) );
        }


        @Override
        public void implementNotNullAdd( AggContext info, AggAddContext add ) {
            Expression acc = add.accumulator().get( 0 );
            Expression next;
            if ( acc.type == PolyNumber.class ) {
                next = Expressions.call( acc, "plus", Expressions.convert_( add.arguments().get( 0 ), PolyNumber.class ) );
            } else {
                next = Expressions.add( acc, Types.castIfNecessary( acc.type, add.arguments().get( 0 ) ) );
            }
            accAdvance( add, acc, next );
        }


        @Override
        public Expression implementNotNullResult( AggContext info, AggResultContext result ) {
            return super.implementNotNullResult( info, result );
        }

    }


    /**
     * Implementor for the {@code MIN} and {@code MAX} aggregate functions.
     */
    static class MinMaxImplementor extends StrictAggImplementor {

        @Override
        protected void implementNotNullReset( AggContext info, AggResetContext reset ) {
            Expression acc = reset.accumulator().get( 0 );
            Primitive p = Primitive.of( acc.getType() );
            boolean isMin = OperatorName.MIN == info.aggregation().getOperatorName();
            Object inf = p == null ? null : (isMin ? p.max : p.min);
            reset.currentBlock().add( Expressions.statement( Expressions.assign( acc, Expressions.constant( inf, acc.getType() ) ) ) );
        }


        @Override
        public void implementNotNullAdd( AggContext info, AggAddContext add ) {
            Expression acc = add.accumulator().get( 0 );
            Expression arg = add.arguments().get( 0 );
            AggFunction aggregation = info.aggregation();
            final Method method = (aggregation.getOperatorName() == OperatorName.MIN
                    ? BuiltInMethod.LESSER
                    : BuiltInMethod.GREATER).method;
            Expression next = Expressions.call( method.getDeclaringClass(), method.getName(), acc, Expressions.unbox( arg ) );
            accAdvance( add, acc, next );
        }

    }


    /**
     * Implementor for the {@code SINGLE_VALUE} aggregate function.
     */
    static class SingleValueImplementor implements AggImplementor {

        @Override
        public List<Type> getStateType( AggContext info ) {
            return Arrays.asList( PolyBoolean.class, info.returnType() );
        }


        @Override
        public void implementReset( AggContext info, AggResetContext reset ) {
            List<Expression> acc = reset.accumulator();
            reset.currentBlock().add( Expressions.statement( Expressions.assign( acc.get( 0 ), FALSE_EXPR ) ) );
            reset.currentBlock().add(
                    Expressions.statement(
                            Expressions.assign(
                                    acc.get( 1 ),
                                    getDefaultValue( acc.get( 1 ).getType() ) ) ) );
        }


        @Override
        public void implementAdd( AggContext info, AggAddContext add ) {
            List<Expression> acc = add.accumulator();
            Expression flag = acc.get( 0 );
            add.currentBlock().add(
                    EnumUtils.ifThen(
                            flag,
                            Expressions.throw_(
                                    Expressions.new_(
                                            IllegalStateException.class,
                                            Expressions.constant( "more than one value in agg " + info.aggregation() ) ) ) ) );
            add.currentBlock().add( Expressions.statement( Expressions.assign( flag, TRUE_EXPR ) ) );
            add.currentBlock().add( Expressions.statement( Expressions.assign( acc.get( 1 ), add.arguments().get( 0 ) ) ) );
        }


        @Override
        public Expression implementResult( AggContext info, AggResultContext result ) {
            return RexToLixTranslator.convert( result.accumulator().get( 1 ), info.returnType() );
        }

    }


    /**
     * Implementor for the {@code COLLECT} aggregate function.
     */
    static class CollectImplementor extends StrictAggImplementor {

        @Override
        protected void implementNotNullReset( AggContext info, AggResetContext reset ) {
            // acc[0] = new ArrayList();
            reset.currentBlock().add( Expressions.statement( Expressions.assign( reset.accumulator().get( 0 ), Expressions.new_( ArrayList.class ) ) ) );
        }


        @Override
        public void implementNotNullAdd( AggContext info, AggAddContext add ) {
            // acc[0].add(arg);
            add.currentBlock().add(
                    Expressions.statement(
                            Expressions.call(
                                    add.accumulator().get( 0 ),
                                    BuiltInMethod.COLLECTION_ADD.method,
                                    add.arguments().get( 0 ) ) ) );
        }

    }


    /**
     * Implementor for the {@code FUSION} aggregate function.
     */
    static class FusionImplementor extends StrictAggImplementor {

        @Override
        protected void implementNotNullReset( AggContext info, AggResetContext reset ) {
            // acc[0] = new ArrayList();
            reset.currentBlock().add(
                    Expressions.statement(
                            Expressions.assign(
                                    reset.accumulator().get( 0 ),
                                    Expressions.new_( ArrayList.class ) ) ) );
        }


        @Override
        public void implementNotNullAdd( AggContext info, AggAddContext add ) {
            // acc[0].add(arg);
            add.currentBlock().add(
                    Expressions.statement(
                            Expressions.call(
                                    add.accumulator().get( 0 ),
                                    BuiltInMethod.COLLECTION_ADDALL.method,
                                    add.arguments().get( 0 ) ) ) );
        }

    }


    /**
     * Implementor for the {@code BIT_AND} and {@code BIT_OR} aggregate function.
     */
    static class BitOpImplementor extends StrictAggImplementor {

        @Override
        protected void implementNotNullReset( AggContext info, AggResetContext reset ) {
            Object initValue = info.aggregation().equals( OperatorRegistry.getAgg( OperatorName.BIT_AND ) ) ? -1 : 0;
            Expression start = Expressions.constant( initValue, info.returnType() );

            reset.currentBlock().add( Expressions.statement( Expressions.assign( reset.accumulator().get( 0 ), start ) ) );
        }


        @Override
        public void implementNotNullAdd( AggContext info, AggAddContext add ) {
            Expression acc = add.accumulator().get( 0 );
            Expression arg = add.arguments().get( 0 );
            AggFunction aggregation = info.aggregation();
            final Method method = (aggregation.equals( OperatorRegistry.getAgg( OperatorName.BIT_AND ) )
                    ? BuiltInMethod.BIT_AND
                    : BuiltInMethod.BIT_OR).method;
            Expression next = Expressions.call( method.getDeclaringClass(), method.getName(), acc, Expressions.unbox( arg ) );
            accAdvance( add, acc, next );
        }

    }


    /**
     * Implementor for the {@code GROUPING} aggregate function.
     */
    static class GroupingImplementor implements AggImplementor {

        @Override
        public List<Type> getStateType( AggContext info ) {
            return ImmutableList.of();
        }


        @Override
        public void implementReset( AggContext info, AggResetContext reset ) {
        }


        @Override
        public void implementAdd( AggContext info, AggAddContext add ) {
        }


        @Override
        public Expression implementResult( AggContext info, AggResultContext result ) {
            final List<Integer> keys = switch ( info.aggregation().getKind() ) {
                case GROUPING -> // "GROUPING(e, ...)", also "GROUPING_ID(e, ...)"
                        result.call().getArgList();
                case GROUP_ID -> // "GROUP_ID()"
                    // We don't implement GROUP_ID properly. In most circumstances, it returns 0, so we always return 0. Logged
                    // [POLYPHENYDB-1824] GROUP_ID returns wrong result
                        ImmutableList.of();
                default -> throw new AssertionError();
            };
            Expression e = null;
            if ( info.groupSets().size() > 1 ) {
                final List<Integer> keyOrdinals = info.keyOrdinals();
                long x = 1L << (keys.size() - 1);
                for ( int k : keys ) {
                    final int i = keyOrdinals.indexOf( k );
                    assert i >= 0;
                    final Expression e2 =
                            EnumUtils.condition(
                                    result.keyField( keyOrdinals.size() + i ),
                                    Expressions.constant( x ),
                                    Expressions.constant( 0L ) );
                    if ( e == null ) {
                        e = e2;
                    } else {
                        e = Expressions.add( e, e2 );
                    }
                    x >>= 1;
                }
            }
            return e != null ? e : Expressions.constant( 0, info.returnType() );
        }

    }


    /**
     * Implementor for user-defined aggregate functions.
     */
    public static class UserDefinedAggReflectiveImplementor extends StrictAggImplementor {

        private final AggregateFunctionImpl afi;


        public UserDefinedAggReflectiveImplementor( AggregateFunctionImpl afi ) {
            this.afi = afi;
        }


        @Override
        public List<Type> getNotNullState( AggContext info ) {
            if ( afi.isStatic ) {
                return Collections.singletonList( afi.accumulatorType );
            }
            return Arrays.asList( afi.accumulatorType, afi.declaringClass );
        }


        @Override
        protected void implementNotNullReset( AggContext info, AggResetContext reset ) {
            List<Expression> acc = reset.accumulator();
            if ( !afi.isStatic ) {
                reset.currentBlock().add( Expressions.statement( Expressions.assign( acc.get( 1 ), Expressions.new_( afi.declaringClass ) ) ) );
            }
            reset.currentBlock().add(
                    Expressions.statement(
                            Expressions.assign(
                                    acc.get( 0 ),
                                    Expressions.call(
                                            afi.isStatic
                                                    ? null
                                                    : acc.get( 1 ), afi.initMethod ) ) ) );
        }


        @Override
        protected void implementNotNullAdd( AggContext info, AggAddContext add ) {
            List<Expression> acc = add.accumulator();
            List<Expression> aggArgs = add.arguments();
            List<Expression> args = new ArrayList<>( aggArgs.size() + 1 );
            args.add( acc.get( 0 ) );
            args.addAll( aggArgs );
            add.currentBlock().add(
                    Expressions.statement(
                            Expressions.assign( acc.get( 0 ), Expressions.call( afi.isStatic ? null : acc.get( 1 ), afi.addMethod, args ) ) ) );
        }


        @Override
        protected Expression implementNotNullResult( AggContext info, AggResultContext result ) {
            List<Expression> acc = result.accumulator();
            return Expressions.call( afi.isStatic ? null : acc.get( 1 ), afi.resultMethod, acc.get( 0 ) );
        }

    }


    /**
     * Implementor for the {@code RANK} windowed aggregate function.
     */
    static class RankImplementor extends StrictWinAggImplementor {

        @Override
        protected void implementNotNullAdd( WinAggContext info, WinAggAddContext add ) {
            Expression acc = add.accumulator().get( 0 );
            // This is an example of the generated code
            BlockBuilder builder = add.nestBlock();
            add.currentBlock().add(
                    EnumUtils.ifThen(
                            Expressions.lessThan(
                                    add.compareRows( Expressions.subtract( add.currentPosition(), Expressions.constant( 1 ) ), add.currentPosition() ),
                                    Expressions.constant( 0 ) ),
                            Expressions.statement(
                                    Expressions.assign( acc, computeNewRank( acc, add ) ) ) ) );
            add.exitBlock();
            add.currentBlock().add(
                    EnumUtils.ifThen(
                            Expressions.greaterThan( add.currentPosition(), add.startIndex() ),
                            builder.toBlock() ) );
        }


        protected Expression computeNewRank( Expression acc, WinAggAddContext add ) {
            Expression pos = add.currentPosition();
            if ( !add.startIndex().equals( Expressions.constant( 0 ) ) ) {
                // In general, currentPosition-startIndex should be used. However, rank/dense_rank does not allow preceding/following clause so we always result in startIndex==0.
                pos = Expressions.subtract( pos, add.startIndex() );
            }
            return pos;
        }


        @Override
        protected Expression implementNotNullResult( WinAggContext info, WinAggResultContext result ) {
            // Rank is 1-based
            return Expressions.add( super.implementNotNullResult( info, result ), Expressions.constant( 1 ) );
        }

    }


    /**
     * Implementor for the {@code DENSE_RANK} windowed aggregate function.
     */
    static class DenseRankImplementor extends RankImplementor {

        @Override
        protected Expression computeNewRank( Expression acc, WinAggAddContext add ) {
            return Expressions.add( acc, Expressions.constant( 1 ) );
        }

    }


    /**
     * Implementor for the {@code FIRST_VALUE} and {@code LAST_VALUE} windowed aggregate functions.
     */
    static class FirstLastValueImplementor implements WinAggImplementor {

        private final SeekType seekType;


        protected FirstLastValueImplementor( SeekType seekType ) {
            this.seekType = seekType;
        }


        @Override
        public List<Type> getStateType( AggContext info ) {
            return Collections.emptyList();
        }


        @Override
        public void implementReset( AggContext info, AggResetContext reset ) {
            // no op
        }


        @Override
        public void implementAdd( AggContext info, AggAddContext add ) {
            // no op
        }


        @Override
        public boolean needCacheWhenFrameIntact() {
            return true;
        }


        @Override
        public Expression implementResult( AggContext info, AggResultContext result ) {
            WinAggResultContext winResult = (WinAggResultContext) result;

            return EnumUtils.condition(
                    winResult.hasRows(),
                    winResult.rowTranslator( winResult.computeIndex( Expressions.constant( 0 ), seekType ) )
                            .translate( winResult.rexArguments().get( 0 ), info.returnType() ),
                    getDefaultValue( info.returnType() ) );
        }

    }


    /**
     * Implementor for the {@code FIRST_VALUE} windowed aggregate function.
     */
    static class FirstValueImplementor extends FirstLastValueImplementor {

        protected FirstValueImplementor() {
            super( SeekType.START );
        }

    }


    /**
     * Implementor for the {@code LAST_VALUE} windowed aggregate function.
     */
    static class LastValueImplementor extends FirstLastValueImplementor {

        protected LastValueImplementor() {
            super( SeekType.END );
        }

    }


    /**
     * Implementor for the {@code NTH_VALUE} windowed aggregate function.
     */
    static class NthValueImplementor implements WinAggImplementor {

        @Override
        public List<Type> getStateType( AggContext info ) {
            return Collections.emptyList();
        }


        @Override
        public void implementReset( AggContext info, AggResetContext reset ) {
            // no op
        }


        @Override
        public void implementAdd( AggContext info, AggAddContext add ) {
            // no op
        }


        @Override
        public boolean needCacheWhenFrameIntact() {
            return true;
        }


        @Override
        public Expression implementResult( AggContext info, AggResultContext result ) {
            WinAggResultContext winResult = (WinAggResultContext) result;

            List<RexNode> rexArgs = winResult.rexArguments();

            ParameterExpression res = Expressions.parameter( 0, info.returnType(), result.currentBlock().newName( "nth" ) );

            RexToLixTranslator currentRowTranslator = winResult.rowTranslator( winResult.computeIndex( Expressions.constant( 0 ), SeekType.START ) );

            Expression dstIndex =
                    winResult.computeIndex(
                            Expressions.subtract(
                                    currentRowTranslator.translate( rexArgs.get( 1 ), int.class ),
                                    Expressions.constant( 1 ) ), SeekType.START );

            Expression rowInRange = winResult.rowInPartition( dstIndex );

            BlockBuilder thenBlock = result.nestBlock();
            Expression nthValue = winResult.rowTranslator( dstIndex ).translate( rexArgs.get( 0 ), res.type );
            thenBlock.add( Expressions.statement( Expressions.assign( res, nthValue ) ) );
            result.exitBlock();
            BlockStatement thenBranch = thenBlock.toBlock();

            Expression defaultValue = getDefaultValue( res.type );

            result.currentBlock().add( Expressions.declare( 0, res, null ) );
            result.currentBlock().add( EnumUtils.ifThenElse( rowInRange, thenBranch, Expressions.statement( Expressions.assign( res, defaultValue ) ) ) );
            return res;
        }

    }


    /**
     * Implementor for the {@code LEAD} and {@code LAG} windowed aggregate functions.
     */
    static class LeadLagImplementor implements WinAggImplementor {

        private final boolean isLead;


        protected LeadLagImplementor( boolean isLead ) {
            this.isLead = isLead;
        }


        @Override
        public List<Type> getStateType( AggContext info ) {
            return Collections.emptyList();
        }


        @Override
        public void implementReset( AggContext info, AggResetContext reset ) {
            // no op
        }


        @Override
        public void implementAdd( AggContext info, AggAddContext add ) {
            // no op
        }


        @Override
        public boolean needCacheWhenFrameIntact() {
            return false;
        }


        @Override
        public Expression implementResult( AggContext info, AggResultContext result ) {
            WinAggResultContext winResult = (WinAggResultContext) result;

            List<RexNode> rexArgs = winResult.rexArguments();

            ParameterExpression res = Expressions.parameter( 0, info.returnType(), result.currentBlock().newName( isLead ? "lead" : "lag" ) );

            Expression offset;
            RexToLixTranslator currentRowTranslator = winResult.rowTranslator( winResult.computeIndex( Expressions.constant( 0 ), SeekType.SET ) );
            if ( rexArgs.size() >= 2 ) {
                // lead(x, offset) or lead(x, offset, default)
                offset = currentRowTranslator.translate( rexArgs.get( 1 ), int.class );
            } else {
                offset = Expressions.constant( 1 );
            }
            if ( !isLead ) {
                offset = Expressions.negate( offset );
            }
            Expression dstIndex = winResult.computeIndex( offset, SeekType.SET );

            Expression rowInRange = winResult.rowInPartition( dstIndex );

            BlockBuilder thenBlock = result.nestBlock();
            Expression lagResult = winResult.rowTranslator( dstIndex ).translate( rexArgs.get( 0 ), res.type );
            thenBlock.add( Expressions.statement( Expressions.assign( res, lagResult ) ) );
            result.exitBlock();
            BlockStatement thenBranch = thenBlock.toBlock();

            Expression defaultValue =
                    rexArgs.size() == 3
                            ? currentRowTranslator.translate( rexArgs.get( 2 ), res.type )
                            : getDefaultValue( res.type );

            result.currentBlock().add( Expressions.declare( 0, res, null ) );
            result.currentBlock().add( EnumUtils.ifThenElse( rowInRange, thenBranch, Expressions.statement( Expressions.assign( res, defaultValue ) ) ) );
            return res;
        }

    }


    /**
     * Implementor for the {@code LEAD} windowed aggregate function.
     */
    public static class LeadImplementor extends LeadLagImplementor {

        protected LeadImplementor() {
            super( true );
        }

    }


    /**
     * Implementor for the {@code LAG} windowed aggregate function.
     */
    public static class LagImplementor extends LeadLagImplementor {

        protected LagImplementor() {
            super( false );
        }

    }


    /**
     * Implementor for the {@code NTILE} windowed aggregate function.
     */
    static class NtileImplementor implements WinAggImplementor {

        @Override
        public List<Type> getStateType( AggContext info ) {
            return Collections.emptyList();
        }


        @Override
        public void implementReset( AggContext info, AggResetContext reset ) {
            // no op
        }


        @Override
        public void implementAdd( AggContext info, AggAddContext add ) {
            // no op
        }


        @Override
        public boolean needCacheWhenFrameIntact() {
            return false;
        }


        @Override
        public Expression implementResult( AggContext info, AggResultContext result ) {
            WinAggResultContext winResult = (WinAggResultContext) result;

            List<RexNode> rexArgs = winResult.rexArguments();

            Expression tiles = winResult.rowTranslator( winResult.index() ).translate( rexArgs.get( 0 ), int.class );

            return Expressions.add(
                    Expressions.constant( 1 ),
                    Expressions.divide(
                            Expressions.multiply(
                                    tiles,
                                    Expressions.subtract( winResult.index(), winResult.startIndex() ) ),
                            winResult.getPartitionRowCount() ) );
        }

    }


    /**
     * Implementor for the {@code ROW_NUMBER} windowed aggregate function.
     */
    static class RowNumberImplementor extends StrictWinAggImplementor {

        @Override
        public List<Type> getNotNullState( WinAggContext info ) {
            return Collections.emptyList();
        }


        @Override
        protected void implementNotNullAdd( WinAggContext info, WinAggAddContext add ) {
            // no op
        }


        @Override
        protected Expression implementNotNullResult( WinAggContext info, WinAggResultContext result ) {
            // Window cannot be empty since ROWS/RANGE is not possible for ROW_NUMBER
            return Expressions.add( Expressions.subtract( result.index(), result.startIndex() ), Expressions.constant( 1 ) );
        }

    }


    /**
     * Implementor for the {@code JSON_OBJECTAGG} aggregate function.
     */
    static class JsonObjectAggImplementor implements AggImplementor {

        private final Method m;


        JsonObjectAggImplementor( Method m ) {
            this.m = m;
        }


        static Supplier<JsonObjectAggImplementor> supplierFor( Method m ) {
            return () -> new JsonObjectAggImplementor( m );
        }


        @Override
        public List<Type> getStateType( AggContext info ) {
            return Collections.singletonList( Map.class );
        }


        @Override
        public void implementReset( AggContext info, AggResetContext reset ) {
            reset.currentBlock().add(
                    Expressions.statement(
                            Expressions.assign( reset.accumulator().get( 0 ), Expressions.new_( HashMap.class ) ) ) );
        }


        @Override
        public void implementAdd( AggContext info, AggAddContext add ) {
            final JsonAgg function = (JsonAgg) info.aggregation();
            add.currentBlock().add(
                    Expressions.statement(
                            Expressions.call(
                                    m,
                                    Iterables.concat(
                                            Collections.singletonList( add.accumulator().get( 0 ) ),
                                            add.arguments(),
                                            Collections.singletonList( Expressions.constant( function.getNullClause() ) ) ) ) ) );
        }


        @Override
        public Expression implementResult( AggContext info, AggResultContext result ) {
            return Expressions.call( BuiltInMethod.JSONIZE.method, result.accumulator().get( 0 ) );
        }

    }


    /**
     * Implementor for the {@code JSON_ARRAYAGG} aggregate function.
     */
    static class JsonArrayAggImplementor implements AggImplementor {

        private final Method m;


        JsonArrayAggImplementor( Method m ) {
            this.m = m;
        }


        static Supplier<JsonArrayAggImplementor> supplierFor( Method m ) {
            return () -> new JsonArrayAggImplementor( m );
        }


        @Override
        public List<Type> getStateType( AggContext info ) {
            return Collections.singletonList( List.class );
        }


        @Override
        public void implementReset( AggContext info, AggResetContext reset ) {
            reset.currentBlock().add(
                    Expressions.statement(
                            Expressions.assign(
                                    reset.accumulator().get( 0 ),
                                    Expressions.new_( ArrayList.class ) ) ) );
        }


        @Override
        public void implementAdd( AggContext info, AggAddContext add ) {
            final JsonAgg function = (JsonAgg) info.aggregation();
            add.currentBlock().add(
                    Expressions.statement(
                            Expressions.call(
                                    m,
                                    Iterables.concat(
                                            Collections.singletonList( add.accumulator().get( 0 ) ),
                                            add.arguments(),
                                            Collections.singletonList( Expressions.constant( function.getNullClause() ) ) ) ) ) );
        }


        @Override
        public Expression implementResult( AggContext info, AggResultContext result ) {
            return Expressions.call( BuiltInMethod.JSONIZE.method, result.accumulator().get( 0 ) );
        }

    }


    /**
     * Implementor for the {@code TRIM} function.
     */
    private static class TrimImplementor implements NotNullImplementor {

        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
            final boolean strict = !translator.conformance.allowExtendedTrim();
            final Object value = ((ConstantExpression) translatedOperands.get( 0 )).value;
            Flag flag = ((TrimFlagHolder) value).getFlag();
            return Expressions.call(
                    BuiltInMethod.TRIM.method,
                    Expressions.constant( flag == Flag.BOTH || flag == Flag.LEADING ),
                    Expressions.constant( flag == Flag.BOTH || flag == Flag.TRAILING ),
                    translatedOperands.get( 1 ),
                    translatedOperands.get( 2 ),
                    Expressions.constant( strict ) );
        }

    }


    /**
     * Implementor for the {@code FLOOR} and {@code CEIL} functions.
     */
    private static class FloorImplementor extends MethodNameImplementor {

        final Method timestampMethod;
        final Method dateMethod;


        FloorImplementor( String methodName, Method timestampMethod, Method dateMethod ) {
            super( methodName );
            this.timestampMethod = timestampMethod;
            this.dateMethod = dateMethod;
        }


        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
            switch ( call.getOperands().size() ) {
                case 1:
                    return switch ( call.getType().getPolyType() ) {
                        case BIGINT, INTEGER, SMALLINT, TINYINT -> translatedOperands.get( 0 );
                        default -> super.implement( translator, call, translatedOperands );
                    };
                case 2:
                    final Type type;
                    final Method floorMethod;
                    Expression operand = translatedOperands.get( 0 );
                    if ( Objects.requireNonNull( call.getType().getPolyType() ) == PolyType.TIMESTAMP ) {
                        type = PolyBigDecimal.class;
                        operand = Expressions.call( PolyBigDecimal.class, "convert", operand );
                        floorMethod = timestampMethod;
                    } else {
                        type = PolyBigDecimal.class;
                        floorMethod = dateMethod;
                    }
                    ConstantExpression tur = (ConstantExpression) translatedOperands.get( 1 );
                    final TimeUnitRange timeUnitRange = (TimeUnitRange) tur.value;
                    return switch ( Objects.requireNonNull( timeUnitRange ) ) {
                        case YEAR, MONTH -> Expressions.call( floorMethod, tur, EnumUtils.convertPolyValue( call.type.getPolyType(), call( operand, type, TimeUnit.DAY ) ) );
                        default -> EnumUtils.convertPolyValue( call.type.getPolyType(), call( operand, type, timeUnitRange.startUnit ) );
                    };
                default:
                    throw new AssertionError();
            }
        }


        private Expression call( Expression operand, Type type, TimeUnit timeUnit ) {
            return Expressions.call( Functions.class, methodName,
                    operand,
                    Types.castIfNecessary( type, EnumUtils.wrapPolyValue( type, Expressions.constant( timeUnit.multiplier ) ) ) );
        }

    }


    /**
     * Implementor for a function that generates calls to a given method.
     */
    @Getter
    @Value
    public static class MethodImplementor implements NotNullImplementor {

        public Method method;


        MethodImplementor( Method method ) {
            this.method = method;
        }


        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
            return implement( translator.typeFactory, call, translatedOperands );
        }


        public Expression implement( JavaTypeFactory typeFactory, RexCall call, List<Expression> translatedOperands ) {
            final Expression expression;
            if ( Modifier.isStatic( method.getModifiers() ) ) {
                expression = Expressions.call( method, translatedOperands );
            } else {
                expression = Expressions.call( translatedOperands.get( 0 ), method, Util.skip( translatedOperands, 1 ) );
            }

            final Type returnType = typeFactory.getJavaClass( call.getType() );
            return Types.castIfNecessary( returnType, expression );
        }

    }


    /**
     * Implementor for SQL functions that generates calls to a given method name.
     * <p>
     * Use this, as opposed to {@link MethodImplementor}, if the SQL function is overloaded; then you can use one implementor for several overloads.
     */
    private static class MethodNameImplementor implements NotNullImplementor {

        protected final String methodName;


        MethodNameImplementor( String methodName ) {
            this.methodName = methodName;
        }


        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
            return Expressions.call( Functions.class, methodName, translatedOperands );
        }

    }


    private record MqlMethodNameImplementor(String methodName) implements NotNullImplementor {


        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
            return Expressions.call( MqlFunctions.class, methodName, translatedOperands );
        }

    }


    /**
     * Implementor for binary operators.
     */
    private record BinaryImplementor(ExpressionType expressionType, String backupMethodName) implements NotNullImplementor {

        /**
         * Types that can be arguments to comparison operators such as {@code <}.
         */
        private static final List<Primitive> COMP_OP_TYPES = ImmutableList.of( Primitive.BYTE, Primitive.CHAR, Primitive.SHORT, Primitive.INT, Primitive.LONG, Primitive.FLOAT, Primitive.DOUBLE );

        private static final List<BinaryOperator> COMPARISON_OPERATORS =
                ImmutableList.of(
                        OperatorRegistry.get( OperatorName.LESS_THAN, BinaryOperator.class ),
                        OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL, BinaryOperator.class ),
                        OperatorRegistry.get( OperatorName.GREATER_THAN, BinaryOperator.class ),
                        OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL, BinaryOperator.class ) );
        public static final String METHOD_POSTFIX_FOR_ANY_TYPE = "Any";


        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> expressions ) {
            // neither nullable:
            //   return x OP y
            // x nullable
            //   null_returns_null
            //     return x == null ? null : x OP y
            //   ignore_null
            //     return x == null ? null : y
            // x, y both nullable
            //   null_returns_null
            //     return x == null || y == null ? null : x OP y
            //   ignore_null
            //     return x == null ? y : y == null ? x : x OP y
            if ( backupMethodName != null ) {
                // If one or both operands have ANY type, use the late-binding backup
                // method.
                if ( anyAnyOperands( call ) ) {
                    return callBackupMethodAnyType( translator, call, expressions );
                }

                final Type type0 = expressions.get( 0 ).getType();
                final Type type1 = expressions.get( 1 ).getType();
                final BinaryOperator op = (BinaryOperator) call.getOperator();
                final Primitive primitive = Primitive.ofBoxOr( type0 );
                if ( primitive == null
                        || type1 == BigDecimal.class
                        || Types.isAssignableFrom( PolyValue.class, type0 )
                        || Types.isAssignableFrom( PolyValue.class, type1 )
                        || COMPARISON_OPERATORS.contains( op )
                        && !COMP_OP_TYPES.contains( primitive ) ) {
                    return Expressions.call( Functions.class, backupMethodName, expressions );
                }
            }
            log.warn( "this should not happen" );
            final Type returnType = translator.typeFactory.getJavaClass( call.getType() );
            return Types.castIfNecessary( returnType, Expressions.makeBinary( expressionType, expressions.get( 0 ), expressions.get( 1 ) ) );
        }


        /**
         * Returns whether any of a call's operands have ANY type.
         */
        private boolean anyAnyOperands( RexCall call ) {
            for ( RexNode operand : call.operands ) {
                if ( operand.getType().getPolyType() == PolyType.ANY ) {
                    return true;
                }
            }
            return false;
        }


        private Expression callBackupMethodAnyType( RexToLixTranslator translator, RexCall call, List<Expression> expressions ) {
            final String backupMethodNameForAnyType = backupMethodName + METHOD_POSTFIX_FOR_ANY_TYPE;

            // one or both of parameter(s) is(are) ANY type
            final Expression expression0 = maybeBox( expressions.get( 0 ) );
            final Expression expression1 = maybeBox( expressions.get( 1 ) );
            return Expressions.call( Functions.class, backupMethodNameForAnyType, expression0, expression1 );
        }


        private Expression maybeBox( Expression expression ) {
            final Primitive primitive = Primitive.of( expression.getType() );
            if ( primitive != null ) {
                expression = Expressions.box( expression, primitive );
            }
            return expression;
        }

    }


    /**
     * Implementor for unary operators.
     */
    private record UnaryImplementor(ExpressionType expressionType) implements NotNullImplementor {


        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
            final Expression operand = Expressions.convert_( translatedOperands.get( 0 ), PolyNumber.class );
            //final UnaryExpression e = Expressions.makeUnary( expressionType, operand );
            final Expression e = Expressions.call( operand, "negate" );
            if ( e.type.equals( operand.type ) ) {
                return e;
            }
            // Certain unary operators do not preserve type. For example, the "-" operator applied to a "byte" expression returns an "int".
            return Expressions.convert_( e, operand.type );
        }

    }


    /**
     * Implementor for the {@code EXTRACT(unit FROM datetime)} function.
     */
    private static class ExtractImplementor implements NotNullImplementor {

        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
            final TimeUnitRange timeUnitRange = (TimeUnitRange) ((ConstantExpression) translatedOperands.get( 0 )).value;
            assert timeUnitRange != null;
            final TimeUnit unit = timeUnitRange.startUnit;
            Expression operand = translatedOperands.get( 1 );
            final PolyType polyType = call.operands.get( 1 ).getType().getPolyType();
            switch ( unit ) {
                case MILLENNIUM:
                case CENTURY:
                case YEAR:
                case QUARTER:
                case MONTH:
                case DAY:
                case DOW:
                case DECADE:
                case DOY:
                case ISODOW:
                case ISOYEAR:
                case WEEK:
                    switch ( polyType ) {
                        case INTERVAL:
                            break;
                        case TIMESTAMP:
                            //operand = EnumUtils.unwrapPolyValue( operand, "longValue" );
                            return Expressions.call( BuiltInMethod.UNIX_DATE_EXTRACT.method, translatedOperands.get( 0 ), operand );

                        case DATE:
                            return Expressions.call( BuiltInMethod.UNIX_DATE_EXTRACT.method, translatedOperands.get( 0 ), operand );
                        default:
                            throw new AssertionError( "unexpected " + polyType );
                    }
                    break;
                case MILLISECOND:
                    return EnumUtils.wrapPolyValue( call.type.getPolyType(), Expressions.modulo( EnumUtils.unwrapPolyValue( operand, "longValue" ), Expressions.constant( TimeUnit.MINUTE.multiplier.longValue() ) ) );
                case MICROSECOND:
                    operand = Expressions.modulo( EnumUtils.unwrapPolyValue( operand, "longValue" ), Expressions.constant( TimeUnit.MINUTE.multiplier.longValue() ) );
                    return EnumUtils.wrapPolyValue( call.type.getPolyType(), Expressions.multiply( operand, Expressions.constant( TimeUnit.SECOND.multiplier.longValue() ) ) );
                case EPOCH:
                    switch ( polyType ) {
                        case DATE:
                            // convert to milliseconds
                            operand = Expressions.multiply( EnumUtils.unwrapPolyValue( operand, "longValue" ), Expressions.constant( TimeUnit.DAY.multiplier.longValue() ) );
                            return EnumUtils.wrapPolyValue( call.type.getPolyType(), Expressions.divide( operand, Expressions.constant( TimeUnit.SECOND.multiplier.longValue() ) ) );
                        case TIMESTAMP:
                            // convert to seconds
                            return EnumUtils.wrapPolyValue( call.type.getPolyType(), Expressions.divide( EnumUtils.unwrapPolyValue( operand, "longValue" ), Expressions.constant( TimeUnit.SECOND.multiplier.longValue() ) ) );
                        case INTERVAL:
                            // no convertlet conversion, pass it as extract
                            throw new AssertionError( "unexpected " + polyType );
                    }
                    break;
                case HOUR:
                case MINUTE:
                case SECOND:
                    if ( Objects.requireNonNull( polyType ) == PolyType.DATE ) {
                        return EnumUtils.wrapPolyValue( call.type.getPolyType(), Expressions.multiply( EnumUtils.unwrapPolyValue( operand, "longValue" ), Expressions.constant( 0L ) ) );
                    }
                    break;
            }

            MethodCallExpression num = Expressions.call( PolyValue.classFrom( call.type.getPolyType() ), "convert", operand );
            num = EnumUtils.unwrapPolyValue( num, "longValue" );
            operand = mod( num, getFactor( unit ) );
            if ( unit == TimeUnit.QUARTER ) {
                operand = Expressions.subtract( operand, Expressions.constant( 1L ) );
            }
            operand = Expressions.divide( operand, Expressions.constant( unit.multiplier.longValue() ) );
            if ( unit == TimeUnit.QUARTER ) {
                operand = Expressions.add( operand, Expressions.constant( 1L ) );
            }
            return EnumUtils.wrapPolyValue( call.type.getPolyType(), operand );
        }


    }


    private static Expression mod( Expression operand, long factor ) {
        if ( factor == 1L ) {
            return operand;
        } else {
            return Expressions.modulo( operand, Expressions.constant( factor ) );
        }
    }


    private static long getFactor( TimeUnit unit ) {
        return switch ( unit ) {
            case DAY -> 1L;
            case HOUR -> TimeUnit.DAY.multiplier.longValue();
            case MINUTE -> TimeUnit.HOUR.multiplier.longValue();
            case SECOND -> TimeUnit.MINUTE.multiplier.longValue();
            case MILLISECOND -> TimeUnit.SECOND.multiplier.longValue();
            case MONTH -> TimeUnit.YEAR.multiplier.longValue();
            case QUARTER -> TimeUnit.YEAR.multiplier.longValue();
            case YEAR, DECADE, CENTURY, MILLENNIUM -> 1L;
            default -> throw Util.unexpected( unit );
        };
    }


    /**
     * Implementor for the SQL {@code CASE} operator.
     */
    private static class CaseImplementor implements CallImplementor {

        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, NullAs nullAs ) {
            return implementRecurse( translator, call, nullAs, 0 );
        }


        private Expression implementRecurse( RexToLixTranslator translator, RexCall call, NullAs nullAs, int i ) {
            List<RexNode> operands = call.getOperands();
            if ( i == operands.size() - 1 ) {
                // the "else" clause
                return translator.translate(
                        translator.builder.ensureType(
                                call.getType(),
                                operands.get( i ),
                                false ),
                        nullAs );
            } else {
                Expression ifTrue;
                try {
                    ifTrue = translator.translate(
                            translator.builder.ensureType(
                                    call.getType(),
                                    operands.get( i + 1 ),
                                    false ),
                            nullAs );
                } catch ( RexToLixTranslator.AlwaysNull e ) {
                    ifTrue = null;
                }

                Expression ifFalse;
                try {
                    ifFalse = implementRecurse( translator, call, nullAs, i + 2 );
                } catch ( RexToLixTranslator.AlwaysNull e ) {
                    if ( ifTrue == null ) {
                        throw RexToLixTranslator.AlwaysNull.INSTANCE;
                    }
                    ifFalse = null;
                }

                Expression test = translator.translate( operands.get( i ), NullAs.FALSE );

                return ifTrue == null || ifFalse == null
                        ? Util.first( ifTrue, ifFalse )
                        : EnumUtils.condition( test, ifTrue, ifFalse );
            }
        }

    }


    /**
     * Implementor for the SQL {@code COALESCE} operator.
     */
    private static class CoalesceImplementor implements CallImplementor {

        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, NullAs nullAs ) {
            return implementRecurse( translator, call.operands, nullAs );
        }


        private Expression implementRecurse( RexToLixTranslator translator, List<RexNode> operands, NullAs nullAs ) {
            if ( operands.size() == 1 ) {
                return translator.translate( operands.get( 0 ) );
            } else {
                return EnumUtils.condition(
                        translator.translate( operands.get( 0 ), NullAs.IS_NULL ),
                        translator.translate( operands.get( 0 ), nullAs ),
                        implementRecurse( translator, Util.skip( operands ), nullAs ) );
            }
        }

    }


    /**
     * Implementor for the SQL {@code CAST} function that optimizes if, say, the argument is already of the desired type.
     */
    private static class CastOptimizedImplementor implements CallImplementor {

        private final CallImplementor accurate;


        private CastOptimizedImplementor() {
            accurate = createImplementor( new CastImplementor(), NullPolicy.STRICT, false );
        }


        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, NullAs nullAs ) {
            // Short-circuit if no cast is required
            RexNode arg = call.getOperands().get( 0 );
            if ( call.getType().equals( arg.getType() ) ) {
                // No cast required, omit cast
                return translator.translate( arg, nullAs );
            }
            if ( PolyTypeUtil.equalSansNullability( translator.typeFactory,
                    call.getType(), arg.getType() )
                    && nullAs == NullAs.NULL
                    && translator.deref( arg ) instanceof RexLiteral ) {
                return RexToLixTranslator.translateLiteral( (RexLiteral) translator.deref( arg ), call.getType(), translator.typeFactory, nullAs );
            }
            return accurate.implement( translator, call, nullAs );
        }

    }


    /**
     * Implementor for the SQL {@code CAST} operator.
     */
    private static class CastImplementor implements NotNullImplementor {

        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
            assert call.getOperands().size() == 1;
            final AlgDataType sourceType = call.getOperands().get( 0 ).getType();
            // It's only possible for the result to be null if both expression and target type are nullable. We assume that the caller did not make a mistake. If expression looks nullable, caller WILL have
            // checked that expression is not null before calling us.
            final boolean nullable =
                    translator.isNullable( call )
                            && sourceType.isNullable()
                            && !Primitive.is( translatedOperands.get( 0 ).getType() );
            final AlgDataType targetType = translator.nullifyType( call.getType(), nullable );
            return translator.translateCast( sourceType, targetType, translatedOperands.get( 0 ) );
        }

    }


    /**
     * Implementor for the {@code REINTERPRET} internal SQL operator.
     */
    private static class ReinterpretImplementor implements NotNullImplementor {

        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
            assert call.getOperands().size() == 1;
            return translatedOperands.get( 0 );
        }

    }


    /**
     * Implementor for a value-constructor.
     */
    private static class ValueConstructorImplementor implements CallImplementor {

        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, NullAs nullAs ) {
            return translator.translateConstructor( call.getOperands(), call.getOperator().getKind() );
        }

    }


    /**
     * Implementor for the {@code ITEM} SQL operator.
     */
    private static class ItemImplementor implements CallImplementor {

        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, NullAs nullAs ) {
            final MethodImplementor implementor = getImplementor( call.getOperands().get( 0 ).getType().getPolyType() );
            // Since we follow PostgreSQL's semantics that an out-of-bound reference returns NULL, x[y] can return null even if x and y are both NOT NULL.
            // (In SQL standard semantics, an out-of-bound reference to an array throws an exception.)
            final NullPolicy nullPolicy = NullPolicy.ANY;
            return implementNullSemantics0( translator, call, nullAs, nullPolicy, false, implementor );
        }


        private MethodImplementor getImplementor( PolyType polyType ) {
            return switch ( polyType ) {
                case ARRAY -> new MethodImplementor( BuiltInMethod.ARRAY_ITEM.method );
                case MAP -> new MethodImplementor( BuiltInMethod.MAP_ITEM.method );
                default -> new MethodImplementor( BuiltInMethod.ANY_ITEM.method );
            };
        }

    }


    private static class ElemMatchImplementor implements CallImplementor {

        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, NullAs nullAs ) {
            BlockBuilder builder = new BlockBuilder();

            final ParameterExpression i_ = Expressions.parameter( int.class, "i" );
            final ParameterExpression predicate = Expressions.parameter( boolean.class, "predicate" );
            final ParameterExpression _list = Expressions.parameter( Types.of( List.class, PolyValue.class ), "_list" );
            final ParameterExpression par = Expressions.parameter( PolyValue.class, "_arr" );
            final ParameterExpression get_ = Expressions.parameter( PolyValue.class, "_elem$" );
            builder.add( Expressions.declare( 0, par, translator.translate( call.getOperands().get( 0 ), NullAs.NOT_POSSIBLE, null ) ) );
            builder.add(
                    Expressions.declare( 0, predicate, Expressions.constant( false ) ) );
            builder.add(
                    Expressions.declare( 0, _list, Expressions.call( BuiltInMethod.MQL_GET_ARRAY.method, Expressions.convert_( par, PolyValue.class ) ) )
            );
            BlockStatement _do = Expressions.block(
                    Expressions.declare( 0, get_, Expressions.convert_( Expressions.call( _list, "get", i_ ), PolyValue.class ) ),
                    EnumUtils.ifThen(
                            translator.translate( call.getOperands().get( 1 ), NullAs.NOT_POSSIBLE, null ),
                            Expressions.block( Expressions.return_( null, Expressions.constant( true ) ) ) )
            );

            builder.add( EnumUtils.for_( i_, _list, _do ) );

            builder.add( Expressions.return_( null, predicate ) );
            translator.getList().append( "forLoop", builder.toBlock() );
            return predicate;
        }


        private RexNode substitute( RexToLixTranslator translator, RexNode node, int pos ) {
            RexNode transformed = node;
            if ( node.isA( Kind.LOCAL_REF ) ) {
                transformed = translator.deref( node );
            }
            if ( transformed instanceof RexCall ) {
                int i = 0;

                for ( RexNode operand : ((RexCall) transformed).getOperands() ) {
                    RexNode n = substitute( translator, operand, i );
                    if ( n != null ) {
                        return n;
                    }
                    i++;
                }
            } else if ( pos == 0 ) {
                return node;
            }
            return null;
        }

    }


    private static class CypherImplementor implements CallImplementor {

        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, NullAs nullAs ) {
            // GRAPH, WHERE
            switch ( call.op.getOperatorName() ) {
                // if i0 is list:
                //     throw exception
                // List list = (List) i0;
                // int count = 0;
                // for e in list:
                //    if where(e):
                //      count++;
                // return list.count() == count (ALL)
                // return count == 1 (SINGLE)
                // return count >= 1 (ANY)
                // return count == 0 (NONE)
                case CYPHER_ALL_MATCH:
                case CYPHER_ANY_MATCH:
                case CYPHER_NONE_MATCH:
                case CYPHER_SINGLE_MATCH:
                    BlockBuilder blockBuilder = translator.getList();

                    RexNode list = call.getOperands().get( 0 );

                    Expression list_ = translator.translate( list, NullAs.NULL );
                    blockBuilder.add( EnumUtils.ifThen(
                            Expressions.typeIs( list_, List.class ),
                            Expressions.block(
                                    Expressions.throw_(
                                            Expressions.new_(
                                                    RuntimeException.class,
                                                    Expressions.constant( "The supplied element is no collection to test the predicate against." ) ) ) ) ) );

                    ParameterExpression cList = Expressions.parameter( List.class );
                    blockBuilder.add( Expressions.declare( Modifier.PRIVATE, cList, Expressions.convert_( list_, List.class ) ) );

                    ParameterExpression count_ = Expressions.parameter( int.class, "count_" + System.nanoTime() );
                    blockBuilder.add( Expressions.declare( Modifier.PRIVATE, count_, Expressions.constant( 0 ) ) );

                    ParameterExpression i_ = Expressions.parameter( int.class, "i_" + System.nanoTime() );
                    blockBuilder.add( Expressions.declare( Modifier.PRIVATE, i_, Expressions.constant( 0 ) ) );

                    ConditionalStatement ifIncr = EnumUtils.ifThen( translator.translate( call.operands.get( 1 ) ), Expressions.block( Expressions.statement( Expressions.increment( i_ ) ) ) );

                    blockBuilder.add( EnumUtils.for_( i_, cList, Expressions.block( ifIncr ) ) );

                    ParameterExpression return_ = Expressions.parameter( boolean.class, "return_" );

                    switch ( call.op.getOperatorName() ) {
                        case CYPHER_ALL_MATCH:
                            blockBuilder.add(
                                    Expressions.declare(
                                            Modifier.PRIVATE,
                                            return_,
                                            Expressions.equal( count_, Expressions.call( list_, "size" ) ) ) );

                            return return_;
                        case CYPHER_SINGLE_MATCH:
                            blockBuilder.add(
                                    Expressions.declare(
                                            Modifier.PRIVATE,
                                            return_,
                                            Expressions.equal( count_, Expressions.constant( 1 ) ) ) );

                            return return_;
                        case CYPHER_NONE_MATCH:
                            blockBuilder.add(
                                    Expressions.declare(
                                            Modifier.PRIVATE,
                                            return_,
                                            Expressions.equal( count_, Expressions.constant( 0 ) ) ) );

                            return return_;
                        case CYPHER_ANY_MATCH:
                            blockBuilder.add(
                                    Expressions.declare(
                                            Modifier.PRIVATE,
                                            return_,
                                            Expressions.greaterThanOrEqual( count_, Expressions.constant( 1 ) ) ) );

                            return return_;
                    }

                case CYPHER_PATH_MATCH:
                    throw new UnsupportedOperationException( "Pattern match is not yet supported" );

            }

            throw new UnsupportedOperationException( "Cypher operation was not supported." );
        }

    }


    /**
     * Implementor for SQL system functions.
     * <p>
     * Several of these are represented internally as constant values, set per execution.
     */
    private static class SystemFunctionImplementor implements CallImplementor {

        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, NullAs nullAs ) {
            switch ( nullAs ) {
                case IS_NULL:
                    return Expressions.constant( false );
                case IS_NOT_NULL:
                    return Expressions.constant( true );
            }
            final Operator op = call.getOperator();
            final Expression root = translator.getRoot();
            if ( op.equals( OperatorRegistry.get( OperatorName.CURRENT_USER ) )
                    || op.getOperatorName() == OperatorName.SESSION_USER
                    || op.getOperatorName() == OperatorName.USER ) {
                return Expressions.constant( "sa" );
            } else if ( op.getOperatorName() == OperatorName.SYSTEM_USER ) {
                return Expressions.constant( System.getProperty( "user.name" ) );
            } else if ( op.getOperatorName() == OperatorName.CURRENT_PATH
                    || op.getOperatorName() == OperatorName.CURRENT_ROLE
                    || op.getOperatorName() == OperatorName.CURRENT_CATALOG ) {
                // By default, the CURRENT_ROLE and CURRENT_CATALOG functions return the empty string because a role or a catalog has to be set explicitly.
                return Expressions.constant( "" );
            } else if ( op.getOperatorName() == OperatorName.CURRENT_TIMESTAMP ) {
                return Expressions.call( BuiltInMethod.CURRENT_TIMESTAMP.method, root );
            } else if ( op.getOperatorName() == OperatorName.CURRENT_TIME ) {
                return Expressions.call( BuiltInMethod.CURRENT_TIME.method, root );
            } else if ( op.getOperatorName() == OperatorName.CURRENT_DATE ) {
                return Expressions.call( BuiltInMethod.CURRENT_DATE.method, root );
            } else if ( op.getOperatorName() == OperatorName.LOCALTIMESTAMP ) {
                return Expressions.call( BuiltInMethod.LOCAL_TIMESTAMP.method, root );
            } else if ( op.getOperatorName() == OperatorName.LOCALTIME ) {
                return Expressions.call( BuiltInMethod.LOCAL_TIME.method, root );
            } else {
                throw new AssertionError( "unknown function " + op );
            }
        }

    }


    /**
     * Implements "IS XXX" operations such as "IS NULL" or "IS NOT TRUE".
     * <p>
     * What these operators have in common:
     * 1. They return TRUE or FALSE, never NULL.
     * 2. Of the 3 input values (TRUE, FALSE, NULL) they return TRUE for 1 or 2,
     * FALSE for the other 2 or 1.
     */
    private record IsXxxImplementor(Boolean seek, boolean negate) implements CallImplementor {


        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, NullAs nullAs ) {
            List<RexNode> operands = call.getOperands();
            assert operands.size() == 1;
            switch ( nullAs ) {
                case IS_NOT_NULL:
                    return BOXED_TRUE_EXPR;
                case IS_NULL:
                    return BOXED_FALSE_EXPR;
            }
            if ( seek == null ) {
                return translator.translate( operands.get( 0 ), negate
                        ? NullAs.IS_NOT_NULL
                        : NullAs.IS_NULL );
            } else {
                return maybeNegate(
                        negate == seek,
                        translator.translate(
                                operands.get( 0 ),
                                seek ? NullAs.FALSE : NullAs.TRUE ) );
            }
        }

    }


    /**
     * Implementor for the {@code NOT} operator.
     */
    private record NotImplementor(NotNullImplementor implementor) implements NotNullImplementor {


        static NotNullImplementor of( NotNullImplementor implementor ) {
            return new NotImplementor( implementor );
        }


        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
            final Expression expression = implementor.implement( translator, call, translatedOperands );
            return Expressions.not( expression );
        }

    }


    /**
     * Implementor for various datetime arithmetic.
     */
    private static class DatetimeArithmeticImplementor implements NotNullImplementor {

        @Override
        public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
            final RexNode operand0 = call.getOperands().get( 0 );
            Expression trop0 = translatedOperands.get( 0 );
            final PolyType typeName1 = call.getOperands().get( 1 ).getType().getPolyType();
            Expression trop1 = translatedOperands.get( 1 );
            final PolyType typeName = call.getType().getPolyType();
            switch ( operand0.getType().getPolyType() ) {
                case DATE:
                    if ( Objects.requireNonNull( typeName ) == PolyType.TIMESTAMP ) {
                        trop0 = Expressions.convert_(
                                Expressions.multiply( trop0, Expressions.constant( DateTimeUtils.MILLIS_PER_DAY ) ),
                                long.class );
                    } else {
                        //case INTERVAL -> trop1;
                        trop1 = trop1;
                    }
                    break;
                case TIME:
                    trop1 = Expressions.convert_( trop1, int.class );
                    break;
            }
            if ( Objects.requireNonNull( typeName1 ) == PolyType.INTERVAL ) {
                if ( Objects.requireNonNull( call.getKind() ) == Kind.MINUS ) {
                    trop1 = Expressions.negate( trop1 );
                }
                if ( Objects.requireNonNull( typeName ) == PolyType.TIME ) {
                    return Expressions.convert_( trop0, long.class );
                }
                final BuiltInMethod method =
                        operand0.getType().getPolyType() == PolyType.TIMESTAMP
                                ? BuiltInMethod.ADD_MONTHS
                                : BuiltInMethod.ADD_MONTHS_INT;
                return Expressions.call( method.method, EnumUtils.convertPolyValue( typeName, trop0 ), trop1 );
            }
            return switch ( call.getKind() ) {
                case MINUS -> {
                    if ( Objects.requireNonNull( typeName ) == PolyType.INTERVAL ) {
                        yield Expressions.call( BuiltInMethod.SUBTRACT_MONTHS.method, trop0, trop1 );
                    }
                    TimeUnit fromUnit = typeName1 == PolyType.DATE ? TimeUnit.DAY : TimeUnit.MILLISECOND;
                    TimeUnit toUnit = TimeUnit.MILLISECOND;
                    yield multiplyDivide(
                            Expressions.convert_( Expressions.subtract( trop0, trop1 ), long.class ),
                            fromUnit.multiplier, toUnit.multiplier );
                }
                default -> throw new AssertionError( call );
            };
        }


        /**
         * Normalizes a TIME value into 00:00:00..23:59:39.
         */
        private Expression normalize( PolyType typeName, Expression e ) {
            if ( Objects.requireNonNull( typeName ) == PolyType.TIME ) {
                return Expressions.call( BuiltInMethod.FLOOR_MOD.method, e, Expressions.constant( DateTimeUtils.MILLIS_PER_DAY ) );
            }
            return e;
        }

    }

}


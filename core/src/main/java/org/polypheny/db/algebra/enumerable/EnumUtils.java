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
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ConditionalStatement;
import org.apache.calcite.linq4j.tree.ConstantUntypedNull;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ForStatement;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.Node;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.SemiJoinType;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.impl.Expressible;
import org.polypheny.db.functions.Functions;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgramBuilder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


/**
 * Utilities for generating programs in the Enumerable (functional) style.
 */
public class EnumUtils {

    private EnumUtils() {
    }


    public static final boolean BRIDGE_METHODS = true;

    public static final List<ParameterExpression> NO_PARAMS = ImmutableList.of();

    public static final List<Expression> NO_EXPRS = ImmutableList.of();

    public static final List<String> LEFT_RIGHT = ImmutableList.of( "left", "right" );


    /**
     * Declares a method that overrides another method.
     */
    public static MethodDeclaration overridingMethodDecl( Method method, Iterable<ParameterExpression> parameters, BlockStatement body ) {
        return Expressions.methodDecl( method.getModifiers() & ~Modifier.ABSTRACT, method.getReturnType(), method.getName(), parameters, body );
    }


    static Type javaClass( JavaTypeFactory typeFactory, AlgDataType type ) {
        final Type clazz = typeFactory.getJavaClass( type );
        return clazz instanceof Class ? clazz : PolyValue[].class;
    }


    public static Class<?> javaRowClass( JavaTypeFactory typeFactory, AlgDataType type ) {
        if ( type.isStruct() && type.getFieldCount() == 1 && !PolyType.GRAPH_TYPES.contains( type.getPolyType() ) ) {
            type = type.getFields().get( 0 ).getType();
        }
        final Type clazz = typeFactory.getJavaClass( type );
        return clazz instanceof Class ? (Class<?>) clazz : PolyValue[].class;
    }


    static List<Type> fieldTypes( final JavaTypeFactory typeFactory, final List<? extends AlgDataType> inputTypes ) {
        return new AbstractList<>() {
            @Override
            public Type get( int index ) {
                return EnumUtils.javaClass( typeFactory, inputTypes.get( index ) );
            }


            @Override
            public int size() {
                return inputTypes.size();
            }
        };
    }


    static List<AlgDataType> fieldRowTypes( final AlgDataType inputRowType, final List<? extends RexNode> extraInputs, final List<Integer> argList ) {
        final List<AlgDataTypeField> inputFields = inputRowType.getFields();
        return new AbstractList<>() {
            @Override
            public AlgDataType get( int index ) {
                final int arg = argList.get( index );
                return arg < inputFields.size()
                        ? inputFields.get( arg ).getType()
                        : extraInputs.get( arg - inputFields.size() ).getType();
            }


            @Override
            public int size() {
                return argList.size();
            }
        };
    }


    static Expression joinSelector( SemiJoinType semiJoinType, PhysType physType, List<PhysType> inputPhysTypes ) {
        JoinAlgType joinAlgType;
        if ( semiJoinType.returnsJustFirstInput() ) {
            // Actual join type does not matter much, joinSelector would skip selection of the columns that are not required (see if (expressions.size() == outputFieldCount) {)
            joinAlgType = JoinAlgType.INNER;
        } else {
            joinAlgType = semiJoinType.toJoinType();
        }
        return joinSelector( joinAlgType, physType, inputPhysTypes );
    }


    static Expression joinSelector( JoinAlgType joinType, PhysType physType, List<PhysType> inputPhysTypes ) {
        // A parameter for each input.
        final List<ParameterExpression> parameters = new ArrayList<>();

        // Generate all fields.
        final List<Expression> expressions = new ArrayList<>();
        final int outputFieldCount = physType.getTupleType().getFieldCount();
        for ( Ord<PhysType> ord : Ord.zip( inputPhysTypes ) ) {
            final PhysType inputPhysType = ord.e.makeNullable( joinType.generatesNullsOn( ord.i ) );
            // If input item is just a primitive, we do not generate specialized primitive apply override since it won't be called anyway
            // Function<T> always operates on boxed arguments
            final ParameterExpression parameter = Expressions.parameter( Primitive.box( inputPhysType.getJavaTupleType() ), EnumUtils.LEFT_RIGHT.get( ord.i ) );
            parameters.add( parameter );
            if ( expressions.size() == outputFieldCount ) {
                // For instance, if semi-join needs to return just the left inputs
                break;
            }
            final int fieldCount = inputPhysType.getTupleType().getFieldCount();
            for ( int i = 0; i < fieldCount; i++ ) {
                Expression expression = inputPhysType.fieldReference( parameter, i, physType.getJavaFieldType( expressions.size() ) );
                if ( joinType.generatesNullsOn( ord.i ) ) {
                    expression =
                            EnumUtils.condition(
                                    Expressions.equal( parameter, Expressions.constant( null ) ),
                                    Expressions.constant( null ),//PolyValue.getNull( inputPhysType.field( i ).fieldClass( 0 ) ).asExpression(),
                                    expression );
                }
                expressions.add( expression );
            }
        }
        return Expressions.lambda( Function2.class, physType.record( expressions ), parameters );
    }


    /**
     * Converts from internal representation to JDBC representation used by arguments of user-defined functions. For example, converts date values from {@code int} to {@link java.sql.Date}.
     */
    static Expression fromInternal( Expression e, Class<?> targetType ) {
        if ( e == ConstantUntypedNull.INSTANCE ) {
            return e;
        }
        if ( !(e.getType() instanceof Class) ) {
            return e;
        }
        if ( targetType.isAssignableFrom( (Class<?>) e.getType() ) ) {
            return e;
        }
        if ( targetType == java.sql.Date.class ) {
            return Expressions.call( BuiltInMethod.INTERNAL_TO_DATE.method, e );
        }
        if ( targetType == java.sql.Time.class ) {
            return Expressions.call( BuiltInMethod.INTERNAL_TO_TIME.method, e );
        }
        if ( targetType == java.sql.Timestamp.class ) {
            return Expressions.call( BuiltInMethod.INTERNAL_TO_TIMESTAMP.method, e );
        }
        if ( Primitive.is( e.type ) && Primitive.isBox( targetType ) ) {
            // E.g. e is "int", target is "Long", generate "(long) e".
            return Expressions.convert_( e, Primitive.ofBox( targetType ).primitiveClass );
        }
        return e;
    }


    static List<Expression> fromInternal( Class<?>[] targetTypes, List<Expression> expressions ) {
        final List<Expression> list = new ArrayList<>();
        for ( int i = 0; i < expressions.size(); i++ ) {
            list.add( fromInternal( expressions.get( i ), targetTypes[i] ) );
        }
        return list;
    }


    static Type fromInternal( Type type ) {
        if ( type == java.sql.Date.class || type == java.sql.Time.class ) {
            return int.class;
        }
        if ( type == java.sql.Timestamp.class ) {
            return long.class;
        }
        return type;
    }



    static Expression enforce( final Type storageType, final Expression e ) {
        if ( storageType != null && e.type != storageType ) {
            if ( e.type == java.sql.Date.class ) {
                if ( storageType == int.class ) {
                    return Expressions.call( BuiltInMethod.DATE_TO_LONG.method, e );
                }
                if ( storageType == Integer.class ) {
                    return Expressions.call( BuiltInMethod.DATE_TO_LONG_OPTIONAL.method, e );
                }
            } else if ( e.type == java.sql.Time.class ) {
                if ( storageType == int.class ) {
                    return Expressions.call( BuiltInMethod.TIME_TO_LONG.method, e );
                }
                if ( storageType == Integer.class ) {
                    return Expressions.call( BuiltInMethod.TIME_TO_LONG_OPTIONAL.method, e );
                }
            } else if ( e.type == java.sql.Timestamp.class ) {
                if ( storageType == long.class ) {
                    return Expressions.call( BuiltInMethod.DATE_TO_LONG.method, e );
                }
                if ( storageType == Long.class ) {
                    return Expressions.call( BuiltInMethod.DATE_TO_LONG_OPTIONAL.method, e );
                }
            }
        }
        return e;
    }


    /**
     * Helper method to create a "for-loop" in an enumerable, which iterates over a specified list.
     * <code>{@code for( int i = 0; i < list.size(); i++ ){ [BlockStatement] } }</code>
     *
     * @param i_ the iterator variable
     * @param _list the list to iterate over
     * @param statement the statements, which are used inside the for loop
     * @return the loop as an enumerable statement
     */
    public static ForStatement for_( ParameterExpression i_, ParameterExpression _list, BlockStatement statement ) {
        return Expressions.for_(
                Expressions.declare(
                        0, i_, Expressions.constant( 0 ) ),
                Expressions.lessThan( i_, Expressions.call( _list, "size" ) ),
                Expressions.postIncrementAssign( i_ ),
                statement
        );
    }


    /**
     * E.g. {@code constantArrayList("x", "y")} returns "Arrays.asList('x', 'y')".
     *
     * @param values List of values
     * @param clazz Type of values
     * @return expression
     */
    public static <T> MethodCallExpression constantArrayList( List<T> values, Type clazz ) {
        return Expressions.call( BuiltInMethod.ARRAYS_AS_LIST.method, Expressions.newArrayInit( clazz, constantList( values ) ) );
    }


    public static MethodCallExpression expressionList( List<Expression> expressions ) {
        return Expressions.call( BuiltInMethod.ARRAYS_AS_LIST.method, expressions );
    }


    public static Expression expressionFlatList( List<Expression> expressions, Class<?> clazz ) {
        List<Expression> list = new ArrayList<>( expressions );
        return Expressions.convert_( Expressions.call( BuiltInMethod.ARRAYS_AS_LIST.method, list ), clazz );
    }


    /**
     * E.g. {@code constantList("x", "y")} returns {@code {ConstantExpression("x"), ConstantExpression("y")}}.
     */
    public static <T> List<Expression> constantList( List<T> values ) {
        return values.stream().map( Expressions::constant ).collect( Collectors.toList() );
    }


    public static <T> Expression getExpression( T value, Class<T> clazz ) {
        if ( value instanceof PolyDictionary ) {
            return ((PolyDictionary) value).asExpression();
        } else if ( value instanceof PolyList<?> ) {
            return ((PolyList<?>) value).asExpression();
        }
        return Expressions.constant( value, clazz );
    }


    @SafeVarargs
    @SuppressWarnings("unused")
    public static Map<Object, Object> ofEntries( Pair<Object, Object>... pairs ) {
        return new HashMap<>( Map.ofEntries( pairs ) );
    }


    public static Expression foldAnd( List<Expression> expressions ) {
        return Expressions.call( PolyBoolean.class, "of", Expressions.foldAnd( expressions.stream().map( e -> e.type == PolyBoolean.class ? Expressions.field( e, "value" ) : e ).collect( Collectors.toList() ) ) );
    }


    public static Expression foldOr( List<Expression> expressions ) {
        return Expressions.call( PolyBoolean.class, "of", Expressions.foldOr( expressions.stream().map( e -> e.type == PolyBoolean.class ? Expressions.field( e, "value" ) : e ).collect( Collectors.toList() ) ) );
    }


    public static Expression not( Expression expression ) {
        return Expressions.call( Functions.class, "not", Expressions.convert_( expression, PolyBoolean.class ) );

    }


    public static ConditionalStatement ifThen( Expression condition, Node ifTrue ) {
        return Expressions.ifThen( unwrapPoly( condition ), ifTrue );
    }


    public static ConditionalStatement ifThenElse( Expression condition, Node ifTrue, Node ifFalse ) {
        return Expressions.ifThenElse( unwrapPoly( condition ), ifTrue, ifFalse );
    }


    public static Expression condition( Expression test, Expression ifTrue, Expression ifFalse ) {
        return Expressions.condition( unwrapPoly( test ), ifTrue, ifFalse );
    }


    public static Expression makeTernary( ExpressionType ternaryType, Expression e0, Expression e1, Expression e2 ) {
        return Expressions.makeTernary( ternaryType, unwrapPoly( e0 ), e1, e2 );
    }


    public static Expression unwrapPoly( Expression expression ) {
        return expression.type == PolyBoolean.class ? Expressions.convert_( Expressions.field( expression, "value" ), boolean.class ) : expression;
    }


    @NotNull
    public static MethodCallExpression unwrapPolyValue( Expression num, String methodName ) {
        return Expressions.call( num, methodName );
    }


    @NotNull
    public static Expression convertPolyValue( PolyType outputType, Expression operand ) {
        Class<?> clazz = PolyValue.classFrom( outputType );
        if ( Types.isAssignableFrom( clazz, operand.type ) ) {
            return operand;
        }
        return Expressions.call( clazz, "convert", operand );
    }


    @NotNull
    public static Expression convertPolyValue( Type type, Expression operand ) {
        return Expressions.call( type, "convert", operand );
    }


    @NotNull
    public static MethodCallExpression wrapPolyValue( PolyType outputType, Expression operand ) {
        return wrapPolyValue( PolyValue.classFrom( outputType ), operand );
    }


    @NotNull
    public static MethodCallExpression wrapPolyValue( Type outputClass, Expression operand ) {
        return Expressions.call( outputClass, "of", operand );
    }


    /**
     * Returns a predicate expression based on a join condition.
     */
    static Expression generatePredicate(
            EnumerableAlgImplementor implementor,
            RexBuilder rexBuilder,
            AlgNode left,
            AlgNode right,
            PhysType leftPhysType,
            PhysType rightPhysType,
            RexNode condition ) {
        final BlockBuilder builder = new BlockBuilder();
        final ParameterExpression left_ = Expressions.parameter( leftPhysType.getJavaTupleType(), "left" );
        final ParameterExpression right_ = Expressions.parameter( rightPhysType.getJavaTupleType(), "right" );
        final RexProgramBuilder program = new RexProgramBuilder(
                implementor.getTypeFactory().builder()
                        .addAll( left.getTupleType().getFields() )
                        .addAll( right.getTupleType().getFields() )
                        .build(),
                rexBuilder );
        program.addCondition( condition );
        builder.add(
                Expressions.return_( null,
                        Expressions.convert_( Expressions.field( RexToLixTranslator.translateCondition( program.getProgram(),
                                        implementor.getTypeFactory(),
                                        builder,
                                        new RexToLixTranslator.InputGetterImpl( ImmutableList.of( Pair.of( left_, leftPhysType ), Pair.of( right_, rightPhysType ) ) ),
                                        implementor.allCorrelateVariables,
                                        implementor.getConformance() ), "value" ),
                                boolean.class ) ) );
        return Expressions.lambda( Predicate2.class, builder.toBlock(), left_, right_ );
    }


    public static Expression convertAlgFields( List<AlgDataTypeField> fields ) {
        return Expressions.newArrayInit( PolyType.class, fields.stream().map( Expressible::asExpression ).toList() );
    }

}


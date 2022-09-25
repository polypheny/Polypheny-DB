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

package org.polypheny.db.adapter.enumerable;


import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.ConstantUntypedNull;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ForStatement;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.MethodDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.constant.SemiJoinType;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.PolyCollections.PolyDictionary;
import org.polypheny.db.runtime.PolyCollections.PolyList;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Util;


/**
 * Utilities for generating programs in the Enumerable (functional) style.
 */
public class EnumUtils {

    private EnumUtils() {
    }


    static final boolean BRIDGE_METHODS = true;

    static final List<ParameterExpression> NO_PARAMS = ImmutableList.of();

    static final List<Expression> NO_EXPRS = ImmutableList.of();

    public static final List<String> LEFT_RIGHT = ImmutableList.of( "left", "right" );


    /**
     * Declares a method that overrides another method.
     */
    public static MethodDeclaration overridingMethodDecl( Method method, Iterable<ParameterExpression> parameters, BlockStatement body ) {
        return Expressions.methodDecl( method.getModifiers() & ~Modifier.ABSTRACT, method.getReturnType(), method.getName(), parameters, body );
    }


    static Type javaClass( JavaTypeFactory typeFactory, AlgDataType type ) {
        final Type clazz = typeFactory.getJavaClass( type );
        return clazz instanceof Class ? clazz : Object[].class;
    }


    public static Class javaRowClass( JavaTypeFactory typeFactory, AlgDataType type ) {
        if ( type.isStruct() && type.getFieldCount() == 1 ) {
            type = type.getFieldList().get( 0 ).getType();
        }
        final Type clazz = typeFactory.getJavaClass( type );
        return clazz instanceof Class ? (Class) clazz : Object[].class;
    }


    static List<Type> fieldTypes( final JavaTypeFactory typeFactory, final List<? extends AlgDataType> inputTypes ) {
        return new AbstractList<Type>() {
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
        final List<AlgDataTypeField> inputFields = inputRowType.getFieldList();
        return new AbstractList<AlgDataType>() {
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
        final int outputFieldCount = physType.getRowType().getFieldCount();
        for ( Ord<PhysType> ord : Ord.zip( inputPhysTypes ) ) {
            final PhysType inputPhysType = ord.e.makeNullable( joinType.generatesNullsOn( ord.i ) );
            // If input item is just a primitive, we do not generate specialized primitive apply override since it won't be called anyway
            // Function<T> always operates on boxed arguments
            final ParameterExpression parameter = Expressions.parameter( Primitive.box( inputPhysType.getJavaRowType() ), EnumUtils.LEFT_RIGHT.get( ord.i ) );
            parameters.add( parameter );
            if ( expressions.size() == outputFieldCount ) {
                // For instance, if semi-join needs to return just the left inputs
                break;
            }
            final int fieldCount = inputPhysType.getRowType().getFieldCount();
            for ( int i = 0; i < fieldCount; i++ ) {
                Expression expression = inputPhysType.fieldReference( parameter, i, physType.getJavaFieldType( expressions.size() ) );
                if ( joinType.generatesNullsOn( ord.i ) ) {
                    expression =
                            Expressions.condition(
                                    Expressions.equal( parameter, Expressions.constant( null ) ),
                                    Expressions.constant( null ),
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
        if ( targetType.isAssignableFrom( (Class) e.getType() ) ) {
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


    static Type toInternal( AlgDataType type ) {
        switch ( type.getPolyType() ) {
            case DATE:
            case TIME:
                return type.isNullable() ? Integer.class : int.class;
            case TIMESTAMP:
                return type.isNullable() ? Long.class : long.class;
            default:
                return null; // we don't care; use the default storage type
        }
    }


    static List<Type> internalTypes( List<? extends RexNode> operandList ) {
        return Util.transform( operandList, node -> toInternal( node.getType() ) );
    }


    static Expression enforce( final Type storageType, final Expression e ) {
        if ( storageType != null && e.type != storageType ) {
            if ( e.type == java.sql.Date.class ) {
                if ( storageType == int.class ) {
                    return Expressions.call( BuiltInMethod.DATE_TO_INT.method, e );
                }
                if ( storageType == Integer.class ) {
                    return Expressions.call( BuiltInMethod.DATE_TO_INT_OPTIONAL.method, e );
                }
            } else if ( e.type == java.sql.Time.class ) {
                if ( storageType == int.class ) {
                    return Expressions.call( BuiltInMethod.TIME_TO_INT.method, e );
                }
                if ( storageType == Integer.class ) {
                    return Expressions.call( BuiltInMethod.TIME_TO_INT_OPTIONAL.method, e );
                }
            } else if ( e.type == java.sql.Timestamp.class ) {
                if ( storageType == long.class ) {
                    return Expressions.call( BuiltInMethod.TIMESTAMP_TO_LONG.method, e );
                }
                if ( storageType == Long.class ) {
                    return Expressions.call( BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL.method, e );
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
    public static <T> MethodCallExpression constantArrayList( List<T> values, Class<?> clazz ) {
        return Expressions.call( BuiltInMethod.ARRAYS_AS_LIST.method, Expressions.newArrayInit( clazz, constantList( values ) ) );
    }


    public static MethodCallExpression expressionList( List<Expression> expressions ) {
        return Expressions.call( BuiltInMethod.ARRAYS_AS_LIST.method, expressions );
    }


    /**
     * E.g. {@code constantList("x", "y")} returns {@code {ConstantExpression("x"), ConstantExpression("y")}}.
     */
    public static <T> List<Expression> constantList( List<T> values ) {
        return values.stream().map( Expressions::constant ).collect( Collectors.toList() );
    }


    public static <T> Expression getExpression( T value, Class<T> clazz ) {
        if ( value instanceof PolyDictionary ) {
            return ((PolyDictionary) value).getAsExpression();
        } else if ( value instanceof PolyList ) {
            return ((PolyList<?>) value).getAsExpression();
        }
        return Expressions.constant( value, clazz );
    }

}


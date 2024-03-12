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

package org.polypheny.db.schema.impl;


import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.enumerable.CallImplementor;
import org.polypheny.db.algebra.enumerable.NullPolicy;
import org.polypheny.db.algebra.enumerable.ReflectiveCallNotNullImplementor;
import org.polypheny.db.algebra.enumerable.RexImpTable;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.schema.Entity;
import org.polypheny.db.schema.ImplementableFunction;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.TableFunction;
import org.polypheny.db.schema.types.QueryableEntity;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Static;


/**
 * Implementation of {@link TableFunction} based on a method.
 */
public class TableFunctionImpl extends ReflectiveFunctionBase implements TableFunction, ImplementableFunction {

    private final CallImplementor implementor;


    /**
     * Private constructor; use {@link #create}.
     */
    private TableFunctionImpl( Method method, CallImplementor implementor ) {
        super( method );
        this.implementor = implementor;
    }


    /**
     * Creates a {@link TableFunctionImpl} from a class, looking for an "eval" method. Returns null if there is no such method.
     */
    public static TableFunction create( Class<?> clazz ) {
        return create( clazz, "eval" );
    }


    /**
     * Creates a {@link TableFunctionImpl} from a class, looking for a method with a given name. Returns null if there is no such method.
     */
    public static TableFunction create( Class<?> clazz, String methodName ) {
        final Method method = findMethod( clazz, methodName );
        if ( method == null ) {
            return null;
        }
        return create( method );
    }


    /**
     * Creates a {@link TableFunctionImpl} from a method.
     */
    public static TableFunction create( final Method method ) {
        if ( !Modifier.isStatic( method.getModifiers() ) ) {
            Class<?> clazz = method.getDeclaringClass();
            if ( !classHasPublicZeroArgsConstructor( clazz ) ) {
                throw Static.RESOURCE.requireDefaultConstructor( clazz.getName() ).ex();
            }
        }
        final Class<?> returnType = method.getReturnType();
        if ( !QueryableEntity.class.isAssignableFrom( returnType ) && !ScannableEntity.class.isAssignableFrom( returnType ) ) {
            return null;
        }
        CallImplementor implementor = createImplementor( method );
        return new TableFunctionImpl( method, implementor );
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory, List<Object> arguments ) {
        return apply( arguments ).getTupleType( typeFactory );
    }


    @Override
    public Type getElementType( List<Object> arguments ) {
        final Entity entity = apply( arguments );
        if ( entity instanceof QueryableEntity queryableTable ) {
            return queryableTable.getElementType();
        } else if ( entity instanceof ScannableEntity ) {
            return Object[].class;
        }
        throw new AssertionError( "Invalid table class: " + entity + " " + entity.getClass() );
    }


    @Override
    public CallImplementor getImplementor() {
        return implementor;
    }


    private static CallImplementor createImplementor( final Method method ) {
        return RexImpTable.createImplementor(
                new ReflectiveCallNotNullImplementor( method ) {
                    @Override
                    public Expression implement( RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands ) {
                        Expression expr = super.implement( translator, call, translatedOperands );
                        final Class<?> returnType = method.getReturnType();
                        if ( QueryableEntity.class.isAssignableFrom( returnType ) ) {
                            Expression queryable = Expressions.call(
                                    Expressions.convert_( expr, QueryableEntity.class ),
                                    BuiltInMethod.QUERYABLE_TABLE_AS_QUERYABLE.method,
                                    Expressions.call( DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_QUERY_PROVIDER.method ),
                                    Expressions.constant( null, SchemaPlus.class )
                                    /*Expressions.constant( call.getOperator().getName(), String.class )*/ );
                            expr = Expressions.call( queryable, BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method );
                        } else {
                            expr = Expressions.call( expr, BuiltInMethod.SCANNABLE_TABLE_SCAN.method, DataContext.ROOT );
                        }
                        return expr;
                    }
                }, NullPolicy.ANY, false );
    }


    private Entity apply( List<Object> arguments ) {
        try {
            Object o = null;
            if ( !Modifier.isStatic( method.getModifiers() ) ) {
                final Constructor<?> constructor = method.getDeclaringClass().getConstructor();
                o = constructor.newInstance();
            }
            final Object table = method.invoke( o, arguments.toArray() );
            return (Entity) table;
        } catch ( IllegalArgumentException e ) {
            throw Static.RESOURCE.illegalArgumentForTableFunctionCall(
                    method.toString(),
                    Arrays.toString( method.getParameterTypes() ),
                    arguments.toString() ).ex( e );
        } catch ( IllegalAccessException | InvocationTargetException | InstantiationException | NoSuchMethodException e ) {
            throw new GenericRuntimeException( e );
        }
    }

}


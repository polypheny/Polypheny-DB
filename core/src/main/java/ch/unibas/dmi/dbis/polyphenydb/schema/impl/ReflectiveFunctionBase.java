/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.schema.impl;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.Function;
import ch.unibas.dmi.dbis.polyphenydb.schema.FunctionParameter;
import ch.unibas.dmi.dbis.polyphenydb.util.ReflectUtil;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;


/**
 * Implementation of a function that is based on a method.
 * This class mainly solves conversion of method parameter types to {@code List<FunctionParameter>} form.
 */
public abstract class ReflectiveFunctionBase implements Function {

    /**
     * Method that implements the function.
     */
    public final Method method;
    /**
     * Types of parameter for the function call.
     */
    public final List<FunctionParameter> parameters;


    /**
     * {@code ReflectiveFunctionBase} constructor
     *
     * @param method method that is used to get type information from
     */
    public ReflectiveFunctionBase( Method method ) {
        this.method = method;
        this.parameters = builder().addMethodParameters( method ).build();
    }


    /**
     * Returns the parameters of this function.
     *
     * @return Parameters; never null
     */
    @Override
    public List<FunctionParameter> getParameters() {
        return parameters;
    }


    /**
     * Verifies if given class has public constructor with zero arguments.
     *
     * @param clazz class to verify
     * @return true if given class has public constructor with zero arguments
     */
    static boolean classHasPublicZeroArgsConstructor( Class<?> clazz ) {
        for ( Constructor<?> constructor : clazz.getConstructors() ) {
            if ( constructor.getParameterTypes().length == 0 && Modifier.isPublic( constructor.getModifiers() ) ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Finds a method in a given class by name.
     *
     * @param clazz class to search method in
     * @param name name of the method to find
     * @return the first method with matching name or null when no method found
     */
    static Method findMethod( Class<?> clazz, String name ) {
        for ( Method method : clazz.getMethods() ) {
            if ( method.getName().equals( name ) && !method.isBridge() ) {
                return method;
            }
        }
        return null;
    }


    /**
     * Creates a ParameterListBuilder.
     */
    public static ParameterListBuilder builder() {
        return new ParameterListBuilder();
    }


    /**
     * Helps build lists of {@link FunctionParameter}.
     */
    public static class ParameterListBuilder {

        final List<FunctionParameter> builder = new ArrayList<>();


        public ImmutableList<FunctionParameter> build() {
            return ImmutableList.copyOf( builder );
        }


        public ParameterListBuilder add( final Class<?> type, final String name ) {
            return add( type, name, false );
        }


        public ParameterListBuilder add( final Class<?> type, final String name, final boolean optional ) {
            final int ordinal = builder.size();
            builder.add(
                    new FunctionParameter() {
                        @Override
                        public int getOrdinal() {
                            return ordinal;
                        }


                        @Override
                        public String getName() {
                            return name;
                        }


                        @Override
                        public RelDataType getType( RelDataTypeFactory typeFactory ) {
                            return typeFactory.createJavaType( type );
                        }


                        @Override
                        public boolean isOptional() {
                            return optional;
                        }
                    } );
            return this;
        }


        public ParameterListBuilder addMethodParameters( Method method ) {
            final Class<?>[] types = method.getParameterTypes();
            for ( int i = 0; i < types.length; i++ ) {
                add( types[i], ReflectUtil.getParameterName( method, i ), ReflectUtil.isParameterOptional( method, i ) );
            }
            return this;
        }
    }
}


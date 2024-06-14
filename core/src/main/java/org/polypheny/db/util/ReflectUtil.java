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

package org.polypheny.db.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.function.Parameter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;


/**
 * Static utilities for Java reflection.
 */
public abstract class ReflectUtil {

    private static final Map<Class<?>, Class<?>> primitiveToBoxingMap;
    private static final Map<Class<?>, Method> primitiveToByteBufferReadMethod;
    private static final Map<Class<?>, Method> primitiveToByteBufferWriteMethod;


    static {
        primitiveToBoxingMap = new HashMap<>();
        primitiveToBoxingMap.put( Boolean.TYPE, Boolean.class );
        primitiveToBoxingMap.put( Byte.TYPE, Byte.class );
        primitiveToBoxingMap.put( Character.TYPE, Character.class );
        primitiveToBoxingMap.put( Double.TYPE, Double.class );
        primitiveToBoxingMap.put( Float.TYPE, Float.class );
        primitiveToBoxingMap.put( Integer.TYPE, Integer.class );
        primitiveToBoxingMap.put( Long.TYPE, Long.class );
        primitiveToBoxingMap.put( Short.TYPE, Short.class );

        primitiveToByteBufferReadMethod = new HashMap<>();
        primitiveToByteBufferWriteMethod = new HashMap<>();
        Method[] methods = ByteBuffer.class.getDeclaredMethods();
        for ( Method method : methods ) {
            Class<?>[] paramTypes = method.getParameterTypes();
            if ( method.getName().startsWith( "get" ) ) {
                if ( !method.getReturnType().isPrimitive() ) {
                    continue;
                }
                if ( paramTypes.length != 1 ) {
                    continue;
                }
                primitiveToByteBufferReadMethod.put( method.getReturnType(), method );

                // special case for Boolean:  treat as byte
                if ( method.getReturnType().equals( Byte.TYPE ) ) {
                    primitiveToByteBufferReadMethod.put( Boolean.TYPE, method );
                }
            } else if ( method.getName().startsWith( "put" ) ) {
                if ( paramTypes.length != 2 ) {
                    continue;
                }
                if ( !paramTypes[1].isPrimitive() ) {
                    continue;
                }
                primitiveToByteBufferWriteMethod.put( paramTypes[1], method );

                // special case for Boolean:  treat as byte
                if ( paramTypes[1].equals( Byte.TYPE ) ) {
                    primitiveToByteBufferWriteMethod.put( Boolean.TYPE, method );
                }
            }
        }
    }


    /**
     * Gets the name of a class with no package qualifiers; if it's an inner class, it will still be qualified by the containing class (X$Y).
     *
     * @param c the class of interest
     * @return the unqualified name
     */
    public static String getUnqualifiedClassName( Class<?> c ) {
        String className = c.getName();
        int lastDot = className.lastIndexOf( '.' );
        if ( lastDot < 0 ) {
            return className;
        }
        return className.substring( lastDot + 1 );
    }



    /**
     * Derives the name of the {@code i}th parameter of a method.
     */
    public static String getParameterName( Method method, int i ) {
        for ( Annotation annotation : method.getParameterAnnotations()[i] ) {
            if ( annotation.annotationType() == Parameter.class ) {
                return ((Parameter) annotation).name();
            }
        }
        return method.getParameters()[i].getName();
    }


    /**
     * Derives whether the {@code i}th parameter of a method is optional.
     */
    public static boolean isParameterOptional( Method method, int i ) {
        for ( Annotation annotation : method.getParameterAnnotations()[i] ) {
            if ( annotation.annotationType() == Parameter.class ) {
                return ((Parameter) annotation).optional();
            }
        }
        return false;
    }


}


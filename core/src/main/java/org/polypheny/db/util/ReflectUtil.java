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
     * Uses reflection to find the correct java.nio.ByteBuffer "absolute get" method for a given primitive type.
     *
     * @param clazz the Class object representing the primitive type
     * @return corresponding method
     */
    public static Method getByteBufferReadMethod( Class<?> clazz ) {
        assert clazz.isPrimitive();
        return primitiveToByteBufferReadMethod.get( clazz );
    }


    /**
     * Uses reflection to find the correct java.nio.ByteBuffer "absolute put" method for a given primitive type.
     *
     * @param clazz the Class object representing the primitive type
     * @return corresponding method
     */
    public static Method getByteBufferWriteMethod( Class<?> clazz ) {
        assert clazz.isPrimitive();
        return primitiveToByteBufferWriteMethod.get( clazz );
    }


    /**
     * Gets the Java boxing class for a primitive class.
     *
     * @param primitiveClass representative class for primitive (e.g. java.lang.Integer.TYPE)
     * @return corresponding boxing Class (e.g. java.lang.Integer)
     */
    public static Class<?> getBoxingClass( Class<?> primitiveClass ) {
        assert primitiveClass.isPrimitive();
        return primitiveToBoxingMap.get( primitiveClass );
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
     * Composes a string representing a human-readable method name (with neither exception nor return type information).
     *
     * @param declaringClass class on which method is defined
     * @param methodName simple name of method without signature
     * @param paramTypes method parameter types
     * @return unmangled method name
     */
    public static String getUnmangledMethodName( Class<?> declaringClass, String methodName, Class<?>[] paramTypes ) {
        StringBuilder sb = new StringBuilder();
        sb.append( declaringClass.getName() );
        sb.append( "." );
        sb.append( methodName );
        sb.append( "(" );
        for ( int i = 0; i < paramTypes.length; ++i ) {
            if ( i > 0 ) {
                sb.append( ", " );
            }
            sb.append( paramTypes[i].getName() );
        }
        sb.append( ")" );
        return sb.toString();
    }


    /**
     * Composes a string representing a human-readable method name (with neither exception nor return type information).
     *
     * @param method method whose name is to be generated
     * @return unmangled method name
     */
    public static String getUnmangledMethodName( Method method ) {
        return getUnmangledMethodName( method.getDeclaringClass(), method.getName(), method.getParameterTypes() );
    }


    /**
     * Shared implementation of the two forms of invokeVisitor.
     *
     * @param visitor object whose visit method is to be invoked
     * @param visitee object to be passed as a parameter to the visit method
     * @param hierarchyRoot if non-null, visitor method will only be invoked if it takes a parameter whose type is a subtype of hierarchyRoot
     * @param visitMethodName name of visit method, e.g. "visit"
     * @return true if a matching visit method was found and invoked
     */
    private static boolean invokeVisitorInternal( Object visitor, Object visitee, Class<?> hierarchyRoot, String visitMethodName ) {
        Class<?> visitorClass = visitor.getClass();
        Class<?> visiteeClass = visitee.getClass();
        Method method = lookupVisitMethod( visitorClass, visiteeClass, visitMethodName );
        if ( method == null ) {
            return false;
        }

        if ( hierarchyRoot != null ) {
            Class<?> paramType = method.getParameterTypes()[0];
            if ( !hierarchyRoot.isAssignableFrom( paramType ) ) {
                return false;
            }
        }

        try {
            method.invoke( visitor, visitee );
        } catch ( IllegalAccessException ex ) {
            throw new GenericRuntimeException( ex );
        } catch ( InvocationTargetException ex ) {
            // visit methods aren't allowed to have throws clauses, so the only exceptions which should come
            // to us are RuntimeExceptions and Errors
            Util.throwIfUnchecked( ex.getTargetException() );
            throw new GenericRuntimeException( ex.getTargetException() );
        }
        return true;
    }


    /**
     * Looks up a visit method.
     *
     * @param visitorClass class of object whose visit method is to be invoked
     * @param visiteeClass class of object to be passed as a parameter to the visit method
     * @param visitMethodName name of visit method
     * @return method found, or null if none found
     */
    public static Method lookupVisitMethod( Class<?> visitorClass, Class<?> visiteeClass, String visitMethodName ) {
        return lookupVisitMethod( visitorClass, visiteeClass, visitMethodName, Collections.emptyList() );
    }


    /**
     * Looks up a visit method taking additional parameters beyond the overloaded visitee type.
     *
     * @param visitorClass class of object whose visit method is to be invoked
     * @param visiteeClass class of object to be passed as a parameter to the visit method
     * @param visitMethodName name of visit method
     * @param additionalParameterTypes list of additional parameter types
     * @return method found, or null if none found
     */
    public static Method lookupVisitMethod( Class<?> visitorClass, Class<?> visiteeClass, String visitMethodName, List<Class<?>> additionalParameterTypes ) {
        // Prepare an array to re-use in recursive calls.  The first argument will have the visitee class substituted into it.
        Class<?>[] paramTypes = new Class[1 + additionalParameterTypes.size()];
        int iParam = 0;
        paramTypes[iParam++] = null;
        for ( Class<?> paramType : additionalParameterTypes ) {
            paramTypes[iParam++] = paramType;
        }

        // Cache Class to candidate Methods, to optimize the case where the original visiteeClass has a diamond-shaped interface inheritance
        // graph. (This is common, for example, in JMI.) The idea is to avoid iterating over a single interface's method more than once in a call.
        Map<Class<?>, Method> cache = new HashMap<>();

        return lookupVisitMethod(
                visitorClass,
                visiteeClass,
                visitMethodName,
                paramTypes,
                cache );
    }


    private static Method lookupVisitMethod( final Class<?> visitorClass, final Class<?> visiteeClass, final String visitMethodName, final Class<?>[] paramTypes, final Map<Class<?>, Method> cache ) {
        // Use containsKey since the result for a Class might be null.
        if ( cache.containsKey( visiteeClass ) ) {
            return cache.get( visiteeClass );
        }

        Method candidateMethod = null;

        paramTypes[0] = visiteeClass;

        try {
            candidateMethod = visitorClass.getMethod( visitMethodName, paramTypes );

            cache.put( visiteeClass, candidateMethod );

            return candidateMethod;
        } catch ( NoSuchMethodException ex ) {
            // not found:  carry on with lookup
        }

        Class<?> superClass = visiteeClass.getSuperclass();
        if ( superClass != null ) {
            candidateMethod = lookupVisitMethod( visitorClass, superClass, visitMethodName, paramTypes, cache );
        }

        Class<?>[] interfaces = visiteeClass.getInterfaces();
        for ( Class<?> anInterface : interfaces ) {
            final Method method = lookupVisitMethod( visitorClass, anInterface, visitMethodName, paramTypes, cache );
            if ( method != null ) {
                if ( candidateMethod != null ) {
                    if ( !method.equals( candidateMethod ) ) {
                        Class<?> c1 = method.getParameterTypes()[0];
                        Class<?> c2 = candidateMethod.getParameterTypes()[0];
                        if ( c1.isAssignableFrom( c2 ) ) {
                            // c2 inherits from c1, so keep candidateMethod (which is more specific than method)
                            continue;
                        } else if ( c2.isAssignableFrom( c1 ) ) {
                            // c1 inherits from c2 (method is more specific than candidate method), so fall through to set candidateMethod = method
                        } else {
                            // c1 and c2 are not directly related
                            throw new IllegalArgumentException( "dispatch ambiguity between " + candidateMethod + " and " + method );
                        }
                    }
                }
                candidateMethod = method;
            }
        }

        cache.put( visiteeClass, candidateMethod );

        return candidateMethod;
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


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


import ch.unibas.dmi.dbis.polyphenydb.schema.TableMacro;
import ch.unibas.dmi.dbis.polyphenydb.schema.TranslatableTable;
import ch.unibas.dmi.dbis.polyphenydb.util.Static;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;


/**
 * Implementation of {@link TableMacro} based on a method.
 */
public class TableMacroImpl extends ReflectiveFunctionBase implements TableMacro {

    /**
     * Private constructor; use {@link #create}.
     */
    private TableMacroImpl( Method method ) {
        super( method );
    }


    /**
     * Creates a {@code TableMacro} from a class, looking for an "eval" method. Returns null if there is no such method.
     */
    public static TableMacro create( Class<?> clazz ) {
        final Method method = findMethod( clazz, "eval" );
        if ( method == null ) {
            return null;
        }
        return create( method );
    }


    /**
     * Creates a {@code TableMacro} from a method.
     */
    public static TableMacro create( final Method method ) {
        Class clazz = method.getDeclaringClass();
        if ( !Modifier.isStatic( method.getModifiers() ) ) {
            if ( !classHasPublicZeroArgsConstructor( clazz ) ) {
                throw Static.RESOURCE.requireDefaultConstructor( clazz.getName() ).ex();
            }
        }
        final Class<?> returnType = method.getReturnType();
        if ( !TranslatableTable.class.isAssignableFrom( returnType ) ) {
            return null;
        }
        return new TableMacroImpl( method );
    }


    /**
     * Applies arguments to yield a table.
     *
     * @param arguments Arguments
     * @return Table
     */
    @Override
    public TranslatableTable apply( List<Object> arguments ) {
        try {
            Object o = null;
            if ( !Modifier.isStatic( method.getModifiers() ) ) {
                final Constructor<?> constructor = method.getDeclaringClass().getConstructor();
                o = constructor.newInstance();
            }
            return (TranslatableTable) method.invoke( o, arguments.toArray() );
        } catch ( IllegalArgumentException e ) {
            throw new RuntimeException( "Expected " + Arrays.toString( method.getParameterTypes() ) + " actual " + arguments, e );
        } catch ( IllegalAccessException | InvocationTargetException | NoSuchMethodException | InstantiationException e ) {
            throw new RuntimeException( e );
        }
    }
}


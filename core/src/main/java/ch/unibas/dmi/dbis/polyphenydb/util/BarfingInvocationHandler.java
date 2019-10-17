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

package ch.unibas.dmi.dbis.polyphenydb.util;


import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;


/**
 * A class derived from <code>BarfingInvocationHandler</code> handles a method call by looking for a method in itself with identical parameters. If no such method is found, it throws {@link UnsupportedOperationException}.
 *
 * It is useful when you are prototyping code. You can rapidly create a prototype class which implements the important methods in an interface, then implement other methods as they are called.
 *
 * @see DelegatingInvocationHandler
 */
public class BarfingInvocationHandler implements InvocationHandler {


    protected BarfingInvocationHandler() {
    }


    @Override
    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable {
        Class clazz = getClass();
        Method matchingMethod;
        try {
            matchingMethod = clazz.getMethod( method.getName(), method.getParameterTypes() );
        } catch ( NoSuchMethodException | SecurityException e ) {
            throw noMethod( method );
        }
        if ( matchingMethod.getReturnType() != method.getReturnType() ) {
            throw noMethod( method );
        }

        // Invoke the method in the derived class.
        try {
            return matchingMethod.invoke( this, args );
        } catch ( UndeclaredThrowableException e ) {
            throw e.getCause();
        }
    }


    /**
     * Called when this class (or its derived class) does not have the required method from the interface.
     */
    protected UnsupportedOperationException noMethod( Method method ) {
        StringBuilder buf = new StringBuilder();
        final Class[] parameterTypes = method.getParameterTypes();
        for ( int i = 0; i < parameterTypes.length; i++ ) {
            if ( i > 0 ) {
                buf.append( "," );
            }
            buf.append( parameterTypes[i].getName() );
        }
        String signature = method.getReturnType().getName() + " " + method.getDeclaringClass().getName() + "." + method.getName() + "(" + buf.toString() + ")";
        return new UnsupportedOperationException( signature );
    }
}


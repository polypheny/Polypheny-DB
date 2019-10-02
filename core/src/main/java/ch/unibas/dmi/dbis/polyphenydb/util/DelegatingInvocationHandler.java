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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


/**
 * A class derived from <code>DelegatingInvocationHandler</code> handles a
 * method call by looking for a method in itself with identical parameters. If
 * no such method is found, it forwards the call to a fallback object, which
 * must implement all of the interfaces which this proxy implements.
 *
 * <p>It is useful in creating a wrapper class around an interface which may
 * change over time.</p>
 *
 * <p>Example:</p>
 *
 * <blockquote>
 * <pre>import java.sql.Connection;
 * Connection connection = ...;
 * Connection tracingConnection = (Connection) Proxy.newProxyInstance(
 *     null,
 *     new Class[] {Connection.class},
 *     new DelegatingInvocationHandler() {
 *         protected Object getTarget() {
 *             return connection;
 *         }
 *         Statement createStatement() {
 *             System.out.println("statement created");
 *             return connection.createStatement();
 *         }
 *     });</pre>
 * </blockquote>
 */
public abstract class DelegatingInvocationHandler implements InvocationHandler {
    //~ Methods ----------------------------------------------------------------


    @Override
    public Object invoke(
            Object proxy,
            Method method,
            Object[] args ) throws Throwable {
        Class clazz = getClass();
        Method matchingMethod;
        try {
            matchingMethod =
                    clazz.getMethod(
                            method.getName(),
                            method.getParameterTypes() );
        } catch ( NoSuchMethodException | SecurityException e ) {
            matchingMethod = null;
        }
        try {
            if ( matchingMethod != null ) {
                // Invoke the method in the derived class.
                return matchingMethod.invoke( this, args );
            } else {
                // Invoke the method on the proxy.
                return method.invoke(
                        getTarget(),
                        args );
            }
        } catch ( InvocationTargetException e ) {
            throw e.getTargetException();
        }
    }


    /**
     * Returns the object to forward method calls to, should the derived class
     * not implement the method. Generally, this object will be a member of the
     * derived class, supplied as a parameter to its constructor.
     */
    protected abstract Object getTarget();
}

// End DelegatingInvocationHandler.java

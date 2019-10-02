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


import org.slf4j.helpers.MessageFormatter;


/**
 * Callback to be called when a test for validity succeeds or fails.
 */
public interface Litmus {

    /**
     * Implementation of {@link ch.unibas.dmi.dbis.polyphenydb.util.Litmus} that throws an {@link java.lang.AssertionError} on failure.
     */
    Litmus THROW = new Litmus() {
        @Override
        public boolean fail( String message, Object... args ) {
            final String s = message == null ? null : MessageFormatter.arrayFormat( message, args ).getMessage();
            throw new AssertionError( s );
        }


        @Override
        public boolean succeed() {
            return true;
        }


        @Override
        public boolean check( boolean condition, String message, Object... args ) {
            if ( condition ) {
                return succeed();
            } else {
                return fail( message, args );
            }
        }
    };

    /**
     * Implementation of {@link ch.unibas.dmi.dbis.polyphenydb.util.Litmus} that returns a status code but does not throw.
     */
    Litmus IGNORE = new Litmus() {
        @Override
        public boolean fail( String message, Object... args ) {
            return false;
        }


        @Override
        public boolean succeed() {
            return true;
        }


        @Override
        public boolean check( boolean condition, String message, Object... args ) {
            return condition;
        }
    };

    /**
     * Called when test fails. Returns false or throws.
     *
     * @param message Message
     * @param args Arguments
     */
    boolean fail( String message, Object... args );

    /**
     * Called when test succeeds. Returns true.
     */
    boolean succeed();

    /**
     * Checks a condition.
     *
     * If the condition is true, calls {@link #succeed}; if the condition is false, calls {@link #fail}, converting {@code info} into a string message.
     */
    boolean check( boolean condition, String message, Object... args );
}

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

package ch.unibas.dmi.dbis.polyphenydb.sql.util;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDataTypeSpec;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDynamicParam;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIntervalQualifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;


/**
 * Basic implementation of {@link SqlVisitor} which does nothing at each node.
 *
 * This class is useful as a base class for classes which implement the {@link SqlVisitor} interface. The derived class can override whichever methods it chooses.
 *
 * @param <R> Return type
 */
public class SqlBasicVisitor<R> implements SqlVisitor<R> {

    public R visit( SqlLiteral literal ) {
        return null;
    }


    public R visit( SqlCall call ) {
        return call.getOperator().acceptCall( this, call );
    }


    public R visit( SqlNodeList nodeList ) {
        R result = null;
        for ( int i = 0; i < nodeList.size(); i++ ) {
            SqlNode node = nodeList.get( i );
            result = node.accept( this );
        }
        return result;
    }


    public R visit( SqlIdentifier id ) {
        return null;
    }


    public R visit( SqlDataTypeSpec type ) {
        return null;
    }


    public R visit( SqlDynamicParam param ) {
        return null;
    }


    public R visit( SqlIntervalQualifier intervalQualifier ) {
        return null;
    }


    /**
     * Argument handler.
     *
     * @param <R> result type
     */
    public interface ArgHandler<R> {

        /**
         * Returns the result of visiting all children of a call to an operator, then the call itself.
         *
         * Typically the result will be the result of the last child visited, or (if R is {@link Boolean}) whether all children were visited successfully.
         */
        R result();

        /**
         * Visits a particular operand of a call, using a given visitor.
         */
        R visitChild( SqlVisitor<R> visitor, SqlNode expr, int i, SqlNode operand );
    }


    /**
     * Default implementation of {@link ArgHandler} which merely calls {@link SqlNode#accept} on each operand.
     *
     * @param <R> result type
     */
    public static class ArgHandlerImpl<R> implements ArgHandler<R> {

        private static final ArgHandler INSTANCE = new ArgHandlerImpl();


        @SuppressWarnings("unchecked")
        public static <R> ArgHandler<R> instance() {
            return INSTANCE;
        }


        public R result() {
            return null;
        }


        public R visitChild( SqlVisitor<R> visitor, SqlNode expr, int i, SqlNode operand ) {
            if ( operand == null ) {
                return null;
            }
            return operand.accept( visitor );
        }
    }
}


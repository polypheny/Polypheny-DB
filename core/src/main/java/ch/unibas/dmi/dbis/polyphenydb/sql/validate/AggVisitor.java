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

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSyntax;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlAbstractGroupFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.SqlBasicVisitor;
import java.util.ArrayList;
import java.util.List;


/**
 * Visitor that can find aggregate and windowed aggregate functions.
 *
 * @see AggFinder
 */
abstract class AggVisitor extends SqlBasicVisitor<Void> {

    protected final SqlOperatorTable opTab;

    /**
     * Whether to find windowed aggregates.
     */
    protected final boolean over;
    protected final AggFinder delegate;

    /**
     * Whether to find regular (non-windowed) aggregates.
     */
    protected boolean aggregate;

    /**
     * Whether to find group functions (e.g. {@code TUMBLE}) or group auxiliary functions (e.g. {@code TUMBLE_START}).
     */
    protected boolean group;


    /**
     * Creates an AggVisitor.
     *
     * @param opTab Operator table
     * @param over Whether to find windowed function calls {@code agg(x) OVER windowSpec}
     * @param aggregate Whether to find non-windowed aggregate calls
     * @param group Whether to find group functions (e.g. {@code TUMBLE})
     * @param delegate Finder to which to delegate when processing the arguments
     */
    AggVisitor( SqlOperatorTable opTab, boolean over, boolean aggregate, boolean group, AggFinder delegate ) {
        this.group = group;
        this.over = over;
        this.aggregate = aggregate;
        this.delegate = delegate;
        this.opTab = opTab;
    }


    public Void visit( SqlCall call ) {
        final SqlOperator operator = call.getOperator();
        // If nested aggregates disallowed or found an aggregate at invalid level
        if ( operator.isAggregator() && !(operator instanceof SqlAbstractGroupFunction) && !operator.requiresOver() ) {
            if ( delegate != null ) {
                return operator.acceptCall( delegate, call );
            }
            if ( aggregate ) {
                return found( call );
            }
        }
        if ( group && operator.isGroup() ) {
            return found( call );
        }
        // User-defined function may not be resolved yet.
        if ( operator instanceof SqlFunction ) {
            final SqlFunction sqlFunction = (SqlFunction) operator;
            if ( sqlFunction.getFunctionType().isUserDefinedNotSpecificFunction() ) {
                final List<SqlOperator> list = new ArrayList<>();
                opTab.lookupOperatorOverloads( sqlFunction.getSqlIdentifier(), sqlFunction.getFunctionType(), SqlSyntax.FUNCTION, list );
                for ( SqlOperator operator2 : list ) {
                    if ( operator2.isAggregator() && !operator2.requiresOver() ) {
                        // If nested aggregates disallowed or found aggregate at invalid level
                        if ( aggregate ) {
                            found( call );
                        }
                    }
                }
            }
        }
        if ( call.isA( SqlKind.QUERY ) ) {
            // don't traverse into queries
            return null;
        }
        if ( call.getKind() == SqlKind.OVER ) {
            if ( over ) {
                return found( call );
            } else {
                // an aggregate function over a window is not an aggregate!
                return null;
            }
        }
        return super.visit( call );
    }


    protected abstract Void found( SqlCall call );
}


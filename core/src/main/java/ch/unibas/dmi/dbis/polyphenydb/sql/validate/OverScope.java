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
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import java.util.List;


/**
 * The name-resolution scope of a OVER clause. The objects visible are those in the parameters found on the left side of the over clause, and objects inherited from the parent scope.
 *
 * This object is both a {@link SqlValidatorScope} only. In the query
 *
 * <blockquote>
 * <pre>
 * SELECT name FROM (
 *     SELECT *
 *     FROM emp OVER (
 *         ORDER BY empno
 *         RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING))
 * </pre>
 * </blockquote>
 *
 * We need to use the {@link OverScope} as a {@link SqlValidatorNamespace} when resolving names used in the window specification.
 */
public class OverScope extends ListScope {

    private final SqlCall overCall;


    /**
     * Creates a scope corresponding to a SELECT clause.
     *
     * @param parent Parent scope, or null
     * @param overCall Call to OVER operator
     */
    OverScope( SqlValidatorScope parent, SqlCall overCall ) {
        super( parent );
        this.overCall = overCall;
    }


    @Override
    public SqlNode getNode() {
        return overCall;
    }


    @Override
    public SqlMonotonicity getMonotonicity( SqlNode expr ) {
        SqlMonotonicity monotonicity = expr.getMonotonicity( this );
        if ( monotonicity != SqlMonotonicity.NOT_MONOTONIC ) {
            return monotonicity;
        }

        if ( children.size() == 1 ) {
            final SqlValidatorNamespace child = children.get( 0 ).namespace;
            final List<Pair<SqlNode, SqlMonotonicity>> monotonicExprs = child.getMonotonicExprs();
            for ( Pair<SqlNode, SqlMonotonicity> pair : monotonicExprs ) {
                if ( expr.equalsDeep( pair.left, Litmus.IGNORE ) ) {
                    return pair.right;
                }
            }
        }
        return super.getMonotonicity( expr );
    }
}


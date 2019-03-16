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


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;


/**
 * An extension to the {@link SqlValidatorScope} interface which indicates that the scope is aggregating.
 *
 * A scope which is aggregating must implement this interface. Such a scope will return the same set of identifiers as its parent scope, but some of those identifiers may not be accessible because they
 * are not in the GROUP BY clause.
 */
public interface AggregatingScope extends SqlValidatorScope {

    /**
     * Checks whether an expression is constant within the GROUP BY clause. If the expression completely matches an expression in the GROUP BY clause, returns true. If the expression is constant within the group,
     * but does not exactly match, returns false. If the expression is not constant, throws an exception. Examples:
     *
     * <ul>
     * <li>If we are 'f(b, c)' in 'SELECT a + f(b, c) FROM t GROUP BY a', then the whole expression matches a group column. Return true.</li>
     * <li>Just an ordinary expression in a GROUP BY query, such as 'f(SUM(a), 1, b)' in 'SELECT f(SUM(a), 1, b) FROM t GROUP BY b'. Returns false.</li>
     * <li>Illegal expression, such as 'f(5, a, b)' in 'SELECT f(a, b) FROM t GROUP BY a'. Throws when it enounters the 'b' operand, because it is not in the group clause.</li>
     * </ul>
     */
    boolean checkAggregateExpr( SqlNode expr, boolean deep );
}


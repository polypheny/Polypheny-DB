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

package ch.unibas.dmi.dbis.polyphenydb.sql2rel;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexRangeRef;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelect;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;


/**
 * Contains the context necessary for a {@link SqlRexConvertlet} to convert a {@link SqlNode} expression into a {@link RexNode}.
 */
public interface SqlRexContext {

    /**
     * Converts an expression from {@link SqlNode} to {@link RexNode} format.
     *
     * @param expr Expression to translate
     * @return Converted expression
     */
    RexNode convertExpression( SqlNode expr );

    /**
     * If the operator call occurs in an aggregate query, returns the number of columns in the GROUP BY clause. For example, for "SELECT count(*) FROM emp GROUP BY deptno, gender", returns 2.
     * If the operator call occurs in window aggregate query, then returns 1 if the window is guaranteed to be non-empty, or 0 if the window might be empty.
     *
     * Returns 0 if the query is implicitly "GROUP BY ()" because of an aggregate expression. For example, "SELECT sum(sal) FROM emp".
     *
     * Returns -1 if the query is not an aggregate query.
     *
     * @return 0 if the query is implicitly GROUP BY (), -1 if the query is not and aggregate query
     * @see ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorBinding#getGroupCount()
     */
    int getGroupCount();

    /**
     * Returns the {@link RexBuilder} to use to create {@link RexNode} objects.
     */
    RexBuilder getRexBuilder();

    /**
     * Returns the expression used to access a given IN or EXISTS {@link SqlSelect sub-query}.
     *
     * @param call IN or EXISTS expression
     * @return Expression used to access current row of sub-query
     */
    RexRangeRef getSubQueryExpr( SqlCall call );

    /**
     * Returns the type factory.
     */
    RelDataTypeFactory getTypeFactory();

    /**
     * Returns the factory which supplies default values for INSERT, UPDATE, and NEW.
     */
    InitializerExpressionFactory getInitializerExpressionFactory();

    /**
     * Returns the validator.
     */
    SqlValidator getValidator();

    /**
     * Converts a literal.
     */
    RexNode convertLiteral( SqlLiteral literal );
}

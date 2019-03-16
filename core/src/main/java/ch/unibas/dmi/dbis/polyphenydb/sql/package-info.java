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

/**
 * Provides a SQL parser and object model.
 *
 * This package, and the dependent <code>ch.unibas.dmi.dbis.polyphenydb.sql.parser</code> package, are independent of the other Polypheny-DB packages,
 * so may be used standalone.
 *
 * <h2>Parser</h2>
 *
 * {@link ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser} parses a SQL string to a parse tree. It only performs the most basic syntactic validation.
 *
 * <h2>Object model</h2>
 *
 * Every node in the parse tree is a {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode}. Sub-types are:
 *
 * <ul>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral} represents a boolean, numeric, string, or date constant, or the value <code>NULL</code>.</li>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier} represents an identifier, such as <code> EMPNO</code> or <code>emp.deptno</code>.</li>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall} is a call to an operator or function.  By means of special operators, we can use this construct to represent virtually every non-leaf node in the tree. For example, a <code>select</code> statement is a call to the 'select' operator.</li>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList} is a list of nodes.</li>
 * </ul>
 *
 * A {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator} describes the behavior of a node in the tree, such as how to un-parse a
 * {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall} into a SQL string.  It is important to note that operators are metadata, not data: there is only
 * one <code>SqlOperator</code> instance representing the '=' operator, even though there may be many calls to it.
 *
 * <code>SqlOperator</code> has several derived classes which make it easy to define new operators:
 * {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunction},
 * {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlBinaryOperator},
 * {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlPrefixOperator},
 * {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlPostfixOperator}.
 * And there are singleton classes for special syntactic constructs {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelectOperator} and
 * {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlJoin.SqlJoinOperator}. (These special operators even have their own sub-types of {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall}:
 * {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelect} and
 * {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlJoin}.)
 *
 * A {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorTable} is a collection of operators. By supplying your own operator table, you can customize the
 * dialect of SQL without modifying the parser.
 *
 * <h2>Validation</h2>
 *
 * {@link ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator} checks that a tree of {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode}s is semantically valid.
 * You supply a {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorTable} to describe the available functions and operators, and a
 * {@link ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorCatalogReader} for access to the database's catalog.
 *
 * <h2>Generating SQL</h2>
 *
 * A {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter} converts a tree of {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode}s into a SQL string.
 * A {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlDialect} defines how this happens.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql;


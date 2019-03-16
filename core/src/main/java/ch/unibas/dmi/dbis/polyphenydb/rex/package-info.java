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
 * Provides a language for representing row-expressions.
 *
 * <h2>Life-cycle</h2>
 *
 * A {@link ch.unibas.dmi.dbis.polyphenydb.sql2rel.SqlToRelConverter} converts a SQL parse tree consisting of {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode} objects into a relational expression ({@link ch.unibas.dmi.dbis.polyphenydb.rel.RelNode}).
 * Several kinds of nodes in this tree have row expressions ({@link ch.unibas.dmi.dbis.polyphenydb.rex.RexNode}).
 *
 * After the relational expression has been optimized, a {@link ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.JavaRelImplementor} converts it into to a plan. If the plan is a Java parse tree, row-expressions are
 * translated into equivalent Java expressions.
 *
 * <h2>Expressions</h2>
 *
 *
 * Every row-expression has a type. (Compare with {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode}, which is created before validation, and therefore types may not be available.)
 *
 * Every node in the parse tree is a {@link ch.unibas.dmi.dbis.polyphenydb.rex.RexNode}. Sub-types are:<
 * <ul>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral} represents a boolean, numeric, string, or date constant, or the value <code>NULL</code>.</li>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rex.RexVariable} represents a leaf of the tree. It has sub-types:
 * <ul>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rex.RexCorrelVariable} is a correlating variable for nested-loop joins</li>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rex.RexInputRef} refers to a field of an input relational expression</li>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rex.RexCall} is a call to an operator or function.  By means of special operators, we can use this construct to represent virtually every non-leaf node in the tree.</li>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rex.RexRangeRef} refers to a collection of contiguous fields from an input relational expression. It usually exists only during translation.</li>
 * </ul>
 * </li>
 * </ul>
 *
 * Expressions are generally created using a {@link ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder} factory.
 *
 * <h2>Related packages</h2>
 * <ul>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.sql} SQL object model</li>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.plan} Core classes, including {@link ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType} and {@link ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory}.</li>
 * </ul>
 */

package ch.unibas.dmi.dbis.polyphenydb.rex;


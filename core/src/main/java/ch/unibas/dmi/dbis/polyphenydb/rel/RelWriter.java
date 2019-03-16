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

package ch.unibas.dmi.dbis.polyphenydb.rel;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import java.util.List;


/**
 * Callback for an expression to dump itself to.
 *
 * It is used for generating EXPLAIN PLAN output, and also for serializing a tree of relational expressions to JSON.
 */
public interface RelWriter {

    /**
     * Prints an explanation of a node, with a list of (term, value) pairs.
     *
     * The term-value pairs are generally gathered by calling {@link RelNode#explain(RelWriter)}.
     * Each sub-class of {@link RelNode} calls {@link #input(String, RelNode)} and {@link #item(String, Object)} to declare term-value pairs.
     *
     * @param rel Relational expression
     * @param valueList List of term-value pairs
     */
    void explain( RelNode rel, List<Pair<String, Object>> valueList );

    /**
     * @return detail level at which plan should be generated
     */
    SqlExplainLevel getDetailLevel();

    /**
     * Adds an input to the explanation of the current node.
     *
     * @param term Term for input, e.g. "left" or "input #1".
     * @param input Input relational expression
     */
    RelWriter input( String term, RelNode input );

    /**
     * Adds an attribute to the explanation of the current node.
     *
     * @param term Term for attribute, e.g. "joinType"
     * @param value Attribute value
     */
    RelWriter item( String term, Object value );

    /**
     * Adds an input to the explanation of the current node, if a condition holds.
     */
    RelWriter itemIf( String term, Object value, boolean condition );

    /**
     * Writes the completed explanation.
     */
    RelWriter done( RelNode node );

    /**
     * Returns whether the writer prefers nested values. Traditional explain writers prefer flattened values.
     */
    boolean nest();
}


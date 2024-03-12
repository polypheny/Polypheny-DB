/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.algebra;


import java.util.List;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.util.Pair;


/**
 * Callback for an expression to dump itself to.
 * <p>
 * It is used for generating EXPLAIN PLAN output, and also for serializing a tree of relational expressions to JSON.
 */
public interface AlgWriter {

    /**
     * Prints an explanation of a node, with a list of (term, value) pairs.
     * <p>
     * The term-value pairs are generally gathered by calling {@link AlgNode#explain(AlgWriter)}.
     * Each sub-class of {@link AlgNode} calls {@link #input(String, AlgNode)} and {@link #item(String, Object)} to declare term-value pairs.
     *
     * @param alg Relational expression
     * @param valueList List of term-value pairs
     */
    void explain( AlgNode alg, List<Pair<String, Object>> valueList );

    /**
     * @return detail level at which plan should be generated
     */
    ExplainLevel getDetailLevel();

    /**
     * Adds an input to the explanation of the current node.
     *
     * @param term Term for input, e.g. "left" or "input #1".
     * @param input Input relational expression
     */
    AlgWriter input( String term, AlgNode input );

    /**
     * Adds an attribute to the explanation of the current node.
     *
     * @param term Term for attribute, e.g. "joinType"
     * @param value Attribute value
     */
    AlgWriter item( String term, Object value );

    /**
     * Adds an input to the explanation of the current node, if a condition holds.
     */
    AlgWriter itemIf( String term, Object value, boolean condition );

    /**
     * Writes the completed explanation.
     */
    AlgWriter done( AlgNode node );

    /**
     * Returns whether the writer prefers nested values. Traditional explain writers prefer flattened values.
     */
    boolean nest();

}


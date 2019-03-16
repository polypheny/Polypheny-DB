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

package ch.unibas.dmi.dbis.polyphenydb.rel.convert;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelTrait;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;


/**
 * A relational expression implements the interface <code>Converter</code> to indicate that it converts a physical attribute, or {@link RelTrait trait},
 * of a relational expression from one value to another.
 *
 * Sometimes this conversion is expensive; for example, to convert a non-distinct to a distinct object stream, we have to clone every object in the input.
 *
 * A converter does not change the logical expression being evaluated; after conversion, the number of rows and the values of those rows will still be the same.
 * By declaring itself to be a converter, a relational expression is telling the planner about this equivalence, and the planner groups expressions which are
 * logically equivalent but have different physical traits into groups called <code>RelSet</code>s.
 *
 * In principle one could devise converters which change multiple traits simultaneously (say change the sort-order and the physical location of a relational expression).
 * In which case, the method {@link #getInputTraits()} would return a {@link ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet}. But for simplicity, this class only allows one trait to
 * be converted at a time; all other traits are assumed to be preserved.
 */
public interface Converter extends RelNode {

    /**
     * Returns the trait of the input relational expression.
     *
     * @return input trait
     */
    RelTraitSet getInputTraits();

    /**
     * Returns the definition of trait which this converter works on.
     *
     * The input relational expression (matched by the rule) must possess this trait and have the value given by {@link #getInputTraits()}, and the traits of the output of this
     * converter given by {@link #getTraitSet()} will have one trait altered and the other orthogonal traits will be the same.
     *
     * @return trait which this converter modifies
     */
    RelTraitDef getTraitDef();

    /**
     * Returns the sole input relational expression
     *
     * @return child relational expression
     */
    RelNode getInput();
}


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

package ch.unibas.dmi.dbis.polyphenydb.plan;


/**
 * RelTrait represents the manifestation of a relational expression trait within a trait definition. For example, a {@code CallingConvention.JAVA} is a trait of the {@link ConventionTraitDef} trait definition.
 *
 * <h3><a id="EqualsHashCodeNote">Note about equals() and hashCode()</a></h3>
 *
 * If all instances of RelTrait for a particular RelTraitDef are defined in an {@code enum} and no new RelTraits can be introduced at runtime, you need not override {@link #hashCode()}
 * and {@link #equals(Object)}. If, however, new RelTrait instances are generated at runtime (e.g. based on state external to the planner), you must implement {@link #hashCode()} and
 * {@link #equals(Object)} for proper {@link RelTraitDef#canonize canonization} of your RelTrait objects.
 */
public interface RelTrait {

    /**
     * Returns the RelTraitDef that defines this RelTrait.
     *
     * @return the RelTraitDef that defines this RelTrait
     */
    RelTraitDef getTraitDef();

    /**
     * See <a href="#EqualsHashCodeNote">note about equals() and hashCode()</a>.
     */
    int hashCode();

    /**
     * See <a href="#EqualsHashCodeNote">note about equals() and hashCode()</a>.
     */
    boolean equals( Object o );

    /**
     * Returns whether this trait satisfies a given trait.
     *
     * A trait satisfies another if it is the same or stricter. For example, {@code ORDER BY x, y} satisfies {@code ORDER BY x}.
     *
     * A trait's {@code satisfies} relation must be a partial order (reflexive, anti-symmetric, transitive). Many traits cannot be "loosened"; their {@code satisfies} is
     * an equivalence relation, where only X satisfies X.
     *
     * If a trait has multiple values (see {@link ch.unibas.dmi.dbis.polyphenydb.plan.RelCompositeTrait}) a collection (T0, T1, ...) satisfies T if any Ti satisfies T.
     *
     * @param trait Given trait
     * @return Whether this trait subsumes a given trait
     */
    boolean satisfies( RelTrait trait );

    /**
     * Returns a succinct name for this trait. The planner may use this String to describe the trait.
     */
    String toString();

    /**
     * Registers a trait instance with the planner.
     *
     * This is an opportunity to add rules that relate to that trait. However, typical implementations will do nothing.
     *
     * @param planner Planner
     */
    void register( RelOptPlanner planner );
}


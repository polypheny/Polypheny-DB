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


import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;


/**
 * Calling convention trait.
 */
public interface Convention extends RelTrait {

    /**
     * Convention that for a relational expression that does not support any convention. It is not implementable, and has to be transformed to something else in order to be implemented.
     *
     * Relational expressions generally start off in this form.
     *
     * Such expressions always have infinite cost.
     */
    Convention NONE = new Impl( "NONE", RelNode.class );

    Class getInterface();

    String getName();

    /**
     * Returns whether we should convert from this convention to {@code toConvention}. Used by {@link ConventionTraitDef}.
     *
     * @param toConvention Desired convention to convert to
     * @return Whether we should convert from this convention to toConvention
     */
    boolean canConvertConvention( Convention toConvention );

    /**
     * Returns whether we should convert from this trait set to the other trait set.
     *
     * The convention decides whether it wants to handle other trait conversions, e.g. collation, distribution, etc.  For a given convention, we will only add abstract converters to handle the
     * trait (convention, collation, distribution, etc.) conversions if this function returns true.
     *
     * @param fromTraits Traits of the RelNode that we are converting from
     * @param toTraits Target traits
     * @return Whether we should add converters
     */
    boolean useAbstractConvertersForConversion( RelTraitSet fromTraits, RelTraitSet toTraits );

    /**
     * Default implementation.
     */
    class Impl implements Convention {

        private final String name;
        private final Class<? extends RelNode> relClass;


        public Impl( String name, Class<? extends RelNode> relClass ) {
            this.name = name;
            this.relClass = relClass;
        }


        @Override
        public String toString() {
            return getName();
        }


        public void register( RelOptPlanner planner ) {
        }


        public boolean satisfies( RelTrait trait ) {
            return this == trait;
        }


        public Class getInterface() {
            return relClass;
        }


        public String getName() {
            return name;
        }


        public RelTraitDef getTraitDef() {
            return ConventionTraitDef.INSTANCE;
        }


        public boolean canConvertConvention( Convention toConvention ) {
            return false;
        }


        public boolean useAbstractConvertersForConversion( RelTraitSet fromTraits, RelTraitSet toTraits ) {
            return false;
        }
    }
}


/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.plan;


import java.io.Serializable;
import org.polypheny.db.algebra.AlgNode;


/**
 * Calling convention trait.
 */
public interface Convention extends AlgTrait, Serializable {

    /**
     * Convention that for a relational expression that does not support any convention. It is not implementable, and has to be transformed to something else in order to be implemented.
     *
     * Relational expressions generally start off in this form.
     *
     * Such expressions always have infinite cost.
     */
    Convention NONE = new Impl( "NONE", AlgNode.class );

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
     * @param fromTraits Traits of the {@link AlgNode} that we are converting from
     * @param toTraits Target traits
     * @return Whether we should add converters
     */
    boolean useAbstractConvertersForConversion( AlgTraitSet fromTraits, AlgTraitSet toTraits );

    /**
     * Default implementation.
     */
    class Impl implements Convention {

        private final String name;
        private final Class<? extends AlgNode> algClass;


        public Impl( String name, Class<? extends AlgNode> algClass ) {
            this.name = name;
            this.algClass = algClass;
        }


        @Override
        public String toString() {
            return getName();
        }


        @Override
        public void register( AlgOptPlanner planner ) {
        }


        @Override
        public boolean satisfies( AlgTrait trait ) {
            return this == trait;
        }


        @Override
        public Class getInterface() {
            return algClass;
        }


        @Override
        public String getName() {
            return name;
        }


        @Override
        public AlgTraitDef getTraitDef() {
            return ConventionTraitDef.INSTANCE;
        }


        @Override
        public boolean canConvertConvention( Convention toConvention ) {
            return false;
        }


        @Override
        public boolean useAbstractConvertersForConversion( AlgTraitSet fromTraits, AlgTraitSet toTraits ) {
            return false;
        }

    }

}


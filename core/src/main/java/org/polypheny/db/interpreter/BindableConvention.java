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

package org.polypheny.db.interpreter;


import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Calling convention that returns results as an {@link org.apache.calcite.linq4j.Enumerable} of object arrays.
 *
 * The relational expression needs to implement {@link org.polypheny.db.runtime.ArrayBindable}. Unlike {@link EnumerableConvention}, no code generation is required.
 */
public enum BindableConvention implements Convention {
    INSTANCE;

    /**
     * Cost of a bindable node versus implementing an equivalent node in a "typical" calling convention.
     */
    public static final double COST_MULTIPLIER = 2.0d;


    @Override
    public String toString() {
        return getName();
    }


    @Override
    public Class<?> getInterface() {
        return BindableAlg.class;
    }


    @Override
    public String getName() {
        return "BINDABLE";
    }


    @Override
    public boolean satisfies( AlgTrait<?> trait ) {
        return this == trait;
    }


    @Override
    public void register( AlgPlanner planner ) {
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


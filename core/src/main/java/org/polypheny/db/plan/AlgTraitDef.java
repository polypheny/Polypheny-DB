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


import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;


/**
 * RelTraitDef represents a class of {@link AlgTrait}s. Implementations of RelTraitDef may be singletons under the following conditions:
 *
 * <ol>
 * <li>if the set of all possible associated RelTraits is finite and fixed (e.g. all RelTraits for this RelTraitDef are known at compile time). For example, the CallingConvention trait
 * meets this requirement, because CallingConvention is effectively an enumeration.</li>
 *
 * <li>Either
 * <ul>
 * <li> {@link #canConvert(AlgOptPlanner, AlgTrait, AlgTrait)} and {@link #convert(AlgOptPlanner, AlgNode, AlgTrait, boolean)} do not require planner-instance-specific information, <b>or</b></li>
 * <li>the RelTraitDef manages separate sets of conversion data internally. See {@link ConventionTraitDef} for an example of this.</li>
 * </ul>
 * </li>
 * </ol>
 *
 * Otherwise, a new instance of RelTraitDef must be constructed and registered with each new planner instantiated.</p>
 *
 * @param <T> Trait that this trait definition is based upon
 */
public abstract class AlgTraitDef<T extends AlgTrait> {

    /**
     * Cache of traits.
     *
     * Uses weak interner to allow GC.
     */
    private final Interner<T> interner = Interners.newWeakInterner();


    protected AlgTraitDef() {
    }


    /**
     * Whether a relational expression may possess more than one instance of this trait simultaneously.
     *
     * A subset has only one instance of a trait.
     */
    public boolean multiple() {
        return false;
    }


    /**
     * @return the specific RelTrait type associated with this RelTraitDef.
     */
    public abstract Class<T> getTraitClass();

    /**
     * @return a simple name for this RelTraitDef (for use in {@link AlgNode#explain}).
     */
    public abstract String getSimpleName();


    /**
     * Takes an arbitrary RelTrait and returns the canonical representation of that RelTrait. Canonized RelTrait objects may always be compared using the equality operator (<code>==</code>).
     *
     * If an equal RelTrait has already been canonized and is still in use, it will be returned. Otherwise, the given RelTrait is made canonical and returned.
     *
     * @param trait a possibly non-canonical RelTrait
     * @return a canonical RelTrait.
     */
    public final T canonize( T trait ) {
        if ( !(trait instanceof AlgCompositeTrait) ) {
            assert getTraitClass().isInstance( trait ) : getClass().getName() + " cannot canonize a " + trait.getClass().getName();
        }
        return interner.intern( trait );
    }


    /**
     * Converts the given {@link AlgNode} to the given RelTrait.
     *
     * @param planner the planner requesting the conversion
     * @param alg {@link AlgNode} to convert
     * @param toTrait RelTrait to convert to
     * @param allowInfiniteCostConverters flag indicating whether infinite cost converters are allowed
     * @return a converted {@link AlgNode} or null if conversion is not possible
     */
    public abstract AlgNode convert( AlgOptPlanner planner, AlgNode alg, T toTrait, boolean allowInfiniteCostConverters );

    /**
     * Tests whether the given RelTrait can be converted to another RelTrait.
     *
     * @param planner the planner requesting the conversion test
     * @param fromTrait the RelTrait to convert from
     * @param toTrait the RelTrait to convert to
     * @return true if fromTrait can be converted to toTrait
     */
    public abstract boolean canConvert( AlgOptPlanner planner, T fromTrait, T toTrait );


    /**
     * Tests whether the given RelTrait can be converted to another RelTrait.
     *
     * @param planner the planner requesting the conversion test
     * @param fromTrait the RelTrait to convert from
     * @param toTrait the RelTrait to convert to
     * @param fromRel the {@link AlgNode} to convert from (with fromTrait)
     * @return true if fromTrait can be converted to toTrait
     */
    public boolean canConvert( AlgOptPlanner planner, T fromTrait, T toTrait, AlgNode fromRel ) {
        return canConvert( planner, fromTrait, toTrait );
    }


    /**
     * Provides notification of the registration of a particular {@link ConverterRule} with a {@link AlgOptPlanner}. The default implementation does nothing.
     *
     * @param planner the planner registering the rule
     * @param converterRule the registered converter rule
     */
    public void registerConverterRule( AlgOptPlanner planner, ConverterRule converterRule ) {
    }


    /**
     * Provides notification that a particular {@link ConverterRule} has been de-registered from a {@link AlgOptPlanner}. The default implementation does nothing.
     *
     * @param planner the planner registering the rule
     * @param converterRule the registered converter rule
     */
    public void deregisterConverterRule( AlgOptPlanner planner, ConverterRule converterRule ) {
    }


    /**
     * Returns the default member of this trait.
     */
    public abstract T getDefault();

}


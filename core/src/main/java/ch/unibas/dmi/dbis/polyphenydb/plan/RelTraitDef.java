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
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterRule;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;


/**
 * RelTraitDef represents a class of {@link RelTrait}s. Implementations of RelTraitDef may be singletons under the following conditions:
 *
 * <ol>
 * <li>if the set of all possible associated RelTraits is finite and fixed (e.g. all RelTraits for this RelTraitDef are known at compile time). For example, the CallingConvention trait
 * meets this requirement, because CallingConvention is effectively an enumeration.</li>
 *
 * <li>Either
 * <ul>
 * <li> {@link #canConvert(RelOptPlanner, RelTrait, RelTrait)} and {@link #convert(RelOptPlanner, RelNode, RelTrait, boolean)} do not require planner-instance-specific information, <b>or</b></li>
 * <li>the RelTraitDef manages separate sets of conversion data internally. See {@link ConventionTraitDef} for an example of this.</li>
 * </ul>
 * </li>
 * </ol>
 *
 * Otherwise, a new instance of RelTraitDef must be constructed and registered with each new planner instantiated.</p>
 *
 * @param <T> Trait that this trait definition is based upon
 */
public abstract class RelTraitDef<T extends RelTrait> {

    /**
     * Cache of traits.
     *
     * Uses weak interner to allow GC.
     */
    private final Interner<T> interner = Interners.newWeakInterner();


    protected RelTraitDef() {
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
     * @return a simple name for this RelTraitDef (for use in {@link RelNode#explain}).
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
        if ( !(trait instanceof RelCompositeTrait) ) {
            assert getTraitClass().isInstance( trait ) : getClass().getName() + " cannot canonize a " + trait.getClass().getName();
        }
        return interner.intern( trait );
    }


    /**
     * Converts the given RelNode to the given RelTrait.
     *
     * @param planner the planner requesting the conversion
     * @param rel RelNode to convert
     * @param toTrait RelTrait to convert to
     * @param allowInfiniteCostConverters flag indicating whether infinite cost converters are allowed
     * @return a converted RelNode or null if conversion is not possible
     */
    public abstract RelNode convert( RelOptPlanner planner, RelNode rel, T toTrait, boolean allowInfiniteCostConverters );

    /**
     * Tests whether the given RelTrait can be converted to another RelTrait.
     *
     * @param planner the planner requesting the conversion test
     * @param fromTrait the RelTrait to convert from
     * @param toTrait the RelTrait to convert to
     * @return true if fromTrait can be converted to toTrait
     */
    public abstract boolean canConvert( RelOptPlanner planner, T fromTrait, T toTrait );


    /**
     * Tests whether the given RelTrait can be converted to another RelTrait.
     *
     * @param planner the planner requesting the conversion test
     * @param fromTrait the RelTrait to convert from
     * @param toTrait the RelTrait to convert to
     * @param fromRel the RelNode to convert from (with fromTrait)
     * @return true if fromTrait can be converted to toTrait
     */
    public boolean canConvert( RelOptPlanner planner, T fromTrait, T toTrait, RelNode fromRel ) {
        return canConvert( planner, fromTrait, toTrait );
    }


    /**
     * Provides notification of the registration of a particular {@link ConverterRule} with a {@link RelOptPlanner}. The default implementation does nothing.
     *
     * @param planner the planner registering the rule
     * @param converterRule the registered converter rule
     */
    public void registerConverterRule( RelOptPlanner planner, ConverterRule converterRule ) {
    }


    /**
     * Provides notification that a particular {@link ConverterRule} has been de-registered from a {@link RelOptPlanner}. The default implementation does nothing.
     *
     * @param planner the planner registering the rule
     * @param converterRule the registered converter rule
     */
    public void deregisterConverterRule( RelOptPlanner planner, ConverterRule converterRule ) {
    }


    /**
     * Returns the default member of this trait.
     */
    public abstract T getDefault();
}


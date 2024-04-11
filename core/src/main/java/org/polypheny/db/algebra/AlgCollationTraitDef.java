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


import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Definition of the ordering trait.
 *
 * Ordering is a physical property (i.e. a trait) because it can be changed without loss of information. The converter to
 * do this is the {@link Sort} operator.
 *
 * Unlike other current traits, a {@link AlgNode} can have more than one value of this trait simultaneously. For example,
 * <code>LogicalScan(table=TIME_BY_DAY)</code> might be sorted by <code>{the_year, the_month, the_date}</code> and also by
 * <code>{time_id}</code>. We have to allow a {@link AlgNode} to belong to more than one RelSubset (these RelSubsets are always in the same set).
 */
public class AlgCollationTraitDef extends AlgTraitDef<AlgCollation> {

    public static final AlgCollationTraitDef INSTANCE = new AlgCollationTraitDef();


    private AlgCollationTraitDef() {
    }


    @Override
    public Class<AlgCollation> getTraitClass() {
        return AlgCollation.class;
    }


    @Override
    public String getSimpleName() {
        return "sort";
    }


    @Override
    public boolean multiple() {
        return true;
    }


    @Override
    public AlgCollation getDefault() {
        return AlgCollations.EMPTY;
    }


    @Override
    public AlgNode convert( AlgPlanner planner, AlgNode alg, AlgCollation toCollation, boolean allowInfiniteCostConverters ) {
        if ( toCollation.getFieldCollations().isEmpty() ) {
            // An empty sort doesn't make sense.
            return null;
        }

        // Create a logical sort, then ask the planner to convert its remaining traits (e.g. convert it to an EnumerableSortRel if alg is enumerable convention)
        final Sort sort = LogicalRelSort.create( alg, toCollation, null, null );
        AlgNode newRel = planner.register( sort, alg );
        final AlgTraitSet newTraitSet = alg.getTraitSet().replace( toCollation );
        if ( !newRel.getTraitSet().equals( newTraitSet ) ) {
            newRel = planner.changeTraits( newRel, newTraitSet );
        }
        return newRel;
    }


    @Override
    public boolean canConvert( AlgPlanner planner, AlgCollation fromTrait, AlgCollation toTrait ) {
        return false;
    }


    @Override
    public boolean canConvert( AlgPlanner planner, AlgCollation fromTrait, AlgCollation toTrait, AlgNode fromAlg ) {
        // Returns true only if we can convert.  In this case, we can only convert if the fromTrait (the input) has fields that the toTrait wants to sort.
        for ( AlgFieldCollation field : toTrait.getFieldCollations() ) {
            int index = field.getFieldIndex();
            if ( index >= fromAlg.getTupleType().getFieldCount() ) {
                return false;
            }
        }
        return true;
    }

}


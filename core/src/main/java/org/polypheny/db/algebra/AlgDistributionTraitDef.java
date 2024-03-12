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


import org.polypheny.db.algebra.core.Exchange;
import org.polypheny.db.algebra.logical.relational.LogicalRelExchange;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Definition of the distribution trait.
 *
 * Distribution is a physical property (i.e. a trait) because it can be changed without loss of information. The converter to do this is the {@link Exchange} operator.
 */
public class AlgDistributionTraitDef extends AlgTraitDef<AlgDistribution> {

    public static final AlgDistributionTraitDef INSTANCE = new AlgDistributionTraitDef();


    private AlgDistributionTraitDef() {
    }


    @Override
    public Class<AlgDistribution> getTraitClass() {
        return AlgDistribution.class;
    }


    @Override
    public String getSimpleName() {
        return "dist";
    }


    @Override
    public AlgDistribution getDefault() {
        return AlgDistributions.ANY;
    }


    @Override
    public AlgNode convert( AlgPlanner planner, AlgNode alg, AlgDistribution toDistribution, boolean allowInfiniteCostConverters ) {
        if ( toDistribution == AlgDistributions.ANY ) {
            return alg;
        }

        // Create a logical sort, then ask the planner to convert its remaining traits (e.g. convert it to an EnumerableSortRel if alg is enumerable convention)
        final Exchange exchange = LogicalRelExchange.create( alg, toDistribution );
        AlgNode newRel = planner.register( exchange, alg );
        final AlgTraitSet newTraitSet = alg.getTraitSet().replace( toDistribution );
        if ( !newRel.getTraitSet().equals( newTraitSet ) ) {
            newRel = planner.changeTraits( newRel, newTraitSet );
        }
        return newRel;
    }


    @Override
    public boolean canConvert( AlgPlanner planner, AlgDistribution fromTrait, AlgDistribution toTrait ) {
        return true;
    }

}


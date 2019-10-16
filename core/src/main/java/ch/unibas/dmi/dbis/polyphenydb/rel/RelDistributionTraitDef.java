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

package ch.unibas.dmi.dbis.polyphenydb.rel;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Exchange;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalExchange;


/**
 * Definition of the distribution trait.
 *
 * Distribution is a physical property (i.e. a trait) because it can be changed without loss of information. The converter to do this is the {@link Exchange} operator.
 */
public class RelDistributionTraitDef extends RelTraitDef<RelDistribution> {

    public static final RelDistributionTraitDef INSTANCE = new RelDistributionTraitDef();


    private RelDistributionTraitDef() {
    }


    @Override
    public Class<RelDistribution> getTraitClass() {
        return RelDistribution.class;
    }


    @Override
    public String getSimpleName() {
        return "dist";
    }


    @Override
    public RelDistribution getDefault() {
        return RelDistributions.ANY;
    }


    @Override
    public RelNode convert( RelOptPlanner planner, RelNode rel, RelDistribution toDistribution, boolean allowInfiniteCostConverters ) {
        if ( toDistribution == RelDistributions.ANY ) {
            return rel;
        }

        // Create a logical sort, then ask the planner to convert its remaining traits (e.g. convert it to an EnumerableSortRel if rel is enumerable convention)
        final Exchange exchange = LogicalExchange.create( rel, toDistribution );
        RelNode newRel = planner.register( exchange, rel );
        final RelTraitSet newTraitSet = rel.getTraitSet().replace( toDistribution );
        if ( !newRel.getTraitSet().equals( newTraitSet ) ) {
            newRel = planner.changeTraits( newRel, newTraitSet );
        }
        return newRel;
    }


    @Override
    public boolean canConvert( RelOptPlanner planner, RelDistribution fromTrait, RelDistribution toTrait ) {
        return true;
    }
}


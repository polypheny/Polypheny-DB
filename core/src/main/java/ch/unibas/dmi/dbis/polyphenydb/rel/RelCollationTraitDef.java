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
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalSort;


/**
 * Definition of the ordering trait.
 *
 * Ordering is a physical property (i.e. a trait) because it can be changed without loss of information. The converter to do this is the {@link Sort} operator.
 *
 * Unlike other current traits, a {@link RelNode} can have more than one value of this trait simultaneously. For example, <code>LogicalTableScan(table=TIME_BY_DAY)</code> might be sorted by
 * <code>{the_year, the_month, the_date}</code> and also by <code>{time_id}</code>. We have to allow a RelNode to belong to more than one RelSubset (these RelSubsets are always in the same set).
 */
public class RelCollationTraitDef extends RelTraitDef<RelCollation> {

    public static final RelCollationTraitDef INSTANCE = new RelCollationTraitDef();


    private RelCollationTraitDef() {
    }


    @Override
    public Class<RelCollation> getTraitClass() {
        return RelCollation.class;
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
    public RelCollation getDefault() {
        return RelCollations.EMPTY;
    }


    @Override
    public RelNode convert( RelOptPlanner planner, RelNode rel, RelCollation toCollation, boolean allowInfiniteCostConverters ) {
        if ( toCollation.getFieldCollations().isEmpty() ) {
            // An empty sort doesn't make sense.
            return null;
        }

        // Create a logical sort, then ask the planner to convert its remaining traits (e.g. convert it to an EnumerableSortRel if rel is enumerable convention)
        final Sort sort = LogicalSort.create( rel, toCollation, null, null );
        RelNode newRel = planner.register( sort, rel );
        final RelTraitSet newTraitSet = rel.getTraitSet().replace( toCollation );
        if ( !newRel.getTraitSet().equals( newTraitSet ) ) {
            newRel = planner.changeTraits( newRel, newTraitSet );
        }
        return newRel;
    }


    @Override
    public boolean canConvert( RelOptPlanner planner, RelCollation fromTrait, RelCollation toTrait ) {
        return false;
    }


    @Override
    public boolean canConvert( RelOptPlanner planner, RelCollation fromTrait, RelCollation toTrait, RelNode fromRel ) {
        // Returns true only if we can convert.  In this case, we can only convert if the fromTrait (the input) has fields that the toTrait wants to sort.
        for ( RelFieldCollation field : toTrait.getFieldCollations() ) {
            int index = field.getFieldIndex();
            if ( index >= fromRel.getRowType().getFieldCount() ) {
                return false;
            }
        }
        return true;
    }
}


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
import ch.unibas.dmi.dbis.polyphenydb.rel.RelVisitor;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;


/**
 * RelTraitPropagationVisitor traverses a RelNode and its <i>unregistered</i> children, making sure that each has a full complement of traits. When a RelNode is found to be missing one or
 * more traits, they are copied from a RelTraitSet given during construction.
 */
public class RelTraitPropagationVisitor extends RelVisitor {

    private final RelTraitSet baseTraits;
    private final RelOptPlanner planner;


    public RelTraitPropagationVisitor(
            RelOptPlanner planner,
            RelTraitSet baseTraits ) {
        this.planner = planner;
        this.baseTraits = baseTraits;
    }


    @Override
    public void visit( RelNode rel, int ordinal, RelNode parent ) {
        // REVIEW: SWZ: 1/31/06: We assume that any special RelNodes, such as the VolcanoPlanner's RelSubset always have a full complement of traits and that they
        // either appear as registered or do nothing when childrenAccept is called on them.

        if ( planner.isRegistered( rel ) ) {
            return;
        }

        RelTraitSet relTraits = rel.getTraitSet();
        for ( int i = 0; i < baseTraits.size(); i++ ) {
            if ( i >= relTraits.size() ) {
                // Copy traits that the new rel doesn't know about.
                Util.discard( RelOptUtil.addTrait( rel, baseTraits.getTrait( i ) ) );

                // FIXME: Return the new rel. We can no longer traits in-place, because rels and traits are immutable.
                throw new AssertionError();
            } else {
                // Verify that the traits are from the same RelTraitDef
                assert relTraits.getTrait( i ).getTraitDef() == baseTraits.getTrait( i ).getTraitDef();
            }
        }
        rel.childrenAccept( this );
    }
}


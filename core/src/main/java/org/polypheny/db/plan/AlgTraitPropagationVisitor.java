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

package org.polypheny.db.plan;


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.util.Util;


/**
 * AlgTraitPropagationVisitor traverses a {@link AlgNode} and its <i>unregistered</i> children, making sure that each has a full complement of traits. When a {@link AlgNode} is found to be missing one or
 * more traits, they are copied from a AlgTraitSet given during construction.
 */
public class AlgTraitPropagationVisitor extends AlgVisitor {

    private final AlgTraitSet baseTraits;
    private final AlgPlanner planner;


    public AlgTraitPropagationVisitor(
            AlgPlanner planner,
            AlgTraitSet baseTraits ) {
        this.planner = planner;
        this.baseTraits = baseTraits;
    }


    @Override
    public void visit( AlgNode alg, int ordinal, AlgNode parent ) {
        // REVIEW: SWZ: We assume that any special AlgNodes, such as the VolcanoPlanner's AlgSubset always have a full complement of traits and that they
        // either appear as registered or do nothing when childrenAccept is called on them.

        if ( planner.isRegistered( alg ) ) {
            return;
        }

        AlgTraitSet algTraits = alg.getTraitSet();
        for ( int i = 0; i < baseTraits.size(); i++ ) {
            if ( i >= algTraits.size() ) {
                // Copy traits that the new alg doesn't know about.
                Util.discard( AlgOptUtil.addTrait( alg, baseTraits.getTrait( i ) ) );

                // FIXME: Return the new alg. We can no longer traits in-place, because algs and traits are immutable.
                throw new AssertionError();
            } else {
                // Verify that the traits are from the same RelTraitDef
                assert algTraits.getTrait( i ).getTraitDef() == baseTraits.getTrait( i ).getTraitDef();
            }
        }
        alg.childrenAccept( this );
    }

}


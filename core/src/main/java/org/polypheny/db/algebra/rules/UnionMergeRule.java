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

package org.polypheny.db.algebra.rules;


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Intersect;
import org.polypheny.db.algebra.core.Minus;
import org.polypheny.db.algebra.core.SetOp;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelMinus;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.Util;


/**
 * UnionMergeRule implements the rule for combining two non-distinct {@link org.polypheny.db.algebra.core.SetOp}s into a single {@link org.polypheny.db.algebra.core.SetOp}.
 *
 * Originally written for {@link Union} (hence the name), but now also applies to {@link Intersect}.
 */
public class UnionMergeRule extends AlgOptRule {

    public static final UnionMergeRule INSTANCE = new UnionMergeRule( LogicalRelUnion.class, "UnionMergeRule", AlgFactories.LOGICAL_BUILDER );
    public static final UnionMergeRule INTERSECT_INSTANCE = new UnionMergeRule( LogicalRelIntersect.class, "IntersectMergeRule", AlgFactories.LOGICAL_BUILDER );
    public static final UnionMergeRule MINUS_INSTANCE = new UnionMergeRule( LogicalRelMinus.class, "MinusMergeRule", AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a UnionMergeRule.
     */
    public UnionMergeRule( Class<? extends SetOp> unionClazz, String description, AlgBuilderFactory algBuilderFactory ) {
        super(
                operand(
                        unionClazz,
                        operand( AlgNode.class, any() ),
                        operand( AlgNode.class, any() ) ),
                algBuilderFactory, description );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final SetOp topOp = call.alg( 0 );
        @SuppressWarnings("unchecked") final Class<? extends SetOp> setOpClass = (Class) operands.get( 0 ).getMatchedClass();

        // For Union and Intersect, we want to combine the set-op that's in the second input first.
        //
        // For example, we reduce
        //    Union(Union(a, b), Union(c, d))
        // to
        //    Union(Union(a, b), c, d)
        // in preference to
        //    Union(a, b, Union(c, d))
        //
        // But for Minus, we can only reduce the left input. It is not valid to reduce
        //    Minus(a, Minus(b, c))
        // to
        //    Minus(a, b, c)
        //
        // Hence, that's why the rule pattern matches on generic RelNodes rather than explicit sub-classes of SetOp.
        // By doing so, and firing this rule in a bottom-up order, it allows us to only specify a single pattern for this rule.
        final SetOp bottomOp;
        if ( setOpClass.isInstance( call.alg( 2 ) ) && !Minus.class.isAssignableFrom( setOpClass ) ) {
            bottomOp = call.alg( 2 );
        } else if ( setOpClass.isInstance( call.alg( 1 ) ) ) {
            bottomOp = call.alg( 1 );
        } else {
            return;
        }

        // Can only combine (1) if all operators are ALL, or (2) top operator is DISTINCT (i.e. not ALL).
        // In case (2), all operators become DISTINCT.
        if ( topOp.all && !bottomOp.all ) {
            return;
        }

        // Combine the inputs from the bottom set-op with the other inputs from the top set-op.
        final AlgBuilder algBuilder = call.builder();
        if ( setOpClass.isInstance( call.alg( 2 ) ) && !Minus.class.isAssignableFrom( setOpClass ) ) {
            algBuilder.push( topOp.getInput( 0 ) );
            algBuilder.pushAll( bottomOp.getInputs() );
            // topOp.getInputs().size() may be more than 2
            for ( int index = 2; index < topOp.getInputs().size(); index++ ) {
                algBuilder.push( topOp.getInput( index ) );
            }
        } else {
            algBuilder.pushAll( bottomOp.getInputs() );
            algBuilder.pushAll( Util.skip( topOp.getInputs() ) );
        }
        int n = bottomOp.getInputs().size() + topOp.getInputs().size() - 1;
        if ( topOp instanceof Union ) {
            algBuilder.union( topOp.all, n );
        } else if ( topOp instanceof Intersect ) {
            algBuilder.intersect( topOp.all, n );
        } else if ( topOp instanceof Minus ) {
            algBuilder.minus( topOp.all, n );
        }
        call.transformTo( algBuilder.build() );
    }

}

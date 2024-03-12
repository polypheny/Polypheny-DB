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


import java.math.BigDecimal;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Intersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Util;


/**
 * Planner rule that translates a distinct {@link org.polypheny.db.algebra.core.Intersect} (<code>all</code> = <code>false</code>) into a group of operators composed of
 * {@link org.polypheny.db.algebra.core.Union}, {@link org.polypheny.db.algebra.core.Aggregate}, etc.
 *
 * Rewrite: (GB-Union All-GB)-GB-UDTF (on all attributes)
 *
 * <h3>Example</h3>
 *
 * Query: <code>R1 Intersect All R2</code>
 *
 * <code>R3 = GB(R1 on all attributes, count(*) as c)<br>
 * union all<br>
 * GB(R2 on all attributes, count(*) as c)</code>
 *
 * <code>R4 = GB(R3 on all attributes, count(c) as cnt, min(c) as m)</code>
 *
 * Note that we do not need <code>min(c)</code> in intersect distinct.
 *
 * <code>R5 = Filter(cnt == #branch)</code>
 *
 * If it is intersect all then
 *
 * <code>R6 = UDTF (R5) which will explode the tuples based on min(c)<br>
 * R7 = Project(R6 on all attributes)</code>
 *
 * Else
 *
 * <code>R6 = Proj(R5 on all attributes)</code>
 *
 * @see org.polypheny.db.algebra.rules.UnionToDistinctRule
 */
public class IntersectToDistinctRule extends AlgOptRule {

    public static final IntersectToDistinctRule INSTANCE = new IntersectToDistinctRule( LogicalRelIntersect.class, AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates an IntersectToDistinctRule.
     */
    public IntersectToDistinctRule( Class<? extends Intersect> intersectClazz, AlgBuilderFactory algBuilderFactory ) {
        super( operand( intersectClazz, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Intersect intersect = call.alg( 0 );
        if ( intersect.all ) {
            return; // nothing we can do
        }
        final AlgCluster cluster = intersect.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final AlgBuilder algBuilder = call.builder();

        // 1st level GB: create a GB (col0, col1, count() as c) for each branch
        for ( AlgNode input : intersect.getInputs() ) {
            algBuilder.push( input );
            algBuilder.aggregate( algBuilder.groupKey( algBuilder.fields() ), algBuilder.countStar( null ) );
        }

        // create a union above all the branches
        final int branchCount = intersect.getInputs().size();
        algBuilder.union( true, branchCount );
        final AlgNode union = algBuilder.peek();

        // 2nd level GB: create a GB (col0, col1, count(c)) for each branch the index of c is union.getTupleType().getFieldList().size() - 1
        final int fieldCount = union.getTupleType().getFieldCount();

        final ImmutableBitSet groupSet = ImmutableBitSet.range( fieldCount - 1 );
        algBuilder.aggregate( algBuilder.groupKey( groupSet ), algBuilder.countStar( null ) );

        // add a filter count(c) = #branches
        algBuilder.filter( algBuilder.equals( algBuilder.field( fieldCount - 1 ), rexBuilder.makeBigintLiteral( new BigDecimal( branchCount ) ) ) );

        // Project all but the last field
        algBuilder.project( Util.skipLast( algBuilder.fields() ) );

        // the schema for intersect distinct is like this
        // R3 on all attributes + count(c) as cnt
        // finally add a project to project out the last column
        call.transformTo( algBuilder.build() );
    }

}


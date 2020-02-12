/*
 * Copyright 2019-2020 The Polypheny Project
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

package ch.unibas.dmi.dbis.polyphenydb.rel.rules;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Intersect;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalIntersect;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.math.BigDecimal;


/**
 * Planner rule that translates a distinct {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Intersect} (<code>all</code> = <code>false</code>) into a group of operators composed of
 * {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Union}, {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate}, etc.
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
 * @see ch.unibas.dmi.dbis.polyphenydb.rel.rules.UnionToDistinctRule
 */
public class IntersectToDistinctRule extends RelOptRule {

    public static final IntersectToDistinctRule INSTANCE = new IntersectToDistinctRule( LogicalIntersect.class, RelFactories.LOGICAL_BUILDER );


    /**
     * Creates an IntersectToDistinctRule.
     */
    public IntersectToDistinctRule( Class<? extends Intersect> intersectClazz, RelBuilderFactory relBuilderFactory ) {
        super( operand( intersectClazz, any() ), relBuilderFactory, null );
    }


    @Override
    public void onMatch( RelOptRuleCall call ) {
        final Intersect intersect = call.rel( 0 );
        if ( intersect.all ) {
            return; // nothing we can do
        }
        final RelOptCluster cluster = intersect.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final RelBuilder relBuilder = call.builder();

        // 1st level GB: create a GB (col0, col1, count() as c) for each branch
        for ( RelNode input : intersect.getInputs() ) {
            relBuilder.push( input );
            relBuilder.aggregate( relBuilder.groupKey( relBuilder.fields() ), relBuilder.countStar( null ) );
        }

        // create a union above all the branches
        final int branchCount = intersect.getInputs().size();
        relBuilder.union( true, branchCount );
        final RelNode union = relBuilder.peek();

        // 2nd level GB: create a GB (col0, col1, count(c)) for each branch the index of c is union.getRowType().getFieldList().size() - 1
        final int fieldCount = union.getRowType().getFieldCount();

        final ImmutableBitSet groupSet = ImmutableBitSet.range( fieldCount - 1 );
        relBuilder.aggregate( relBuilder.groupKey( groupSet ), relBuilder.countStar( null ) );

        // add a filter count(c) = #branches
        relBuilder.filter( relBuilder.equals( relBuilder.field( fieldCount - 1 ), rexBuilder.makeBigintLiteral( new BigDecimal( branchCount ) ) ) );

        // Project all but the last field
        relBuilder.project( Util.skipLast( relBuilder.fields() ) );

        // the schema for intersect distinct is like this
        // R3 on all attributes + count(c) as cnt
        // finally add a project to project out the last column
        call.transformTo( relBuilder.build() );
    }
}


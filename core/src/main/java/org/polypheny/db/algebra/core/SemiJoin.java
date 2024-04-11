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

package org.polypheny.db.algebra.core;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.metadata.AlgMdUtil;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.ValidatorUtil;


/**
 * Relational expression that joins two relational expressions according to some condition, but outputs only columns from the left input, and eliminates duplicates.
 *
 * The effect is something like the SQL {@code IN} operator.
 */
public class SemiJoin extends EquiJoin {

    /**
     * Creates a SemiJoin.
     *
     * Use {@link #create} unless you know what you're doing.
     *
     * @param cluster cluster that join belongs to
     * @param traitSet Trait set
     * @param left left join input
     * @param right right join input
     * @param condition join condition
     * @param leftKeys left keys of the semijoin
     * @param rightKeys right keys of the semijoin
     */
    public SemiJoin( AlgCluster cluster, AlgTraitSet traitSet, AlgNode left, AlgNode right, RexNode condition, ImmutableList<Integer> leftKeys, ImmutableList<Integer> rightKeys ) {
        super( cluster, traitSet, left, right, condition, leftKeys, rightKeys, ImmutableSet.of(), JoinAlgType.INNER );
    }


    /**
     * Creates a SemiJoin.
     */
    public static SemiJoin create( AlgNode left, AlgNode right, RexNode condition, ImmutableList<Integer> leftKeys, ImmutableList<Integer> rightKeys ) {
        final AlgCluster cluster = left.getCluster();
        return new SemiJoin( cluster, cluster.traitSetOf( Convention.NONE ), left, right, condition, leftKeys, rightKeys );
    }


    @Override
    public SemiJoin copy( AlgTraitSet traitSet, RexNode condition, AlgNode left, AlgNode right, JoinAlgType joinType, boolean semiJoinDone ) {
        assert joinType == JoinAlgType.INNER;
        final JoinInfo joinInfo = JoinInfo.of( left, right, condition );
        assert joinInfo.isEqui();
        return new SemiJoin( getCluster(), traitSet, left, right, condition, joinInfo.leftKeys, joinInfo.rightKeys );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        // REVIEW jvs:  Just for now...
        return planner.getCostFactory().makeTinyCost();
    }


    @Override
    public double estimateTupleCount( AlgMetadataQuery mq ) {
        return Util.first( AlgMdUtil.getSemiJoinRowCount( mq, left, right, joinType, condition ), 1D );
    }


    /**
     * {@inheritDoc}
     *
     * In the case of semi-join, the row type consists of columns from left input only.
     */
    @Override
    public AlgDataType deriveRowType() {
        return ValidatorUtil.deriveJoinRowType(
                left.getTupleType(),
                null,
                JoinAlgType.INNER,
                getCluster().getTypeFactory(),
                null );
    }

}


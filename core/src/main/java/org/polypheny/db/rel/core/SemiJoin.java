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

package org.polypheny.db.rel.core;


import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.metadata.RelMdUtil;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.validate.SqlValidatorUtil;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Util;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


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
    public SemiJoin( RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition, ImmutableIntList leftKeys, ImmutableIntList rightKeys ) {
        super( cluster, traitSet, left, right, condition, leftKeys, rightKeys, ImmutableSet.of(), JoinRelType.INNER );
    }


    /**
     * Creates a SemiJoin.
     */
    public static SemiJoin create( RelNode left, RelNode right, RexNode condition, ImmutableIntList leftKeys, ImmutableIntList rightKeys ) {
        final RelOptCluster cluster = left.getCluster();
        return new SemiJoin( cluster, cluster.traitSetOf( Convention.NONE ), left, right, condition, leftKeys, rightKeys );
    }


    @Override
    public SemiJoin copy( RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone ) {
        assert joinType == JoinRelType.INNER;
        final JoinInfo joinInfo = JoinInfo.of( left, right, condition );
        assert joinInfo.isEqui();
        return new SemiJoin( getCluster(), traitSet, left, right, condition, joinInfo.leftKeys, joinInfo.rightKeys );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        // REVIEW jvs:  Just for now...
        return planner.getCostFactory().makeTinyCost();
    }


    @Override
    public double estimateRowCount( RelMetadataQuery mq ) {
        return Util.first( RelMdUtil.getSemiJoinRowCount( mq, left, right, joinType, condition ), 1D );
    }


    /**
     * {@inheritDoc}
     *
     * In the case of semi-join, the row type consists of columns from left input only.
     */
    @Override
    public RelDataType deriveRowType() {
        return SqlValidatorUtil.deriveJoinRowType(
                left.getRowType(),
                null,
                JoinRelType.INNER,
                getCluster().getTypeFactory(),
                null,
                ImmutableList.of() );
    }
}


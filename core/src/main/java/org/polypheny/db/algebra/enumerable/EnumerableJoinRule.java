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

package org.polypheny.db.algebra.enumerable;


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.JoinInfo;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.Convention;


/**
 * Planner rule that converts a {@link LogicalRelJoin} relational expression {@link EnumerableConvention enumerable calling convention}.
 */
class EnumerableJoinRule extends ConverterRule {

    EnumerableJoinRule() {
        super( LogicalRelJoin.class, Convention.NONE, EnumerableConvention.INSTANCE, "EnumerableJoinRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        LogicalRelJoin join = (LogicalRelJoin) alg;
        List<AlgNode> newInputs = new ArrayList<>();
        for ( AlgNode input : join.getInputs() ) {
            if ( !(input.getConvention() instanceof EnumerableConvention) ) {
                input = convert( input, input.getTraitSet().replace( EnumerableConvention.INSTANCE ) );
            }
            newInputs.add( input );
        }
        final AlgCluster cluster = join.getCluster();
        final AlgNode left = newInputs.get( 0 );
        final AlgNode right = newInputs.get( 1 );
        final JoinInfo info = JoinInfo.of( left, right, join.getCondition() );
        if ( !info.isEqui() && join.getJoinType() != JoinAlgType.INNER ) {
            // EnumerableJoinRel only supports equi-join. We can put a filter on top if it is an inner join.
            try {
                return EnumerableThetaJoin.create( left, right, join.getCondition(), join.getVariablesSet(), join.getJoinType() );
            } catch ( InvalidAlgException e ) {
                EnumerableRules.LOGGER.debug( e.toString() );
                return null;
            }
        }
        AlgNode newAlg;
        try {
            newAlg = EnumerableJoin.create( left, right, info.getEquiCondition( left, right, cluster.getRexBuilder() ), info.leftKeys, info.rightKeys, join.getVariablesSet(), join.getJoinType() );
        } catch ( InvalidAlgException e ) {
            EnumerableRules.LOGGER.debug( e.toString() );
            return null;
        }
        if ( !info.isEqui() ) {
            newAlg = new EnumerableFilter( cluster, newAlg.getTraitSet(), newAlg, info.getRemaining( cluster.getRexBuilder() ) );
        }
        return newAlg;
    }

}


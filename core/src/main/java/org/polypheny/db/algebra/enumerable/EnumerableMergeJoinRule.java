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
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.JoinInfo;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Planner rule that converts a {@link LogicalRelJoin} relational expression {@link EnumerableConvention enumerable calling convention}.
 *
 * @see EnumerableJoinRule
 */
class EnumerableMergeJoinRule extends ConverterRule {

    EnumerableMergeJoinRule() {
        super( LogicalRelJoin.class, Convention.NONE, EnumerableConvention.INSTANCE, EnumerableMergeJoinRule.class.getSimpleName() );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        LogicalRelJoin join = (LogicalRelJoin) alg;
        final JoinInfo info = JoinInfo.of( join.getLeft(), join.getRight(), join.getCondition() );
        if ( join.getJoinType() != JoinAlgType.INNER ) {
            // EnumerableMergeJoin only supports inner join. (It supports non-equi join, using a post-filter; see below.)
            return null;
        }
        if ( info.pairs().isEmpty() ) {
            // EnumerableMergeJoin CAN support cartesian join, but disable it for now.
            return null;
        }
        final List<AlgNode> newInputs = new ArrayList<>();
        final List<AlgCollation> collations = new ArrayList<>();
        int offset = 0;
        for ( Ord<AlgNode> ord : Ord.zip( join.getInputs() ) ) {
            AlgTraitSet traits = ord.e.getTraitSet().replace( EnumerableConvention.INSTANCE );
            if ( !info.pairs().isEmpty() ) {
                final List<AlgFieldCollation> fieldCollations = new ArrayList<>();
                for ( int key : info.keys().get( ord.i ) ) {
                    fieldCollations.add( new AlgFieldCollation( key, AlgFieldCollation.Direction.ASCENDING, AlgFieldCollation.NullDirection.LAST ) );
                }
                final AlgCollation collation = AlgCollations.of( fieldCollations );
                collations.add( AlgCollations.shift( collation, offset ) );
                traits = traits.replace( collation );
            }
            newInputs.add( convert( ord.e, traits ) );
            offset += ord.e.getTupleType().getFieldCount();
        }
        final AlgNode left = newInputs.get( 0 );
        final AlgNode right = newInputs.get( 1 );
        final AlgCluster cluster = join.getCluster();
        AlgNode newRel;
        try {
            AlgTraitSet traits = join.getTraitSet().replace( EnumerableConvention.INSTANCE );
            if ( !collations.isEmpty() ) {
                traits = traits.replace( collations );
            }
            newRel = new EnumerableMergeJoin(
                    cluster,
                    traits,
                    left,
                    right,
                    info.getEquiCondition( left, right, cluster.getRexBuilder() ),
                    info.leftKeys,
                    info.rightKeys,
                    join.getVariablesSet(),
                    join.getJoinType() );
        } catch ( InvalidAlgException e ) {
            EnumerableRules.LOGGER.debug( e.toString() );
            return null;
        }
        if ( !info.isEqui() ) {
            newRel = new EnumerableFilter( cluster, newRel.getTraitSet(), newRel, info.getRemaining( cluster.getRexBuilder() ) );
        }
        return newRel;
    }

}


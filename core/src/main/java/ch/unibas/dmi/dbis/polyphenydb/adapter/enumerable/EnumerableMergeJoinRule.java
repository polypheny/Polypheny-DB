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

package ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.InvalidRelException;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollations;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinInfo;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.Ord;


/**
 * Planner rule that converts a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin} relational expression {@link EnumerableConvention enumerable calling convention}.
 *
 * @see ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableJoinRule
 */
class EnumerableMergeJoinRule extends ConverterRule {

    EnumerableMergeJoinRule() {
        super( LogicalJoin.class, Convention.NONE, EnumerableConvention.INSTANCE, "EnumerableMergeJoinRule" );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        LogicalJoin join = (LogicalJoin) rel;
        final JoinInfo info = JoinInfo.of( join.getLeft(), join.getRight(), join.getCondition() );
        if ( join.getJoinType() != JoinRelType.INNER ) {
            // EnumerableMergeJoin only supports inner join. (It supports non-equi join, using a post-filter; see below.)
            return null;
        }
        if ( info.pairs().size() == 0 ) {
            // EnumerableMergeJoin CAN support cartesian join, but disable it for now.
            return null;
        }
        final List<RelNode> newInputs = new ArrayList<>();
        final List<RelCollation> collations = new ArrayList<>();
        int offset = 0;
        for ( Ord<RelNode> ord : Ord.zip( join.getInputs() ) ) {
            RelTraitSet traits = ord.e.getTraitSet().replace( EnumerableConvention.INSTANCE );
            if ( !info.pairs().isEmpty() ) {
                final List<RelFieldCollation> fieldCollations = new ArrayList<>();
                for ( int key : info.keys().get( ord.i ) ) {
                    fieldCollations.add( new RelFieldCollation( key, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST ) );
                }
                final RelCollation collation = RelCollations.of( fieldCollations );
                collations.add( RelCollations.shift( collation, offset ) );
                traits = traits.replace( collation );
            }
            newInputs.add( convert( ord.e, traits ) );
            offset += ord.e.getRowType().getFieldCount();
        }
        final RelNode left = newInputs.get( 0 );
        final RelNode right = newInputs.get( 1 );
        final RelOptCluster cluster = join.getCluster();
        RelNode newRel;
        try {
            RelTraitSet traits = join.getTraitSet().replace( EnumerableConvention.INSTANCE );
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
        } catch ( InvalidRelException e ) {
            EnumerableRules.LOGGER.debug( e.toString() );
            return null;
        }
        if ( !info.isEqui() ) {
            newRel = new EnumerableFilter( cluster, newRel.getTraitSet(), newRel, info.getRemaining( cluster.getRexBuilder() ) );
        }
        return newRel;
    }
}


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

package ch.unibas.dmi.dbis.polyphenydb.rel.logical;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelInput;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelShuttle;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.CorrelationId;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;


/**
 * Sub-class of {@link Join} not targeted at any particular engine or calling convention.
 *
 * Some rules:
 *
 * <ul>
 * <li>{@link ch.unibas.dmi.dbis.polyphenydb.rel.rules.JoinExtractFilterRule} converts an {@link LogicalJoin inner join} to a {@link LogicalFilter filter} on top of a {@link LogicalJoin cartesian inner join}.</li>
 * <li>{@code net.sf.farrago.fennel.rel.FennelCartesianJoinRule} implements a LogicalJoin as a cartesian product.</li>
 * </ul>
 */
public final class LogicalJoin extends Join {

    // NOTE jvs 14-Mar-2006:  Normally we don't use state like this to control rule firing, but due to the non-local nature of semijoin optimizations, it's pretty much required.
    private final boolean semiJoinDone;

    private final ImmutableList<RelDataTypeField> systemFieldList;


    /**
     * Creates a LogicalJoin.
     *
     * Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param left Left input
     * @param right Right input
     * @param condition Join condition
     * @param joinType Join type
     * @param variablesSet Set of variables that are set by the LHS and used by the RHS and are not available to nodes above this LogicalJoin in the tree
     * @param semiJoinDone Whether this join has been translated to a semi-join
     * @param systemFieldList List of system fields that will be prefixed to output row type; typically empty but must not be null
     * @see #isSemiJoinDone()
     */
    public LogicalJoin( RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone, ImmutableList<RelDataTypeField> systemFieldList ) {
        super( cluster, traitSet, left, right, condition, variablesSet, joinType );
        this.semiJoinDone = semiJoinDone;
        this.systemFieldList = Objects.requireNonNull( systemFieldList );
    }


    @Deprecated // to be removed before 2.0
    public LogicalJoin( RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition, JoinRelType joinType, Set<String> variablesStopped, boolean semiJoinDone, ImmutableList<RelDataTypeField> systemFieldList ) {
        this( cluster, traitSet, left, right, condition, CorrelationId.setOf( variablesStopped ), joinType, semiJoinDone, systemFieldList );
    }


    @Deprecated // to be removed before 2.0
    public LogicalJoin( RelOptCluster cluster, RelNode left, RelNode right, RexNode condition, JoinRelType joinType, Set<String> variablesStopped ) {
        this( cluster, cluster.traitSetOf( Convention.NONE ), left, right, condition, CorrelationId.setOf( variablesStopped ), joinType, false, ImmutableList.of() );
    }


    @Deprecated // to be removed before 2.0
    public LogicalJoin( RelOptCluster cluster, RelNode left, RelNode right, RexNode condition, JoinRelType joinType, Set<String> variablesStopped, boolean semiJoinDone, ImmutableList<RelDataTypeField> systemFieldList ) {
        this( cluster, cluster.traitSetOf( Convention.NONE ), left, right, condition, CorrelationId.setOf( variablesStopped ), joinType, semiJoinDone, systemFieldList );
    }


    /**
     * Creates a LogicalJoin by parsing serialized output.
     */
    public LogicalJoin( RelInput input ) {
        this( input.getCluster(), input.getCluster().traitSetOf( Convention.NONE ), input.getInputs().get( 0 ), input.getInputs().get( 1 ), input.getExpression( "condition" ),
                ImmutableSet.of(), input.getEnum( "joinType", JoinRelType.class ), false, ImmutableList.of() );
    }


    /**
     * Creates a LogicalJoin, flagged with whether it has been translated to a semi-join.
     */
    public static LogicalJoin create( RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone, ImmutableList<RelDataTypeField> systemFieldList ) {
        final RelOptCluster cluster = left.getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalJoin( cluster, traitSet, left, right, condition, variablesSet, joinType, semiJoinDone, systemFieldList );
    }


    @Deprecated // to be removed before 2.0
    public static LogicalJoin create( RelNode left, RelNode right, RexNode condition, JoinRelType joinType, Set<String> variablesStopped, boolean semiJoinDone, ImmutableList<RelDataTypeField> systemFieldList ) {
        return create( left, right, condition, CorrelationId.setOf( variablesStopped ), joinType, semiJoinDone, systemFieldList );
    }


    /**
     * Creates a LogicalJoin.
     */
    public static LogicalJoin create( RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType ) {
        return create( left, right, condition, variablesSet, joinType, false, ImmutableList.of() );
    }


    @Deprecated // to be removed before 2.0
    public static LogicalJoin create( RelNode left, RelNode right, RexNode condition, JoinRelType joinType, Set<String> variablesStopped ) {
        return create( left, right, condition, CorrelationId.setOf( variablesStopped ), joinType, false, ImmutableList.of() );
    }


    @Override
    public LogicalJoin copy( RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalJoin( getCluster(), getCluster().traitSetOf( Convention.NONE ), left, right, conditionExpr, variablesSet, joinType, semiJoinDone, systemFieldList );
    }


    @Override
    public RelNode accept( RelShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        // Don't ever print semiJoinDone=false. This way, we don't clutter things up in optimizers that don't use semi-joins.
        return super.explainTerms( pw ).itemIf( "semiJoinDone", semiJoinDone, semiJoinDone );
    }


    @Override
    public boolean isSemiJoinDone() {
        return semiJoinDone;
    }


    @Override
    public List<RelDataTypeField> getSystemFieldList() {
        return systemFieldList;
    }
}


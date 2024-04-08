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

package org.polypheny.db.algebra.logical.relational;


import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;


/**
 * Sub-class of {@link Join} not targeted at any particular engine or calling convention.
 *
 * Some rules:
 *
 * <ul>
 * <li>{@link org.polypheny.db.algebra.rules.JoinExtractFilterRule} converts an {@link LogicalRelJoin inner join} to a {@link LogicalRelFilter filter} on top of a {@link LogicalRelJoin cartesian inner join}.</li>
 * <li>{@code net.sf.farrago.fennel.alg.FennelCartesianJoinRule} implements a LogicalJoin as a cartesian product.</li>
 * </ul>
 */
public final class LogicalRelJoin extends Join implements RelAlg {

    // NOTE jvs 14-Mar-2006:  Normally we don't use state like this to control rule firing, but due to the non-local nature of semijoin optimizations, it's pretty much required.
    private final boolean semiJoinDone;


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
     * @see #isSemiJoinDone()
     */
    public LogicalRelJoin( AlgCluster cluster, AlgTraitSet traitSet, AlgNode left, AlgNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinAlgType joinType, boolean semiJoinDone ) {
        super( cluster, traitSet.replace( ModelTrait.RELATIONAL ), left, right, condition, variablesSet, joinType );
        this.semiJoinDone = semiJoinDone;
    }


    /**
     * Creates a LogicalJoin, flagged with whether it has been translated to a semi-join.
     */
    public static LogicalRelJoin create( AlgNode left, AlgNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinAlgType joinType, boolean semiJoinDone ) {
        final AlgCluster cluster = left.getCluster();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalRelJoin( cluster, traitSet, left, right, condition, variablesSet, joinType, semiJoinDone );
    }


    @Deprecated // to be removed before 2.0
    public static LogicalRelJoin create( AlgNode left, AlgNode right, RexNode condition, JoinAlgType joinType, Set<String> variablesStopped, boolean semiJoinDone ) {
        return create( left, right, condition, CorrelationId.setOf( variablesStopped ), joinType, semiJoinDone );
    }


    /**
     * Creates a LogicalJoin.
     */
    public static LogicalRelJoin create( AlgNode left, AlgNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinAlgType joinType ) {
        return create( left, right, condition, variablesSet, joinType, false );
    }


    @Deprecated // to be removed before 2.0
    public static LogicalRelJoin create( AlgNode left, AlgNode right, RexNode condition, JoinAlgType joinType, Set<String> variablesStopped ) {
        return create( left, right, condition, CorrelationId.setOf( variablesStopped ), joinType, false );
    }


    @Override
    public LogicalRelJoin copy( AlgTraitSet traitSet, RexNode conditionExpr, AlgNode left, AlgNode right, JoinAlgType joinType, boolean semiJoinDone ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalRelJoin( getCluster(), getCluster().traitSetOf( Convention.NONE ).replace( ModelTrait.RELATIONAL ), left, right, conditionExpr, variablesSet, joinType, semiJoinDone );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        // Don't ever print semiJoinDone=false. This way, we don't clutter things up in optimizers that don't use semi-joins.
        return super.explainTerms( pw ).itemIf( "semiJoinDone", semiJoinDone, semiJoinDone );
    }


    @Override
    public boolean isSemiJoinDone() {
        return semiJoinDone;
    }


}

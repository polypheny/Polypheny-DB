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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.metadata.AlgMdUtil;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.rules.JoinAddRedundantSemiJoinRule;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexChecker;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.ValidatorUtil;


/**
 * Relational expression that combines two relational expressions according to some condition.
 * <p>
 * Each output row has columns from the left and right inputs. The set of output rows is a subset of the cartesian product
 * of the two inputs; precisely which subset depends on the join condition.
 */
public abstract class Join extends BiAlg {

    @Getter
    protected final RexNode condition;
    protected final ImmutableSet<CorrelationId> variablesSet;

    /**
     * Values must be of enumeration {@link JoinAlgType}, except that {@link JoinAlgType#RIGHT} is disallowed.
     */
    @Getter
    protected final JoinAlgType joinType;

    // Next time we need to change the constructor of Join, let's change the "Set<String> variablesStopped" parameter to "Set<CorrelationId> variablesSet".
    // At that point we would deprecate AlgNode.getVariablesStopped().


    /**
     * Creates a Join.
     * <p>
     * Note: We plan to change the {@code variablesStopped} parameter to {@code Set&lt;CorrelationId&gt; variablesSet}
     * because {@link #getVariablesSet()} is preferred over {@link #getVariablesStopped()}. This constructor is not
     * deprecated, for now, because maintaining overloaded constructors in multiple sub-classes would be onerous.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param left Left input
     * @param right Right input
     * @param condition Join condition
     * @param joinType Join type
     * @param variablesSet Set variables that are set by the LHS and used by the RHS and are not available to nodes above this Join in the tree
     */
    protected Join( AlgCluster cluster, AlgTraitSet traitSet, AlgNode left, AlgNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinAlgType joinType ) {
        super( cluster, traitSet, left, right );
        this.condition = Objects.requireNonNull( condition );
        this.variablesSet = ImmutableSet.copyOf( variablesSet );
        this.joinType = Objects.requireNonNull( joinType );
    }


    @Override
    public List<RexNode> getChildExps() {
        return ImmutableList.of( condition );
    }


    @Override
    public AlgNode accept( RexShuttle shuttle ) {
        RexNode condition = shuttle.apply( this.condition );
        if ( this.condition == condition ) {
            return this;
        }
        return copy( traitSet, condition, left, right, joinType, isSemiJoinDone() );
    }


    @Override
    public boolean isValid( Litmus litmus, Context context ) {
        if ( !super.isValid( litmus, context ) ) {
            return false;
        }
        if ( getTupleType().getFieldCount() != left.getTupleType().getFieldCount() + (this instanceof SemiJoin ? 0 : right.getTupleType().getFieldCount()) ) {
            return litmus.fail( "field count mismatch" );
        }
        if ( condition != null ) {
            if ( condition.getType().getPolyType() != PolyType.BOOLEAN ) {
                return litmus.fail( "condition must be boolean: {}", condition.getType() );
            }
            // The input to the condition is a row type consisting of system fields, left fields, and right fields. Very similar to the output row type, except that fields
            // have not yet been made due to outer joins.
            RexChecker checker =
                    new RexChecker(
                            getCluster().getTypeFactory().builder()
                                    .addAll( getLeft().getTupleType().getFields() )
                                    .addAll( getRight().getTupleType().getFields() )
                                    .build(),
                            context, litmus );
            condition.accept( checker );
            if ( checker.getFailureCount() > 0 ) {
                return litmus.fail( checker.getFailureCount() + " failures in condition " + condition );
            }
        }
        return litmus.succeed();
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        // REVIEW jvs: Just for now...
        double rowCount = mq.getTupleCount( this );
        return planner.getCostFactory().makeCost( rowCount, 0, 0 );
    }


    @Override
    public double estimateTupleCount( AlgMetadataQuery mq ) {
        return Util.first( AlgMdUtil.getJoinRowCount( mq, this, condition ), 1D );
    }


    @Override
    public ImmutableSet<CorrelationId> getVariablesSet() {
        return variablesSet;
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .item( "condition", condition )
                .item( "joinType", joinType.lowerName );
    }


    @Override
    protected AlgDataType deriveRowType() {
        return ValidatorUtil.deriveJoinRowType( left.getTupleType(), right.getTupleType(), joinType, getCluster().getTypeFactory(), null );
    }


    /**
     * Returns whether this LogicalJoin has already spawned a {@link SemiJoin} via {@link JoinAddRedundantSemiJoinRule}.
     * <p>
     * The base implementation returns false.
     *
     * @return whether this join has already spawned a semi join
     */
    public boolean isSemiJoinDone() {
        return false;
    }



    @Override
    public final Join copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert inputs.size() == 2;
        return copy( traitSet, getCondition(), inputs.get( 0 ), inputs.get( 1 ), joinType, isSemiJoinDone() );
    }


    /**
     * Creates a copy of this join, overriding condition, system fields and inputs.
     *
     * General contract as {@link AlgNode#copy}.
     *
     * @param traitSet Traits
     * @param conditionExpr Condition
     * @param left Left input
     * @param right Right input
     * @param joinType Join type
     * @param semiJoinDone Whether this join has been translated to a semi-join
     * @return Copy of this join
     */
    @SuppressWarnings("JavadocBlankLines")
    public abstract Join copy( AlgTraitSet traitSet, RexNode conditionExpr, AlgNode left, AlgNode right, JoinAlgType joinType, boolean semiJoinDone );


    @Override
    public boolean containsJoin() {
        return true;
    }


    /**
     * Analyzes the join condition.
     *
     * @return Analyzed join condition
     */
    public JoinInfo analyzeCondition() {
        return JoinInfo.of( left, right, condition );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                left.algCompareString() + "$" +
                right.algCompareString() + "$" +
                (condition != null ? condition.hashCode() : "") + "$" +
                (joinType != null ? joinType.name() : "") + "&";
    }


}


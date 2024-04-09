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
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.metadata.AlgMdUtil;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexChecker;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.util.Litmus;


/**
 * Relational expression that iterates over its input and returns elements for which
 * <code>condition</code> evaluates to <code>true</code>.
 *
 * If the condition allows nulls, then a null value is treated the same as false.
 *
 * @see LogicalRelFilter
 */
@Getter
public abstract class Filter extends SingleAlg {

    protected final RexNode condition;


    /**
     * Creates a filter.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param traits the traits of this rel
     * @param child input relational expression
     * @param condition boolean expression which determines whether a row is allowed to pass
     */
    protected Filter( AlgCluster cluster, AlgTraitSet traits, AlgNode child, RexNode condition ) {
        super( cluster, traits, child );
        assert condition != null;
        assert RexUtil.isFlat( condition ) : condition;
        this.condition = condition;
        // Too expensive for everyday use:
        assert !RuntimeConfig.DEBUG.getBoolean() || isValid( Litmus.THROW, null );
    }


    @Override
    public final AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return copy( traitSet, sole( inputs ), getCondition() );
    }


    public abstract Filter copy( AlgTraitSet traitSet, AlgNode input, RexNode condition );


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
        return copy( traitSet, getInput(), condition );
    }


    @Override
    public boolean isValid( Litmus litmus, Context context ) {
        if ( RexUtil.isNullabilityCast( getCluster().getTypeFactory(), condition ) ) {
            return litmus.fail( "Cast for just nullability not allowed" );
        }
        final RexChecker checker = new RexChecker( getInput().getTupleType(), context, litmus );
        condition.accept( checker );
        if ( checker.getFailureCount() > 0 ) {
            return litmus.fail( null );
        }
        return litmus.succeed();
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        double dRows = mq.getTupleCount( this );
        double dCpu = mq.getTupleCount( getInput() );
        double dIo = 0;
        return planner.getCostFactory().makeCost( dRows, dCpu, dIo );
    }


    @Override
    public double estimateTupleCount( AlgMetadataQuery mq ) {
        return AlgMdUtil.estimateFilteredRows( getInput(), condition, mq );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "condition", condition );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                (condition != null ? condition.hashCode() : "") + "&";
    }

}

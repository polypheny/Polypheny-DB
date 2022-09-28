/*
 * Copyright 2019-2022 The Polypheny Project
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


import java.util.List;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.rules.AggregateProjectPullUpConstantsRule;
import org.polypheny.db.algebra.rules.AggregateReduceFunctionsRule;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * <code>LogicalAggregate</code> is a relational operator which eliminates duplicates and computes totals.
 *
 * Rules:
 *
 * <ul>
 * <li>{@link AggregateProjectPullUpConstantsRule}</li>
 * <li>{@link org.polypheny.db.algebra.rules.AggregateExpandDistinctAggregatesRule}</li>
 * <li>{@link AggregateReduceFunctionsRule}</li>
 * </ul>
 */
public final class LogicalAggregate extends Aggregate {

    /**
     * Creates a LogicalAggregate.
     *
     * Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param child input relational expression
     * @param groupSet Bit set of grouping fields
     * @param groupSets Grouping sets, or null to use just {@code groupSet}
     * @param aggCalls Array of aggregates to compute, not null
     */
    public LogicalAggregate( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        super( cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls );
    }


    /**
     * Creates a LogicalAggregate by parsing serialized output.
     */
    public LogicalAggregate( AlgInput input ) {
        super( input );
    }


    /**
     * Creates a LogicalAggregate.
     */
    public static LogicalAggregate create( final AlgNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        return create_( input, false, groupSet, groupSets, aggCalls );
    }


    @Deprecated // to be removed before 2.0
    public static LogicalAggregate create( final AlgNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        return create_( input, indicator, groupSet, groupSets, aggCalls );
    }


    private static LogicalAggregate create_( final AlgNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        final AlgOptCluster cluster = input.getCluster();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalAggregate( cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls );
    }


    @Override
    public LogicalAggregate copy( AlgTraitSet traitSet, AlgNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalAggregate( getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}


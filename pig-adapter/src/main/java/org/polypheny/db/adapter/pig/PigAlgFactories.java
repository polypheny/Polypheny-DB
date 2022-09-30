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

package org.polypheny.db.adapter.pig;


import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.AlgFactories.ScanFactory;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Implementations of factories in {@link AlgFactories} for the Pig adapter.
 */
public class PigAlgFactories {

    public static final Context ALL_PIG_REL_FACTORIES = Contexts.of( PigScanFactory.INSTANCE, PigFilterFactory.INSTANCE, PigAggregateFactory.INSTANCE, PigJoinFactory.INSTANCE );


    // prevent instantiation
    private PigAlgFactories() {
    }


    /**
     * Implementation of {@link ScanFactory} that returns a {@link PigScan}.
     */
    public static class PigScanFactory implements ScanFactory {

        public static final PigScanFactory INSTANCE = new PigScanFactory();


        @Override
        public AlgNode createScan( AlgOptCluster cluster, AlgOptTable table ) {
            return new PigScan( cluster, cluster.traitSetOf( PigAlg.CONVENTION ), table );
        }

    }


    /**
     * Implementation of {@link AlgFactories.FilterFactory} that returns a {@link PigFilter}.
     */
    public static class PigFilterFactory implements AlgFactories.FilterFactory {

        public static final PigFilterFactory INSTANCE = new PigFilterFactory();


        @Override
        public AlgNode createFilter( AlgNode input, RexNode condition ) {
            return new PigFilter( input.getCluster(), input.getTraitSet().replace( PigAlg.CONVENTION ), input, condition );
        }

    }


    /**
     * Implementation of {@link AlgFactories.AggregateFactory} that returns a {@link PigAggregate}.
     */
    public static class PigAggregateFactory implements AlgFactories.AggregateFactory {

        public static final PigAggregateFactory INSTANCE = new PigAggregateFactory();


        @Override
        public AlgNode createAggregate( AlgNode input, boolean indicator, ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
            return new PigAggregate( input.getCluster(), input.getTraitSet(), input, indicator, groupSet, groupSets, aggCalls );
        }

    }


    /**
     * Implementation of {@link AlgFactories.JoinFactory} that returns a {@link PigJoin}.
     */
    public static class PigJoinFactory implements AlgFactories.JoinFactory {

        public static final PigJoinFactory INSTANCE = new PigJoinFactory();


        @Override
        public AlgNode createJoin( AlgNode left, AlgNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinAlgType joinType, boolean semiJoinDone ) {
            return new PigJoin( left.getCluster(), left.getTraitSet(), left, right, condition, joinType );
        }

    }

}


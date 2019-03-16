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

package ch.unibas.dmi.dbis.polyphenydb.adapter.pig;


import ch.unibas.dmi.dbis.polyphenydb.plan.Contexts;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.AggregateCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.plan.Context;
import ch.unibas.dmi.dbis.polyphenydb.plan.Contexts;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.AggregateCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.CorrelationId;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Set;


/**
 * Implementations of factories in {@link RelFactories} for the Pig adapter.
 */
public class PigRelFactories {

    public static final Context ALL_PIG_REL_FACTORIES = Contexts.of( PigTableScanFactory.INSTANCE, PigFilterFactory.INSTANCE, PigAggregateFactory.INSTANCE, PigJoinFactory.INSTANCE );


    // prevent instantiation
    private PigRelFactories() {
    }


    /**
     * Implementation of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories.TableScanFactory} that returns a {@link PigTableScan}.
     */
    public static class PigTableScanFactory implements RelFactories.TableScanFactory {

        public static final PigTableScanFactory INSTANCE = new PigTableScanFactory();


        @Override
        public RelNode createScan( RelOptCluster cluster, RelOptTable table ) {
            return new PigTableScan( cluster, cluster.traitSetOf( PigRel.CONVENTION ), table );
        }
    }


    /**
     * Implementation of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories.FilterFactory} that returns a {@link PigFilter}.
     */
    public static class PigFilterFactory implements RelFactories.FilterFactory {

        public static final PigFilterFactory INSTANCE = new PigFilterFactory();


        @Override
        public RelNode createFilter( RelNode input, RexNode condition ) {
            return new PigFilter( input.getCluster(), input.getTraitSet().replace( PigRel.CONVENTION ), input, condition );
        }
    }


    /**
     * Implementation of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories.AggregateFactory} that returns a {@link PigAggregate}.
     */
    public static class PigAggregateFactory implements RelFactories.AggregateFactory {

        public static final PigAggregateFactory INSTANCE = new PigAggregateFactory();


        @Override
        public RelNode createAggregate( RelNode input, boolean indicator, ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
            return new PigAggregate( input.getCluster(), input.getTraitSet(), input, indicator, groupSet, groupSets, aggCalls );
        }
    }


    /**
     * Implementation of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.RelFactories.JoinFactory} that returns a {@link PigJoin}.
     */
    public static class PigJoinFactory implements RelFactories.JoinFactory {

        public static final PigJoinFactory INSTANCE = new PigJoinFactory();


        @Override
        public RelNode createJoin( RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone ) {
            return new PigJoin( left.getCluster(), left.getTraitSet(), left, right, condition, joinType );
        }


        @SuppressWarnings("deprecation")
        @Override
        public RelNode createJoin( RelNode left, RelNode right, RexNode condition, JoinRelType joinType, Set<String> variablesStopped, boolean semiJoinDone ) {
            return new PigJoin( left.getCluster(), left.getTraitSet(), left, right, condition, joinType );
        }
    }
}


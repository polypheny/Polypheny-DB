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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.logical.relational.LogicalAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalJoin;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Various {@link AlgOptRule}s using the Pig convention.
 */
public class PigRules {

    public static final List<ConverterRule> ALL_PIG_OPT_RULES = ImmutableList.of(
            PigFilterRule.INSTANCE,
            PigScanRule.INSTANCE,
            PigProjectRule.INSTANCE,
            PigAggregateRule.INSTANCE,
            PigJoinRule.INSTANCE );


    // prevent instantiation
    private PigRules() {
    }


    /**
     * Rule to convert a {@link LogicalFilter} to a {@link PigFilter}.
     */
    private static class PigFilterRule extends ConverterRule {

        private static final PigFilterRule INSTANCE = new PigFilterRule();


        private PigFilterRule() {
            super( LogicalFilter.class, Convention.NONE, PigAlg.CONVENTION, "PigFilterRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalFilter filter = (LogicalFilter) alg;
            final AlgTraitSet traitSet = filter.getTraitSet().replace( PigAlg.CONVENTION );
            return new PigFilter( alg.getCluster(), traitSet, convert( filter.getInput(), PigAlg.CONVENTION ), filter.getCondition() );
        }

    }


    /**
     * Rule to convert a {@link LogicalScan} to a {@link PigScan}.
     */
    private static class PigScanRule extends ConverterRule {

        private static final PigScanRule INSTANCE = new PigScanRule();


        private PigScanRule() {
            super( LogicalScan.class, Convention.NONE, PigAlg.CONVENTION, "PigScanRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalScan scan = (LogicalScan) alg;
            final AlgTraitSet traitSet = scan.getTraitSet().replace( PigAlg.CONVENTION );
            return new PigScan( alg.getCluster(), traitSet, scan.getTable() );
        }

    }


    /**
     * Rule to convert a {@link LogicalProject} to a {@link PigProject}.
     */
    private static class PigProjectRule extends ConverterRule {

        private static final PigProjectRule INSTANCE = new PigProjectRule();


        private PigProjectRule() {
            super( LogicalProject.class, Convention.NONE, PigAlg.CONVENTION, "PigProjectRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalProject project = (LogicalProject) alg;
            final AlgTraitSet traitSet = project.getTraitSet().replace( PigAlg.CONVENTION );
            return new PigProject( project.getCluster(), traitSet, project.getInput(), project.getProjects(), project.getRowType() );
        }

    }


    /**
     * Rule to convert a {@link LogicalAggregate} to a {@link PigAggregate}.
     */
    private static class PigAggregateRule extends ConverterRule {

        private static final PigAggregateRule INSTANCE = new PigAggregateRule();


        private PigAggregateRule() {
            super( LogicalAggregate.class, Convention.NONE, PigAlg.CONVENTION, "PigAggregateRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalAggregate agg = (LogicalAggregate) alg;
            final AlgTraitSet traitSet = agg.getTraitSet().replace( PigAlg.CONVENTION );
            return new PigAggregate( agg.getCluster(), traitSet, agg.getInput(), agg.indicator, agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList() );
        }

    }


    /**
     * Rule to convert a {@link LogicalJoin} to a {@link PigJoin}.
     */
    private static class PigJoinRule extends ConverterRule {

        private static final PigJoinRule INSTANCE = new PigJoinRule();


        private PigJoinRule() {
            super( LogicalJoin.class, Convention.NONE, PigAlg.CONVENTION, "PigJoinRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalJoin join = (LogicalJoin) alg;
            final AlgTraitSet traitSet = join.getTraitSet().replace( PigAlg.CONVENTION );
            return new PigJoin( join.getCluster(), traitSet, join.getLeft(), join.getRight(), join.getCondition(), join.getJoinType() );
        }

    }

}

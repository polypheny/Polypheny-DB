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


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalAggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableScan;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * Various {@link RelOptRule}s using the Pig convention.
 */
public class PigRules {

    public static final List<ConverterRule> ALL_PIG_OPT_RULES = ImmutableList.of( PigFilterRule.INSTANCE, PigTableScanRule.INSTANCE, PigProjectRule.INSTANCE, PigAggregateRule.INSTANCE, PigJoinRule.INSTANCE );


    // prevent instantiation
    private PigRules() {
    }


    /**
     * Rule to convert a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter} to a {@link PigFilter}.
     */
    private static class PigFilterRule extends ConverterRule {

        private static final PigFilterRule INSTANCE = new PigFilterRule();


        private PigFilterRule() {
            super( LogicalFilter.class, Convention.NONE, PigRel.CONVENTION, "PigFilterRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalFilter filter = (LogicalFilter) rel;
            final RelTraitSet traitSet = filter.getTraitSet().replace( PigRel.CONVENTION );
            return new PigFilter( rel.getCluster(), traitSet, convert( filter.getInput(), PigRel.CONVENTION ), filter.getCondition() );
        }
    }


    /**
     * Rule to convert a {@link LogicalTableScan} to a {@link PigTableScan}.
     */
    private static class PigTableScanRule extends ConverterRule {

        private static final PigTableScanRule INSTANCE = new PigTableScanRule();


        private PigTableScanRule() {
            super( LogicalTableScan.class, Convention.NONE, PigRel.CONVENTION, "PigTableScanRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalTableScan scan = (LogicalTableScan) rel;
            final RelTraitSet traitSet = scan.getTraitSet().replace( PigRel.CONVENTION );
            return new PigTableScan( rel.getCluster(), traitSet, scan.getTable() );
        }
    }


    /**
     * Rule to convert a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject} to a {@link PigProject}.
     */
    private static class PigProjectRule extends ConverterRule {

        private static final PigProjectRule INSTANCE = new PigProjectRule();


        private PigProjectRule() {
            super( LogicalProject.class, Convention.NONE, PigRel.CONVENTION, "PigProjectRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalProject project = (LogicalProject) rel;
            final RelTraitSet traitSet = project.getTraitSet().replace( PigRel.CONVENTION );
            return new PigProject( project.getCluster(), traitSet, project.getInput(), project.getProjects(), project.getRowType() );
        }
    }


    /**
     * Rule to convert a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalAggregate} to a {@link PigAggregate}.
     */
    private static class PigAggregateRule extends ConverterRule {

        private static final PigAggregateRule INSTANCE = new PigAggregateRule();


        private PigAggregateRule() {
            super( LogicalAggregate.class, Convention.NONE, PigRel.CONVENTION, "PigAggregateRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalAggregate agg = (LogicalAggregate) rel;
            final RelTraitSet traitSet = agg.getTraitSet().replace( PigRel.CONVENTION );
            return new PigAggregate( agg.getCluster(), traitSet, agg.getInput(), agg.indicator, agg.getGroupSet(), agg.getGroupSets(), agg.getAggCallList() );
        }
    }


    /**
     * Rule to convert a {@link ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalJoin} to a {@link PigJoin}.
     */
    private static class PigJoinRule extends ConverterRule {

        private static final PigJoinRule INSTANCE = new PigJoinRule();


        private PigJoinRule() {
            super( LogicalJoin.class, Convention.NONE, PigRel.CONVENTION, "PigJoinRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalJoin join = (LogicalJoin) rel;
            final RelTraitSet traitSet = join.getTraitSet().replace( PigRel.CONVENTION );
            return new PigJoin( join.getCluster(), traitSet, join.getLeft(), join.getRight(), join.getCondition(), join.getJoinType() );
        }
    }
}


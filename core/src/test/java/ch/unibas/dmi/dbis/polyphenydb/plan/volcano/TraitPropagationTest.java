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
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.plan.volcano;


import static org.junit.Assert.assertEquals;

import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableTableScan;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.Context;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbServerStatement;
import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.ConventionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptAbstractTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleOperand;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptSchema;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTrait;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.AbstractConverter.ExpandConversionRule;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbCatalogReader;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.AbstractRelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollationTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollations;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.convert.ConverterRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.AggregateCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalAggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalProject;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMdCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.SortRemoveRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Statistic;
import ch.unibas.dmi.dbis.polyphenydb.schema.Statistics;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainFormat;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.tools.FrameworkConfig;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.tools.RuleSet;
import ch.unibas.dmi.dbis.polyphenydb.tools.RuleSets;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.Test;


/**
 * Tests that determine whether trait propagation work in Volcano Planner.
 */
public class TraitPropagationTest {

    static final Convention PHYSICAL = new Convention.Impl( "PHYSICAL", Phys.class );
    static final RelCollation COLLATION = RelCollations.of( new RelFieldCollation( 0, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.FIRST ) );

    static final RuleSet RULES = RuleSets.ofList( PhysAggRule.INSTANCE, PhysProjRule.INSTANCE, PhysTableRule.INSTANCE, PhysSortRule.INSTANCE, SortRemoveRule.INSTANCE, ExpandConversionRule.INSTANCE );


    @Test
    public void testOne() throws Exception {
        RelNode planned = run( new PropAction(), RULES );
        if ( PolyphenyDbPrepareImpl.DEBUG ) {
            System.out.println( RelOptUtil.dumpPlan( "LOGICAL PLAN", planned, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES ) );
        }
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        assertEquals( "Sortedness was not propagated", 3, mq.getCumulativeCost( planned ).getRows(), 0 );
    }


    /**
     * Materialized anonymous class for simplicity
     */
    private static class PropAction {

        public RelNode apply( RelOptCluster cluster, RelOptSchema relOptSchema, SchemaPlus rootSchema ) {
            final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            final RelOptPlanner planner = cluster.getPlanner();

            final RelDataType stringType = typeFactory.createJavaType( String.class );
            final RelDataType integerType = typeFactory.createJavaType( Integer.class );
            final RelDataType sqlBigInt = typeFactory.createSqlType( SqlTypeName.BIGINT );

            // SELECT * from T;
            final Table table = new AbstractTable() {
                public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
                    return typeFactory.builder()
                            .add( "s", stringType )
                            .add( "i", integerType ).build();
                }


                @Override
                public Statistic getStatistic() {
                    return Statistics.of( 100d, ImmutableList.of(), ImmutableList.of( COLLATION ) );
                }
            };

            final RelOptAbstractTable t1 = new RelOptAbstractTable( relOptSchema, "t1", table.getRowType( typeFactory ) ) {
                @Override
                public <T> T unwrap( Class<T> clazz ) {
                    return clazz.isInstance( table )
                            ? clazz.cast( table )
                            : super.unwrap( clazz );
                }
            };

            final RelNode rt1 = EnumerableTableScan.create( cluster, t1 );

            // project s column
            RelNode project = LogicalProject.create( rt1,
                    ImmutableList.of( (RexNode) rexBuilder.makeInputRef( stringType, 0 ), rexBuilder.makeInputRef( integerType, 1 ) ),
                    typeFactory.builder().add( "s", stringType ).add( "i", integerType ).build() );

            // aggregate on s, count
            AggregateCall aggCall = AggregateCall.create( SqlStdOperatorTable.COUNT, false, false, Collections.singletonList( 1 ), -1, RelCollations.EMPTY, sqlBigInt, "cnt" );
            RelNode agg = new LogicalAggregate( cluster, cluster.traitSetOf( Convention.NONE ), project, false, ImmutableBitSet.of( 0 ), null, Collections.singletonList( aggCall ) );

            final RelNode rootRel = agg;

            RelOptUtil.dumpPlan( "LOGICAL PLAN", rootRel, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES );

            RelTraitSet desiredTraits = rootRel.getTraitSet().replace( PHYSICAL );
            final RelNode rootRel2 = planner.changeTraits( rootRel, desiredTraits );
            planner.setRoot( rootRel2 );
            return planner.findBestExp();
        }
    }

    // RULES


    /**
     * Rule for PhysAgg
     */
    private static class PhysAggRule extends RelOptRule {

        static final PhysAggRule INSTANCE = new PhysAggRule();


        private PhysAggRule() {
            super( anyChild( LogicalAggregate.class ), "PhysAgg" );
        }


        public void onMatch( RelOptRuleCall call ) {
            RelTraitSet empty = call.getPlanner().emptyTraitSet();
            LogicalAggregate rel = call.rel( 0 );
            assert rel.getGroupSet().cardinality() == 1;
            int aggIndex = rel.getGroupSet().iterator().next();
            RelTrait collation = RelCollations.of( new RelFieldCollation( aggIndex, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.FIRST ) );
            RelTraitSet desiredTraits = empty.replace( PHYSICAL ).replace( collation );
            RelNode convertedInput = convert( rel.getInput(), desiredTraits );
            call.transformTo( new PhysAgg( rel.getCluster(), empty.replace( PHYSICAL ), convertedInput, rel.indicator, rel.getGroupSet(), rel.getGroupSets(), rel.getAggCallList() ) );
        }
    }


    /**
     * Rule for PhysProj
     */
    private static class PhysProjRule extends RelOptRule {

        static final PhysProjRule INSTANCE = new PhysProjRule( false );

        final boolean subsetHack;


        private PhysProjRule( boolean subsetHack ) {
            super( RelOptRule.operand( LogicalProject.class, anyChild( RelNode.class ) ), "PhysProj" );
            this.subsetHack = subsetHack;
        }


        public void onMatch( RelOptRuleCall call ) {
            LogicalProject rel = call.rel( 0 );
            RelNode rawInput = call.rel( 1 );
            RelNode input = convert( rawInput, PHYSICAL );

            if ( subsetHack && input instanceof RelSubset ) {
                RelSubset subset = (RelSubset) input;
                for ( RelNode child : subset.getRels() ) {
                    // skip logical nodes
                    if ( child.getTraitSet().getTrait( ConventionTraitDef.INSTANCE ) == Convention.NONE ) {
                        continue;
                    } else {
                        RelTraitSet outcome = child.getTraitSet().replace( PHYSICAL );
                        call.transformTo( new PhysProj( rel.getCluster(), outcome, convert( child, outcome ), rel.getChildExps(), rel.getRowType() ) );
                    }
                }
            } else {
                call.transformTo( PhysProj.create( input, rel.getChildExps(), rel.getRowType() ) );
            }
        }
    }


    /**
     * Rule for PhysSort
     */
    private static class PhysSortRule extends ConverterRule {

        static final PhysSortRule INSTANCE = new PhysSortRule();


        PhysSortRule() {
            super( Sort.class, Convention.NONE, PHYSICAL, "PhysSortRule" );
        }


        public RelNode convert( RelNode rel ) {
            final Sort sort = (Sort) rel;
            final RelNode input = convert( sort.getInput(), rel.getCluster().traitSetOf( PHYSICAL ) );
            return new PhysSort( rel.getCluster(), input.getTraitSet().plus( sort.getCollation() ), convert( input, input.getTraitSet().replace( PHYSICAL ) ), sort.getCollation(), null, null );
        }
    }


    /**
     * Rule for PhysTable
     */
    private static class PhysTableRule extends RelOptRule {

        static final PhysTableRule INSTANCE = new PhysTableRule();


        private PhysTableRule() {
            super( anyChild( EnumerableTableScan.class ), "PhysScan" );
        }


        public void onMatch( RelOptRuleCall call ) {
            EnumerableTableScan rel = call.rel( 0 );
            call.transformTo( new PhysTable( rel.getCluster() ) );
        }
    }

    /* RELS */


    /**
     * Market interface for Phys nodes
     */
    private interface Phys extends RelNode {

    }


    /**
     * Physical Aggregate RelNode
     */
    private static class PhysAgg extends Aggregate implements Phys {

        PhysAgg( RelOptCluster cluster, RelTraitSet traits, RelNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
            super( cluster, traits, child, indicator, groupSet, groupSets, aggCalls );
        }


        public Aggregate copy( RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
            return new PhysAgg( getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls );
        }


        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return planner.getCostFactory().makeCost( 1, 1, 1 );
        }
    }


    /**
     * Physical Project RelNode
     */
    private static class PhysProj extends Project implements Phys {

        PhysProj( RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexNode> exps, RelDataType rowType ) {
            super( cluster, traits, child, exps, rowType );
        }


        public static PhysProj create( final RelNode input, final List<RexNode> projects, RelDataType rowType ) {
            final RelOptCluster cluster = input.getCluster();
            final RelMetadataQuery mq = RelMetadataQuery.instance();
            final RelTraitSet traitSet = cluster.traitSet().replace( PHYSICAL )
                    .replaceIfs(
                            RelCollationTraitDef.INSTANCE,
                            () -> RelMdCollation.project( mq, input, projects ) );
            return new PhysProj( cluster, traitSet, input, projects, rowType );
        }


        public PhysProj copy( RelTraitSet traitSet, RelNode input, List<RexNode> exps, RelDataType rowType ) {
            return new PhysProj( getCluster(), traitSet, input, exps, rowType );
        }


        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return planner.getCostFactory().makeCost( 1, 1, 1 );
        }
    }


    /**
     * Physical Sort RelNode
     */
    private static class PhysSort extends Sort implements Phys {

        PhysSort( RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch ) {
            super( cluster, traits, child, collation, offset, fetch );
        }


        public PhysSort copy( RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch ) {
            return new PhysSort( getCluster(), traitSet, newInput, newCollation, offset, fetch );
        }


        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return planner.getCostFactory().makeCost( 1, 1, 1 );
        }
    }


    /**
     * Physical Table RelNode
     */
    private static class PhysTable extends AbstractRelNode implements Phys {

        PhysTable( RelOptCluster cluster ) {
            super( cluster, cluster.traitSet().replace( PHYSICAL ).replace( COLLATION ) );
            RelDataTypeFactory typeFactory = cluster.getTypeFactory();
            final RelDataType stringType = typeFactory.createJavaType( String.class );
            final RelDataType integerType = typeFactory.createJavaType( Integer.class );
            this.rowType = typeFactory.builder().add( "s", stringType ).add( "i", integerType ).build();
        }


        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return planner.getCostFactory().makeCost( 1, 1, 1 );
        }
    }


    /* UTILS */
    public static RelOptRuleOperand anyChild( Class<? extends RelNode> first ) {
        return RelOptRule.operand( first, RelOptRule.any() );
    }


    // Created so that we can control when the TraitDefs are defined (e.g. before the cluster is created).
    private static RelNode run( PropAction action, RuleSet rules ) throws Exception {

        FrameworkConfig config = Frameworks.newConfigBuilder().ruleSets( rules ).build();

        final Properties info = new Properties();
        final Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info );
        final PolyphenyDbServerStatement statement = connection.createStatement().unwrap( PolyphenyDbServerStatement.class );
        final Context prepareContext = statement.createPrepareContext();
        final JavaTypeFactory typeFactory = prepareContext.getTypeFactory();
        PolyphenyDbCatalogReader catalogReader = new PolyphenyDbCatalogReader( prepareContext.getRootSchema(), prepareContext.getDefaultSchemaPath(), typeFactory );
        final RexBuilder rexBuilder = new RexBuilder( typeFactory );
        final RelOptPlanner planner = new VolcanoPlanner( config.getCostFactory(), config.getContext() );

        // set up rules before we generate cluster
        planner.clearRelTraitDefs();
        planner.addRelTraitDef( RelCollationTraitDef.INSTANCE );
        planner.addRelTraitDef( ConventionTraitDef.INSTANCE );

        planner.clear();
        for ( RelOptRule r : rules ) {
            planner.addRule( r );
        }

        final RelOptCluster cluster = RelOptCluster.create( planner, rexBuilder );
        return action.apply( cluster, catalogReader, prepareContext.getRootSchema().plus() );
    }
}


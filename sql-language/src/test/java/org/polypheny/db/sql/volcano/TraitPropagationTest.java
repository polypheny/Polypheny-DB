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
 */

package org.polypheny.db.sql.volcano;


import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.Ignore;
import org.junit.Test;
import org.polypheny.db.adapter.enumerable.EnumerableScan;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.logical.relational.LogicalAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.rules.SortRemoveRule;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.avatica.PolyphenyDbServerStatement;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptAbstractTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.plan.volcano.AbstractConverter.ExpandConversionRule;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Statistic;
import org.polypheny.db.schema.Statistics;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.AbstractTable;
import org.polypheny.db.tools.FrameworkConfig;
import org.polypheny.db.tools.Frameworks;
import org.polypheny.db.tools.RuleSet;
import org.polypheny.db.tools.RuleSets;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Tests that determine whether trait propagation work in Volcano Planner.
 */
@Ignore // TODO MV fix
public class TraitPropagationTest {

    static final Convention PHYSICAL = new Convention.Impl( "PHYSICAL", Phys.class );
    static final AlgCollation COLLATION = AlgCollations.of( new AlgFieldCollation( 0, AlgFieldCollation.Direction.ASCENDING, AlgFieldCollation.NullDirection.FIRST ) );

    static final RuleSet RULES = RuleSets.ofList( PhysAggRule.INSTANCE, PhysProjRule.INSTANCE, PhysTableRule.INSTANCE, PhysSortRule.INSTANCE, SortRemoveRule.INSTANCE, ExpandConversionRule.INSTANCE );


    @Test
    public void testOne() throws Exception {
        AlgNode planned = run( new PropAction(), RULES );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            System.out.println( AlgOptUtil.dumpPlan( "LOGICAL PLAN", planned, ExplainFormat.TEXT, ExplainLevel.ALL_ATTRIBUTES ) );
        }
        final AlgMetadataQuery mq = AlgMetadataQuery.instance();
        assertEquals( "Sortedness was not propagated", 3, mq.getCumulativeCost( planned ).getRows(), 0 );
    }


    /**
     * Materialized anonymous class for simplicity
     */
    private static class PropAction {

        public AlgNode apply( AlgOptCluster cluster, AlgOptSchema algOptSchema, SchemaPlus rootSchema ) {
            final AlgDataTypeFactory typeFactory = cluster.getTypeFactory();
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            final AlgOptPlanner planner = cluster.getPlanner();

            final AlgDataType stringType = typeFactory.createJavaType( String.class );
            final AlgDataType integerType = typeFactory.createJavaType( Integer.class );
            final AlgDataType sqlBigInt = typeFactory.createPolyType( PolyType.BIGINT );

            // SELECT * from T;
            final Table table = new AbstractTable() {
                @Override
                public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
                    return typeFactory.builder()
                            .add( "s", null, stringType )
                            .add( "i", null, integerType ).build();
                }


                @Override
                public Statistic getStatistic() {
                    return Statistics.of( 100d, ImmutableList.of(), ImmutableList.of( COLLATION ) );
                }
            };

            final AlgOptAbstractTable t1 = new AlgOptAbstractTable( algOptSchema, "t1", table.getRowType( typeFactory ) ) {
                @Override
                public <T> T unwrap( Class<T> clazz ) {
                    return clazz.isInstance( table )
                            ? clazz.cast( table )
                            : super.unwrap( clazz );
                }
            };

            final AlgNode rt1 = EnumerableScan.create( cluster, t1 );

            // project s column
            AlgNode project = LogicalProject.create(
                    rt1,
                    ImmutableList.of( (RexNode) rexBuilder.makeInputRef( stringType, 0 ), rexBuilder.makeInputRef( integerType, 1 ) ),
                    typeFactory.builder().add( "s", null, stringType ).add( "i", null, integerType ).build() );

            // aggregate on s, count
            AggregateCall aggCall = AggregateCall.create( OperatorRegistry.getAgg( OperatorName.COUNT ), false, false, Collections.singletonList( 1 ), -1, AlgCollations.EMPTY, sqlBigInt, "cnt" );
            AlgNode agg = new LogicalAggregate( cluster, cluster.traitSetOf( Convention.NONE ), project, false, ImmutableBitSet.of( 0 ), null, Collections.singletonList( aggCall ) );

            final AlgNode rootRel = agg;

            AlgOptUtil.dumpPlan( "LOGICAL PLAN", rootRel, ExplainFormat.TEXT, ExplainLevel.DIGEST_ATTRIBUTES );

            AlgTraitSet desiredTraits = rootRel.getTraitSet().replace( PHYSICAL );
            final AlgNode rootRel2 = planner.changeTraits( rootRel, desiredTraits );
            planner.setRoot( rootRel2 );
            return planner.findBestExp();
        }

    }

    // RULES


    /**
     * Rule for PhysAgg
     */
    private static class PhysAggRule extends AlgOptRule {

        static final PhysAggRule INSTANCE = new PhysAggRule();


        private PhysAggRule() {
            super( anyChild( LogicalAggregate.class ), "PhysAgg" );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            AlgTraitSet empty = call.getPlanner().emptyTraitSet();
            LogicalAggregate alg = call.alg( 0 );
            assert alg.getGroupSet().cardinality() == 1;
            int aggIndex = alg.getGroupSet().iterator().next();
            AlgTrait collation = AlgCollations.of( new AlgFieldCollation( aggIndex, AlgFieldCollation.Direction.ASCENDING, AlgFieldCollation.NullDirection.FIRST ) );
            AlgTraitSet desiredTraits = empty.replace( PHYSICAL ).replace( collation );
            AlgNode convertedInput = convert( alg.getInput(), desiredTraits );
            call.transformTo( new PhysAgg( alg.getCluster(), empty.replace( PHYSICAL ), convertedInput, alg.indicator, alg.getGroupSet(), alg.getGroupSets(), alg.getAggCallList() ) );
        }

    }


    /**
     * Rule for PhysProj
     */
    private static class PhysProjRule extends AlgOptRule {

        static final PhysProjRule INSTANCE = new PhysProjRule( false );

        final boolean subsetHack;


        private PhysProjRule( boolean subsetHack ) {
            super( AlgOptRule.operand( LogicalProject.class, anyChild( AlgNode.class ) ), "PhysProj" );
            this.subsetHack = subsetHack;
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            LogicalProject alg = call.alg( 0 );
            AlgNode rawInput = call.alg( 1 );
            AlgNode input = convert( rawInput, PHYSICAL );

            if ( subsetHack && input instanceof AlgSubset ) {
                AlgSubset subset = (AlgSubset) input;
                for ( AlgNode child : subset.getAlgs() ) {
                    // skip logical nodes
                    if ( child.getTraitSet().getTrait( ConventionTraitDef.INSTANCE ) == Convention.NONE ) {
                        continue;
                    } else {
                        AlgTraitSet outcome = child.getTraitSet().replace( PHYSICAL );
                        call.transformTo( new PhysProj( alg.getCluster(), outcome, convert( child, outcome ), alg.getChildExps(), alg.getRowType() ) );
                    }
                }
            } else {
                call.transformTo( PhysProj.create( input, alg.getChildExps(), alg.getRowType() ) );
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


        @Override
        public AlgNode convert( AlgNode alg ) {
            final Sort sort = (Sort) alg;
            final AlgNode input = convert( sort.getInput(), alg.getCluster().traitSetOf( PHYSICAL ) );
            return new PhysSort( alg.getCluster(), input.getTraitSet().plus( sort.getCollation() ), convert( input, input.getTraitSet().replace( PHYSICAL ) ), sort.getCollation(), null, null );
        }

    }


    /**
     * Rule for PhysTable
     */
    private static class PhysTableRule extends AlgOptRule {

        static final PhysTableRule INSTANCE = new PhysTableRule();


        private PhysTableRule() {
            super( anyChild( EnumerableScan.class ), "PhysScan" );
        }


        @Override
        public void onMatch( AlgOptRuleCall call ) {
            EnumerableScan alg = call.alg( 0 );
            call.transformTo( new PhysTable( alg.getCluster() ) );
        }

    }

    /* RELS */


    /**
     * Market interface for Phys nodes
     */
    private interface Phys extends AlgNode {

    }


    /**
     * Physical Aggregate AlgNode
     */
    private static class PhysAgg extends Aggregate implements Phys {

        PhysAgg( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
            super( cluster, traits, child, indicator, groupSet, groupSets, aggCalls );
        }


        @Override
        public Aggregate copy( AlgTraitSet traitSet, AlgNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
            return new PhysAgg( getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeCost( 1, 1, 1 );
        }

    }


    /**
     * Physical Project AlgNode
     */
    private static class PhysProj extends Project implements Phys {

        PhysProj( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, List<RexNode> exps, AlgDataType rowType ) {
            super( cluster, traits, child, exps, rowType );
        }


        public static PhysProj create( final AlgNode input, final List<RexNode> projects, AlgDataType rowType ) {
            final AlgOptCluster cluster = input.getCluster();
            final AlgMetadataQuery mq = AlgMetadataQuery.instance();
            final AlgTraitSet traitSet = cluster.traitSet().replace( PHYSICAL )
                    .replaceIfs(
                            AlgCollationTraitDef.INSTANCE,
                            () -> AlgMdCollation.project( mq, input, projects ) );
            return new PhysProj( cluster, traitSet, input, projects, rowType );
        }


        @Override
        public PhysProj copy( AlgTraitSet traitSet, AlgNode input, List<RexNode> exps, AlgDataType rowType ) {
            return new PhysProj( getCluster(), traitSet, input, exps, rowType );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeCost( 1, 1, 1 );
        }

    }


    /**
     * Physical Sort AlgNode
     */
    private static class PhysSort extends Sort implements Phys {

        PhysSort( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, AlgCollation collation, RexNode offset, RexNode fetch ) {
            super( cluster, traits, child, collation, offset, fetch );
        }


        @Override
        public PhysSort copy( AlgTraitSet traitSet, AlgNode newInput, AlgCollation newCollation, RexNode offset, RexNode fetch ) {
            return new PhysSort( getCluster(), traitSet, newInput, newCollation, offset, fetch );
        }


        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeCost( 1, 1, 1 );
        }

    }


    /**
     * Physical Table AlgNode
     */
    private static class PhysTable extends AbstractAlgNode implements Phys {

        PhysTable( AlgOptCluster cluster ) {
            super( cluster, cluster.traitSet().replace( PHYSICAL ).replace( COLLATION ) );
            AlgDataTypeFactory typeFactory = cluster.getTypeFactory();
            final AlgDataType stringType = typeFactory.createJavaType( String.class );
            final AlgDataType integerType = typeFactory.createJavaType( Integer.class );
            this.rowType = typeFactory.builder().add( "s", null, stringType ).add( "i", null, integerType ).build();
        }


        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            return planner.getCostFactory().makeCost( 1, 1, 1 );
        }


        @Override
        public String algCompareString() {
            // Compare makes no sense here. Use hashCode() to avoid errors.
            return this.getClass().getSimpleName() + "$" + hashCode() + "&";
        }

    }


    /* UTILS */
    public static AlgOptRuleOperand anyChild( Class<? extends AlgNode> first ) {
        return AlgOptRule.operand( first, AlgOptRule.any() );
    }


    // Created so that we can control when the TraitDefs are defined (e.g. before the cluster is created).
    private static AlgNode run( PropAction action, RuleSet rules ) throws Exception {

        FrameworkConfig config = Frameworks.newConfigBuilder().ruleSets( rules ).build();

        final Properties info = new Properties();
        final Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", info );
        final PolyphenyDbServerStatement statement = connection.createStatement().unwrap( PolyphenyDbServerStatement.class );
        final Context prepareContext = statement.createPrepareContext();
        final JavaTypeFactory typeFactory = prepareContext.getTypeFactory();
        PolyphenyDbCatalogReader catalogReader = new PolyphenyDbCatalogReader( prepareContext.getRootSchema(), prepareContext.getDefaultSchemaPath(), typeFactory );
        final RexBuilder rexBuilder = new RexBuilder( typeFactory );
        final AlgOptPlanner planner = new VolcanoPlanner( config.getCostFactory(), config.getContext() );

        // set up rules before we generate cluster
        planner.clearRelTraitDefs();
        planner.addAlgTraitDef( AlgCollationTraitDef.INSTANCE );
        planner.addAlgTraitDef( ConventionTraitDef.INSTANCE );

        planner.clear();
        for ( AlgOptRule r : rules ) {
            planner.addRule( r );
        }

        final AlgOptCluster cluster = AlgOptCluster.create( planner, rexBuilder );
        return action.apply( cluster, catalogReader, prepareContext.getRootSchema().plus() );
    }

}


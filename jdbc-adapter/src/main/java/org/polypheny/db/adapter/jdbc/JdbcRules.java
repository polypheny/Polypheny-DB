/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.jdbc;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.jdbc.rel2sql.SqlImplementor;
import org.polypheny.db.adapter.jdbc.rel2sql.SqlImplementor.Result;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTrait;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.prepare.Prepare;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.InvalidRelException;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelWriter;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rel.convert.ConverterRule;
import org.polypheny.db.rel.core.Aggregate;
import org.polypheny.db.rel.core.AggregateCall;
import org.polypheny.db.rel.core.Calc;
import org.polypheny.db.rel.core.CorrelationId;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.core.Intersect;
import org.polypheny.db.rel.core.Join;
import org.polypheny.db.rel.core.JoinRelType;
import org.polypheny.db.rel.core.Minus;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.SemiJoin;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.Union;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.metadata.RelMdUtil;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexMultisetUtil;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexOver;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.sql.SqlAggFunction;
import org.polypheny.db.sql.SqlDialect;
import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.fun.SqlItemOperator;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Rules and relational operators for {@link JdbcConvention} calling convention.
 */
public class JdbcRules {

    private JdbcRules() {
    }


    protected static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();


    public static List<RelOptRule> rules( JdbcConvention out ) {
        return rules( out, RelFactories.LOGICAL_BUILDER );
    }


    public static List<RelOptRule> rules( JdbcConvention out, RelBuilderFactory relBuilderFactory ) {
        return ImmutableList.of(
                new JdbcToEnumerableConverterRule( out, relBuilderFactory ),
                new JdbcJoinRule( out, relBuilderFactory ),
                new JdbcCalcRule( out, relBuilderFactory ),
                new JdbcProjectRule( out, relBuilderFactory ),
                new JdbcFilterRule( out, relBuilderFactory ),
                new JdbcAggregateRule( out, relBuilderFactory ),
                new JdbcSortRule( out, relBuilderFactory ),
                new JdbcUnionRule( out, relBuilderFactory ),
                new JdbcIntersectRule( out, relBuilderFactory ),
                new JdbcMinusRule( out, relBuilderFactory ),
                new JdbcTableModificationRule( out, relBuilderFactory ),
                new JdbcValuesRule( out, relBuilderFactory ) );
    }


    /**
     * Abstract base class for rule that converts to JDBC.
     */
    abstract static class JdbcConverterRule extends ConverterRule {

        protected final JdbcConvention out;


        <R extends RelNode> JdbcConverterRule(
                Class<R> clazz,
                Predicate<? super R> predicate,
                RelTrait in,
                JdbcConvention out,
                RelBuilderFactory relBuilderFactory,
                String description ) {
            super( clazz, predicate, in, out, relBuilderFactory, description );
            this.out = out;
        }

    }


    /**
     * Rule that converts a join to JDBC.
     */
    public static class JdbcJoinRule extends JdbcConverterRule {


        /**
         * Creates a JdbcJoinRule.
         */
        public JdbcJoinRule( JdbcConvention out, RelBuilderFactory relBuilderFactory ) {
            super(
                    Join.class,
                    (Predicate<RelNode>) r -> true,
                    Convention.NONE,
                    out,
                    relBuilderFactory,
                    "JdbcJoinRule." + out );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            if ( rel instanceof SemiJoin ) {
                // It's not possible to convert semi-joins. They have fewer columns than regular joins.
                return null;
            }
            return convert( (Join) rel, true );
        }


        /**
         * Converts a {@code Join} into a {@code JdbcJoin}.
         *
         * @param join Join operator to convert
         * @param convertInputTraits Whether to convert input to {@code join}'s JDBC convention
         * @return A new JdbcJoin
         */
        public RelNode convert( Join join, boolean convertInputTraits ) {
            final List<RelNode> newInputs = new ArrayList<>();
            for ( RelNode input : join.getInputs() ) {
                if ( convertInputTraits && input.getConvention() != getOutTrait() ) {
                    input = convert( input, input.getTraitSet().replace( out ) );
                }
                newInputs.add( input );
            }
            if ( convertInputTraits && !canJoinOnCondition( join.getCondition() ) ) {
                return null;
            }
            try {
                return new JdbcJoin(
                        join.getCluster(),
                        join.getTraitSet().replace( out ),
                        newInputs.get( 0 ),
                        newInputs.get( 1 ),
                        join.getCondition(),
                        join.getVariablesSet(),
                        join.getJoinType() );
            } catch ( InvalidRelException e ) {
                LOGGER.debug( e.toString() );
                return null;
            }
        }


        /**
         * Returns whether a condition is supported by {@link JdbcJoin}.
         *
         * Corresponds to the capabilities of {@link SqlImplementor#convertConditionToSqlNode}.
         *
         * @param node Condition
         * @return Whether condition is supported
         */
        private boolean canJoinOnCondition( RexNode node ) {
            final List<RexNode> operands;
            switch ( node.getKind() ) {
                case AND:
                case OR:
                    operands = ((RexCall) node).getOperands();
                    for ( RexNode operand : operands ) {
                        if ( !canJoinOnCondition( operand ) ) {
                            return false;
                        }
                    }
                    return true;

                case EQUALS:
                case IS_NOT_DISTINCT_FROM:
                case NOT_EQUALS:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                    operands = ((RexCall) node).getOperands();
                    if ( (operands.get( 0 ) instanceof RexInputRef) && (operands.get( 1 ) instanceof RexInputRef) ) {
                        return true;
                    }
                    // fall through

                default:
                    return false;
            }
        }

    }


    /**
     * Join operator implemented in JDBC convention.
     */
    public static class JdbcJoin extends Join implements JdbcRel {

        /**
         * Creates a JdbcJoin.
         */
        public JdbcJoin(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelNode left,
                RelNode right,
                RexNode condition,
                Set<CorrelationId> variablesSet,
                JoinRelType joinType ) throws InvalidRelException {
            super( cluster, traitSet, left, right, condition, variablesSet, joinType );
        }


        @Override
        public JdbcJoin copy( RelTraitSet traitSet, RexNode condition, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone ) {
            try {
                return new JdbcJoin( getCluster(), traitSet, left, right, condition, variablesSet, joinType );
            } catch ( InvalidRelException e ) {
                // Semantic error not possible. Must be a bug. Convert to internal error.
                throw new AssertionError( e );
            }
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            // We always "build" the
            double rowCount = mq.getRowCount( this );

            return planner.getCostFactory().makeCost( rowCount, 0, 0 );
        }


        @Override
        public double estimateRowCount( RelMetadataQuery mq ) {
            final double leftRowCount = left.estimateRowCount( mq );
            final double rightRowCount = right.estimateRowCount( mq );
            return Math.max( leftRowCount, rightRowCount );
        }


        @Override
        public Result implement( JdbcImplementor implementor ) {
            return implementor.implement( this );
        }

    }


    /**
     * Rule to convert a {@link Calc} to an {@link org.polypheny.db.adapter.jdbc.JdbcRules.JdbcCalc}.
     */
    private static class JdbcCalcRule extends JdbcConverterRule {

        /**
         * Creates a JdbcCalcRule.
         */
        private JdbcCalcRule( JdbcConvention out, RelBuilderFactory relBuilderFactory ) {
            super( Calc.class, (Predicate<RelNode>) r -> true, Convention.NONE, out, relBuilderFactory, "JdbcCalcRule." + out );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final Calc calc = (Calc) rel;

            // If there's a multiset, let FarragoMultisetSplitter work on it first.
            if ( RexMultisetUtil.containsMultiset( calc.getProgram() ) ) {
                return null;
            }

            return new JdbcCalc(
                    rel.getCluster(),
                    rel.getTraitSet().replace( out ),
                    convert( calc.getInput(), calc.getTraitSet().replace( out ) ),
                    calc.getProgram() );
        }

    }


    /**
     * Calc operator implemented in JDBC convention.
     *
     * @see Calc
     */
    public static class JdbcCalc extends SingleRel implements JdbcRel {

        private final RexProgram program;


        public JdbcCalc( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexProgram program ) {
            super( cluster, traitSet, input );
            assert getConvention() instanceof JdbcConvention;
            this.program = program;
            this.rowType = program.getOutputRowType();
        }


        @Override
        public RelWriter explainTerms( RelWriter pw ) {
            return program.explainCalc( super.explainTerms( pw ) );
        }


        @Override
        public double estimateRowCount( RelMetadataQuery mq ) {
            return RelMdUtil.estimateFilteredRows( getInput(), program, mq );
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            double dRows = mq.getRowCount( this );
            double dCpu = mq.getRowCount( getInput() ) * program.getExprCount();
            double dIo = 0;
            return planner.getCostFactory().makeCost( dRows, dCpu, dIo );
        }


        @Override
        public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
            return new JdbcCalc( getCluster(), traitSet, sole( inputs ), program );
        }


        @Override
        public Result implement( JdbcImplementor implementor ) {
            return implementor.implement( this );
        }


        @Override
        public String relCompareString() {
            return this.getClass().getSimpleName() + "$" +
                    input.relCompareString() + "$" +
                    (program != null ? program.toString() : "") + "&";
        }

    }


    /**
     * Rule to convert a {@link Project} to an {@link org.polypheny.db.adapter.jdbc.JdbcRules.JdbcProject}.
     */
    public static class JdbcProjectRule extends JdbcConverterRule {

        /**
         * Creates a JdbcProjectRule.
         */
        public JdbcProjectRule( final JdbcConvention out, RelBuilderFactory relBuilderFactory ) {
            super( Project.class, (Predicate<Project>) project ->
                            (out.dialect.supportsWindowFunctions()
                                    || !RexOver.containsOver( project.getProjects(), null ))
                                    && !userDefinedFunctionInProject( project )
                                    && !knnFunctionInProject( project )
                                    && !multimediaFunctionInProject( project )
                                    && (out.dialect.supportsNestedArrays() || !itemOperatorInProject( project )),
                    Convention.NONE, out, relBuilderFactory, "JdbcProjectRule." + out );
        }


        private static boolean userDefinedFunctionInProject( Project project ) {
            CheckingUserDefinedFunctionVisitor visitor = new CheckingUserDefinedFunctionVisitor();
            for ( RexNode node : project.getChildExps() ) {
                node.accept( visitor );
                if ( visitor.containsUserDefinedFunction() ) {
                    return true;
                }
            }
            return false;
        }


        // TODO js(knn): Make sure this is not just a hotfix.
        private static boolean knnFunctionInProject( Project project ) {
            CheckingKnnFunctionVisitor visitor = new CheckingKnnFunctionVisitor();
            for ( RexNode node : project.getChildExps() ) {
                node.accept( visitor );
                if ( visitor.containsKnnFunction() ) {
                    return true;
                }
            }
            return false;
        }


        private static boolean multimediaFunctionInProject( Project project ) {
            CheckingMultimediaFunctionVisitor visitor = new CheckingMultimediaFunctionVisitor();
            for ( RexNode node : project.getChildExps() ) {
                node.accept( visitor );
                if ( visitor.containsMultimediaFunction() ) {
                    return true;
                }
            }
            return false;
        }


        private static boolean itemOperatorInProject( Project project ) {
            CheckingItemOperatorVisitor visitor = new CheckingItemOperatorVisitor();
            for ( RexNode node : project.getChildExps() ) {
                node.accept( visitor );
                if ( visitor.containsItemOperator() ) {
                    return true;
                }
            }
            return false;
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final Project project = (Project) rel;

            return new JdbcProject(
                    rel.getCluster(),
                    rel.getTraitSet().replace( out ),
                    convert( project.getInput(), project.getInput().getTraitSet().replace( out ) ),
                    project.getProjects(),
                    project.getRowType() );
        }

    }


    /**
     * Implementation of {@link Project} in {@link JdbcConvention jdbc calling convention}.
     */
    public static class JdbcProject extends Project implements JdbcRel {

        public JdbcProject( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<? extends RexNode> projects, RelDataType rowType ) {
            super( cluster, traitSet, input, projects, rowType );
            assert getConvention() instanceof JdbcConvention;
        }


        @Override
        public JdbcProject copy( RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType ) {
            return new JdbcProject( getCluster(), traitSet, input, projects, rowType );
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return super.computeSelfCost( planner, mq ).multiplyBy( JdbcConvention.COST_MULTIPLIER );
        }


        @Override
        public Result implement( JdbcImplementor implementor ) {
            return implementor.implement( this );
        }

    }


    /**
     * Rule to convert a {@link Filter} to an {@link org.polypheny.db.adapter.jdbc.JdbcRules.JdbcFilter}.
     */
    public static class JdbcFilterRule extends JdbcConverterRule {


        /**
         * Creates a JdbcFilterRule.
         */
        public JdbcFilterRule( JdbcConvention out, RelBuilderFactory relBuilderFactory ) {
            super( Filter.class,
                    (Predicate<Filter>) filter -> (
                            !userDefinedFunctionInFilter( filter )
                                    && !knnFunctionInFilter( filter )
                                    && !multimediaFunctionInFilter( filter )
                                    && (out.dialect.supportsNestedArrays() || !itemOperatorInFilter( filter ))),
                    Convention.NONE, out, relBuilderFactory, "JdbcFilterRule." + out );
        }


        private static boolean userDefinedFunctionInFilter( Filter filter ) {
            CheckingUserDefinedFunctionVisitor visitor = new CheckingUserDefinedFunctionVisitor();
            filter.getCondition().accept( visitor );
            return visitor.containsUserDefinedFunction();
        }


        private static boolean knnFunctionInFilter( Filter filter ) {
            CheckingKnnFunctionVisitor visitor = new CheckingKnnFunctionVisitor();
            for ( RexNode node : filter.getChildExps() ) {
                node.accept( visitor );
                if ( visitor.containsKnnFunction() ) {
                    return true;
                }
            }
            return false;
        }


        private static boolean multimediaFunctionInFilter( Filter filter ) {
            CheckingMultimediaFunctionVisitor visitor = new CheckingMultimediaFunctionVisitor();
            for ( RexNode node : filter.getChildExps() ) {
                node.accept( visitor );
                if ( visitor.containsMultimediaFunction() ) {
                    return true;
                }
            }
            return false;
        }


        private static boolean itemOperatorInFilter( Filter filter ) {
            CheckingItemOperatorVisitor visitor = new CheckingItemOperatorVisitor();
            for ( RexNode node : filter.getChildExps() ) {
                node.accept( visitor );
                if ( visitor.containsItemOperator() ) {
                    return true;
                }
            }
            return false;
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final Filter filter = (Filter) rel;

            return new JdbcFilter(
                    rel.getCluster(),
                    rel.getTraitSet().replace( out ),
                    convert( filter.getInput(), filter.getInput().getTraitSet().replace( out ) ),
                    filter.getCondition() );
        }

    }


    /**
     * Implementation of {@link Filter} in {@link JdbcConvention jdbc calling convention}.
     */
    public static class JdbcFilter extends Filter implements JdbcRel {

        public JdbcFilter( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode condition ) {
            super( cluster, traitSet, input, condition );
            assert getConvention() instanceof JdbcConvention;
        }


        @Override
        public JdbcFilter copy( RelTraitSet traitSet, RelNode input, RexNode condition ) {
            return new JdbcFilter( getCluster(), traitSet, input, condition );
        }


        @Override
        public Result implement( JdbcImplementor implementor ) {
            return implementor.implement( this );
        }

    }


    /**
     * Rule to convert a {@link org.polypheny.db.rel.core.Aggregate} to a
     * {@link org.polypheny.db.adapter.jdbc.JdbcRules.JdbcAggregate}.
     */
    public static class JdbcAggregateRule extends JdbcConverterRule {

        /**
         * Creates a JdbcAggregateRule.
         */
        public JdbcAggregateRule( JdbcConvention out, RelBuilderFactory relBuilderFactory ) {
            super( Aggregate.class, (Predicate<RelNode>) r -> true, Convention.NONE, out, relBuilderFactory, "JdbcAggregateRule." + out );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final Aggregate agg = (Aggregate) rel;
            if ( agg.getGroupSets().size() != 1 ) {
                // GROUPING SETS not supported; see
                // [POLYPHENYDB-734] Push GROUPING SETS to underlying SQL via JDBC adapter
                return null;
            }
            final RelTraitSet traitSet = agg.getTraitSet().replace( out );
            try {
                return new JdbcAggregate(
                        rel.getCluster(),
                        traitSet,
                        convert( agg.getInput(), out ),
                        agg.indicator,
                        agg.getGroupSet(),
                        agg.getGroupSets(),
                        agg.getAggCallList() );
            } catch ( InvalidRelException e ) {
                LOGGER.debug( e.toString() );
                return null;
            }
        }

    }


    /**
     * Returns whether this JDBC data source can implement a given aggregate function.
     */
    private static boolean canImplement( SqlAggFunction aggregation, SqlDialect sqlDialect ) {
        return sqlDialect.supportsAggregateFunction( aggregation.getKind() );
    }


    /**
     * Aggregate operator implemented in JDBC convention.
     */
    public static class JdbcAggregate extends Aggregate implements JdbcRel {

        public JdbcAggregate(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelNode input,
                boolean indicator,
                ImmutableBitSet groupSet,
                List<ImmutableBitSet> groupSets,
                List<AggregateCall> aggCalls )
                throws InvalidRelException {
            super( cluster, traitSet, input, indicator, groupSet, groupSets, aggCalls );
            assert getConvention() instanceof JdbcConvention;
            assert this.groupSets.size() == 1 : "Grouping sets not supported";
            assert !this.indicator;
            final SqlDialect dialect = ((JdbcConvention) getConvention()).dialect;
            for ( AggregateCall aggCall : aggCalls ) {
                if ( !canImplement( aggCall.getAggregation(), dialect ) ) {
                    throw new InvalidRelException( "cannot implement aggregate function " + aggCall.getAggregation() );
                }
            }
        }


        @Override
        public JdbcAggregate copy(
                RelTraitSet traitSet,
                RelNode input,
                boolean indicator,
                ImmutableBitSet groupSet,
                List<ImmutableBitSet> groupSets,
                List<AggregateCall> aggCalls ) {
            try {
                return new JdbcAggregate( getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls );
            } catch ( InvalidRelException e ) {
                // Semantic error not possible. Must be a bug. Convert to internal error.
                throw new AssertionError( e );
            }
        }


        @Override
        public Result implement( JdbcImplementor implementor ) {
            return implementor.implement( this );
        }

    }


    /**
     * Rule to convert a {@link Sort} to an {@link org.polypheny.db.adapter.jdbc.JdbcRules.JdbcSort}.
     */
    public static class JdbcSortRule extends JdbcConverterRule {

        /**
         * Creates a JdbcSortRule.
         */
        public JdbcSortRule( JdbcConvention out, RelBuilderFactory relBuilderFactory ) {
            super( Sort.class, (Predicate<RelNode>) r -> true, Convention.NONE, out, relBuilderFactory, "JdbcSortRule." + out );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            return convert( (Sort) rel, true );
        }


        /**
         * Converts a {@code Sort} into a {@code JdbcSort}.
         *
         * @param sort Sort operator to convert
         * @param convertInputTraits Whether to convert input to {@code sort}'s JDBC convention
         * @return A new JdbcSort
         */
        public RelNode convert( Sort sort, boolean convertInputTraits ) {
            final RelTraitSet traitSet = sort.getTraitSet().replace( out );

            final RelNode input;
            if ( convertInputTraits ) {
                final RelTraitSet inputTraitSet = sort.getInput().getTraitSet().replace( out );
                input = convert( sort.getInput(), inputTraitSet );
            } else {
                input = sort.getInput();
            }

            return new JdbcSort( sort.getCluster(), traitSet, input, sort.getCollation(), sort.offset, sort.fetch );
        }

    }


    /**
     * Sort operator implemented in JDBC convention.
     */
    public static class JdbcSort extends Sort implements JdbcRel {

        public JdbcSort( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch ) {
            super( cluster, traitSet, input, collation, offset, fetch );
            assert getConvention() instanceof JdbcConvention;
            assert getConvention() == input.getConvention();
        }


        @Override
        public JdbcSort copy( RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch ) {
            return new JdbcSort( getCluster(), traitSet, newInput, newCollation, offset, fetch );
        }


        @Override
        public Result implement( JdbcImplementor implementor ) {
            return implementor.implement( this );
        }

    }


    /**
     * Rule to convert an {@link org.polypheny.db.rel.core.Union} to a
     * {@link org.polypheny.db.adapter.jdbc.JdbcRules.JdbcUnion}.
     */
    public static class JdbcUnionRule extends JdbcConverterRule {

        /**
         * Creates a JdbcUnionRule.
         */
        public JdbcUnionRule( JdbcConvention out, RelBuilderFactory relBuilderFactory ) {
            super( Union.class, (Predicate<RelNode>) r -> true, Convention.NONE, out, relBuilderFactory, "JdbcUnionRule." + out );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final Union union = (Union) rel;
            final RelTraitSet traitSet = union.getTraitSet().replace( out );
            return new JdbcUnion( rel.getCluster(), traitSet, RelOptRule.convertList( union.getInputs(), out ), union.all );
        }

    }


    /**
     * Union operator implemented in JDBC convention.
     */
    public static class JdbcUnion extends Union implements JdbcRel {

        public JdbcUnion( RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
            super( cluster, traitSet, inputs, all );
        }


        @Override
        public JdbcUnion copy( RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
            return new JdbcUnion( getCluster(), traitSet, inputs, all );
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
        }


        @Override
        public Result implement( JdbcImplementor implementor ) {
            return implementor.implement( this );
        }

    }


    /**
     * Rule to convert a {@link org.polypheny.db.rel.core.Intersect} to a
     * {@link org.polypheny.db.adapter.jdbc.JdbcRules.JdbcIntersect}.
     */
    public static class JdbcIntersectRule extends JdbcConverterRule {

        /**
         * Creates a JdbcIntersectRule.
         */
        private JdbcIntersectRule( JdbcConvention out, RelBuilderFactory relBuilderFactory ) {
            super( Intersect.class, (Predicate<RelNode>) r -> true, Convention.NONE, out, relBuilderFactory, "JdbcIntersectRule." + out );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final Intersect intersect = (Intersect) rel;
            if ( intersect.all ) {
                return null; // INTERSECT ALL not implemented
            }
            final RelTraitSet traitSet = intersect.getTraitSet().replace( out );
            return new JdbcIntersect( rel.getCluster(), traitSet, RelOptRule.convertList( intersect.getInputs(), out ), false );
        }

    }


    /**
     * Intersect operator implemented in JDBC convention.
     */
    public static class JdbcIntersect extends Intersect implements JdbcRel {

        public JdbcIntersect( RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
            super( cluster, traitSet, inputs, all );
            assert !all;
        }


        @Override
        public JdbcIntersect copy( RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
            return new JdbcIntersect( getCluster(), traitSet, inputs, all );
        }


        @Override
        public Result implement( JdbcImplementor implementor ) {
            return implementor.implement( this );
        }

    }


    /**
     * Rule to convert a {@link Minus} to a {@link org.polypheny.db.adapter.jdbc.JdbcRules.JdbcMinus}.
     */
    public static class JdbcMinusRule extends JdbcConverterRule {

        /**
         * Creates a JdbcMinusRule.
         */
        private JdbcMinusRule( JdbcConvention out, RelBuilderFactory relBuilderFactory ) {
            super( Minus.class, (Predicate<RelNode>) r -> true, Convention.NONE, out, relBuilderFactory, "JdbcMinusRule." + out );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final Minus minus = (Minus) rel;
            if ( minus.all ) {
                return null; // EXCEPT ALL not implemented
            }
            final RelTraitSet traitSet = rel.getTraitSet().replace( out );
            return new JdbcMinus( rel.getCluster(), traitSet, RelOptRule.convertList( minus.getInputs(), out ), false );
        }

    }


    /**
     * Minus operator implemented in JDBC convention.
     */
    public static class JdbcMinus extends Minus implements JdbcRel {

        public JdbcMinus( RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
            super( cluster, traitSet, inputs, all );
            assert !all;
        }


        @Override
        public JdbcMinus copy( RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
            return new JdbcMinus( getCluster(), traitSet, inputs, all );
        }


        @Override
        public Result implement( JdbcImplementor implementor ) {
            return implementor.implement( this );
        }

    }


    /**
     * Rule that converts a table-modification to JDBC.
     */
    public static class JdbcTableModificationRule extends JdbcConverterRule {

        /**
         * Creates a JdbcTableModificationRule.
         */
        private JdbcTableModificationRule( JdbcConvention out, RelBuilderFactory relBuilderFactory ) {
            super( TableModify.class, (Predicate<RelNode>) r -> true, Convention.NONE, out, relBuilderFactory, "JdbcTableModificationRule." + out );
        }


        @Override
        public boolean matches( RelOptRuleCall call ) {
            final TableModify tableModify = call.rel( 0 );
            if ( tableModify.getTable().unwrap( JdbcTable.class ) != null ) {
                JdbcTable table = tableModify.getTable().unwrap( JdbcTable.class );
                if ( out.getJdbcSchema() == table.getSchema() ) {
                    return true;
                }
            }
            return false;
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final TableModify modify = (TableModify) rel;
            final ModifiableTable modifiableTable = modify.getTable().unwrap( ModifiableTable.class );
            if ( modifiableTable == null ) {
                return null;
            }
            final RelTraitSet traitSet = modify.getTraitSet().replace( out );
            return new JdbcTableModify(
                    modify.getCluster(),
                    traitSet,
                    modify.getTable(),
                    modify.getCatalogReader(),
                    RelOptRule.convert( modify.getInput(), traitSet ),
                    modify.getOperation(),
                    modify.getUpdateColumnList(),
                    modify.getSourceExpressionList(),
                    modify.isFlattened() );
        }

    }


    /**
     * Table-modification operator implemented in JDBC convention.
     */
    public static class JdbcTableModify extends TableModify implements JdbcRel {

        private final Expression expression;


        public JdbcTableModify(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelOptTable table,
                Prepare.CatalogReader catalogReader,
                RelNode input,
                Operation operation,
                List<String> updateColumnList,
                List<RexNode> sourceExpressionList,
                boolean flattened ) {
            super( cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened );
            assert input.getConvention() instanceof JdbcConvention;
            assert getConvention() instanceof JdbcConvention;
            final ModifiableTable modifiableTable = table.unwrap( ModifiableTable.class );
            if ( modifiableTable == null ) {
                throw new AssertionError(); // TODO: user error in validator
            }
            this.expression = table.getExpression( Queryable.class );
            if ( expression == null ) {
                throw new AssertionError(); // TODO: user error in validator
            }
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
        }


        @Override
        public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
            return new JdbcTableModify(
                    getCluster(),
                    traitSet,
                    getTable(),
                    getCatalogReader(),
                    AbstractRelNode.sole( inputs ),
                    getOperation(),
                    getUpdateColumnList(),
                    getSourceExpressionList(),
                    isFlattened() );
        }


        @Override
        public Result implement( JdbcImplementor implementor ) {
            return implementor.implement( this );
        }

    }


    /**
     * Rule that converts a values operator to JDBC.
     */
    public static class JdbcValuesRule extends JdbcConverterRule {

        /**
         * Creates a JdbcValuesRule.
         */
        private JdbcValuesRule( JdbcConvention out, RelBuilderFactory relBuilderFactory ) {
            super( Values.class, (Predicate<RelNode>) r -> true, Convention.NONE, out, relBuilderFactory, "JdbcValuesRule." + out );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            Values values = (Values) rel;
            return new JdbcValues( values.getCluster(), values.getRowType(), values.getTuples(), values.getTraitSet().replace( out ) );
        }

    }


    /**
     * Values operator implemented in JDBC convention.
     */
    public static class JdbcValues extends Values implements JdbcRel {

        JdbcValues( RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet ) {
            super( cluster, rowType, tuples, traitSet );
        }


        @Override
        public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
            assert inputs.isEmpty();
            return new JdbcValues( getCluster(), rowType, tuples, traitSet );
        }


        @Override
        public Result implement( JdbcImplementor implementor ) {
            return implementor.implement( this );
        }

    }


    /**
     * Visitor for checking whether part of projection is a user defined function or not
     */
    private static class CheckingUserDefinedFunctionVisitor extends RexVisitorImpl<Void> {

        private boolean containsUsedDefinedFunction = false;


        CheckingUserDefinedFunctionVisitor() {
            super( true );
        }


        public boolean containsUserDefinedFunction() {
            return containsUsedDefinedFunction;
        }


        @Override
        public Void visitCall( RexCall call ) {
            SqlOperator operator = call.getOperator();
            if ( operator instanceof SqlFunction && ((SqlFunction) operator).getFunctionType().isUserDefined() ) {
                containsUsedDefinedFunction |= true;
            }
            return super.visitCall( call );
        }

    }


    private static class CheckingKnnFunctionVisitor extends RexVisitorImpl<Void> {

        private boolean containsKnnFunction = false;


        CheckingKnnFunctionVisitor() {
            super( true );
        }


        public boolean containsKnnFunction() {
            return containsKnnFunction;
        }


        @Override
        public Void visitCall( RexCall call ) {
            SqlOperator operator = call.getOperator();
            if ( operator instanceof SqlFunction && ((SqlFunction) operator).getFunctionType().isKnn() ) {
                containsKnnFunction |= true;
            }
            return super.visitCall( call );
        }

    }


    private static class CheckingMultimediaFunctionVisitor extends RexVisitorImpl<Void> {

        private boolean containsMultimediaFunction = false;


        CheckingMultimediaFunctionVisitor() {
            super( true );
        }


        public boolean containsMultimediaFunction() {
            return containsMultimediaFunction;
        }


        @Override
        public Void visitCall( RexCall call ) {
            SqlOperator operator = call.getOperator();
            if ( operator instanceof SqlFunction && ((SqlFunction) operator).getFunctionType().isMultimedia() ) {
                containsMultimediaFunction = true;
            }
            return super.visitCall( call );
        }

    }


    private static class CheckingItemOperatorVisitor extends RexVisitorImpl<Void> {

        private boolean containsItemOperator = false;


        CheckingItemOperatorVisitor() {
            super( true );
        }


        public boolean containsItemOperator() {
            return containsItemOperator;
        }


        @Override
        public Void visitCall( RexCall call ) {
            SqlOperator operator = call.getOperator();
            if ( operator instanceof SqlItemOperator ) {
                containsItemOperator |= true;
            }
            return super.visitCall( call );
        }

    }

}

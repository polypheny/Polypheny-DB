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

package org.polypheny.db.algebra.core;


import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import javax.annotation.Nonnull;
import org.bson.BsonValue;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.SemiJoinType;
import org.polypheny.db.algebra.core.ConditionalExecute.Condition;
import org.polypheny.db.algebra.logical.LogicalAggregate;
import org.polypheny.db.algebra.logical.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.LogicalCorrelate;
import org.polypheny.db.algebra.logical.LogicalDocuments;
import org.polypheny.db.algebra.logical.LogicalExchange;
import org.polypheny.db.algebra.logical.LogicalFilter;
import org.polypheny.db.algebra.logical.LogicalIntersect;
import org.polypheny.db.algebra.logical.LogicalJoin;
import org.polypheny.db.algebra.logical.LogicalMatch;
import org.polypheny.db.algebra.logical.LogicalMinus;
import org.polypheny.db.algebra.logical.LogicalProject;
import org.polypheny.db.algebra.logical.LogicalSort;
import org.polypheny.db.algebra.logical.LogicalSortExchange;
import org.polypheny.db.algebra.logical.LogicalTableScan;
import org.polypheny.db.algebra.logical.LogicalUnion;
import org.polypheny.db.algebra.logical.LogicalValues;
import org.polypheny.db.algebra.logical.LogicalViewScan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptTable.ToAlgContext;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.TranslatableTable;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Contains factory interface and default implementation for creating various alg nodes.
 */
public class AlgFactories {

    public static final ProjectFactory DEFAULT_PROJECT_FACTORY = new ProjectFactoryImpl();

    public static final FilterFactory DEFAULT_FILTER_FACTORY = new FilterFactoryImpl();

    public static final JoinFactory DEFAULT_JOIN_FACTORY = new JoinFactoryImpl();

    public static final CorrelateFactory DEFAULT_CORRELATE_FACTORY = new CorrelateFactoryImpl();

    public static final SemiJoinFactory DEFAULT_SEMI_JOIN_FACTORY = new SemiJoinFactoryImpl();

    public static final SortFactory DEFAULT_SORT_FACTORY = new SortFactoryImpl();

    public static final ExchangeFactory DEFAULT_EXCHANGE_FACTORY = new ExchangeFactoryImpl();

    public static final SortExchangeFactory DEFAULT_SORT_EXCHANGE_FACTORY = new SortExchangeFactoryImpl();

    public static final AggregateFactory DEFAULT_AGGREGATE_FACTORY = new AggregateFactoryImpl();

    public static final MatchFactory DEFAULT_MATCH_FACTORY = new MatchFactoryImpl();

    public static final SetOpFactory DEFAULT_SET_OP_FACTORY = new SetOpFactoryImpl();

    public static final ValuesFactory DEFAULT_VALUES_FACTORY = new ValuesFactoryImpl();

    public static final TableScanFactory DEFAULT_TABLE_SCAN_FACTORY = new TableScanFactoryImpl();

    public static final DocumentsFactory DEFAULT_DOCUMENTS_FACTORY = new DocumentsFactoryImpl();

    public static final ConditionalExecuteFactory DEFAULT_CONDITIONAL_EXECUTE_FACTORY = new ConditionalExecuteFactoryImpl();

    /**
     * A {@link AlgBuilderFactory} that creates a {@link AlgBuilder} that will create logical relational expressions for everything.
     */
    public static final AlgBuilderFactory LOGICAL_BUILDER =
            AlgBuilder.proto(
                    Contexts.of(
                            DEFAULT_PROJECT_FACTORY,
                            DEFAULT_FILTER_FACTORY,
                            DEFAULT_JOIN_FACTORY,
                            DEFAULT_SEMI_JOIN_FACTORY,
                            DEFAULT_SORT_FACTORY,
                            DEFAULT_EXCHANGE_FACTORY,
                            DEFAULT_SORT_EXCHANGE_FACTORY,
                            DEFAULT_AGGREGATE_FACTORY,
                            DEFAULT_MATCH_FACTORY,
                            DEFAULT_SET_OP_FACTORY,
                            DEFAULT_VALUES_FACTORY,
                            DEFAULT_TABLE_SCAN_FACTORY,
                            DEFAULT_CONDITIONAL_EXECUTE_FACTORY ) );


    private AlgFactories() {
    }


    /**
     * Can create a {@link LogicalProject} of the appropriate type for this rule's calling convention.
     */
    public interface ProjectFactory {

        /**
         * Creates a project.
         */
        AlgNode createProject( AlgNode input, List<? extends RexNode> childExprs, List<String> fieldNames );

    }


    /**
     * Implementation of {@link ProjectFactory} that returns a vanilla
     * {@link LogicalProject}.
     */
    private static class ProjectFactoryImpl implements ProjectFactory {

        @Override
        public AlgNode createProject( AlgNode input, List<? extends RexNode> childExprs, List<String> fieldNames ) {
            return LogicalProject.create( input, childExprs, fieldNames );
        }

    }


    /**
     * Can create a {@link Sort} of the appropriate type for this rule's calling convention.
     */
    public interface SortFactory {

        /**
         * Creates a sort.
         */
        AlgNode createSort( AlgNode input, AlgCollation collation, RexNode offset, RexNode fetch );

    }


    /**
     * Implementation of {@link AlgFactories.SortFactory} that returns a vanilla {@link Sort}.
     */
    private static class SortFactoryImpl implements SortFactory {

        @Override
        public AlgNode createSort( AlgNode input, AlgCollation collation, RexNode offset, RexNode fetch ) {
            return LogicalSort.create( input, collation, offset, fetch );
        }

    }


    /**
     * Can create a {@link org.polypheny.db.algebra.core.Exchange} of the appropriate type for a rule's calling convention.
     */
    public interface ExchangeFactory {

        /**
         * Creates a Exchange.
         */
        AlgNode createExchange( AlgNode input, AlgDistribution distribution );

    }


    /**
     * Implementation of {@link AlgFactories.ExchangeFactory} that returns a {@link Exchange}.
     */
    private static class ExchangeFactoryImpl implements ExchangeFactory {

        @Override
        public AlgNode createExchange( AlgNode input, AlgDistribution distribution ) {
            return LogicalExchange.create( input, distribution );
        }

    }


    /**
     * Can create a {@link SortExchange} of the appropriate type for a rule's calling convention.
     */
    public interface SortExchangeFactory {

        /**
         * Creates a {@link SortExchange}.
         */
        AlgNode createSortExchange( AlgNode input, AlgDistribution distribution, AlgCollation collation );

    }


    /**
     * Implementation of {@link AlgFactories.SortExchangeFactory} that returns a {@link SortExchange}.
     */
    private static class SortExchangeFactoryImpl implements SortExchangeFactory {

        @Override
        public AlgNode createSortExchange( AlgNode input, AlgDistribution distribution, AlgCollation collation ) {
            return LogicalSortExchange.create( input, distribution, collation );
        }

    }


    /**
     * Can create a {@link SetOp} for a particular kind of set operation (UNION, EXCEPT, INTERSECT) and of the appropriate
     * type for this rule's calling convention.
     */
    public interface SetOpFactory {

        /**
         * Creates a set operation.
         */
        AlgNode createSetOp( Kind kind, List<AlgNode> inputs, boolean all );

    }


    /**
     * Implementation of {@link AlgFactories.SetOpFactory} that returns a vanilla {@link SetOp} for the particular kind of
     * set operation (UNION, EXCEPT, INTERSECT).
     */
    private static class SetOpFactoryImpl implements SetOpFactory {

        @Override
        public AlgNode createSetOp( Kind kind, List<AlgNode> inputs, boolean all ) {
            switch ( kind ) {
                case UNION:
                    return LogicalUnion.create( inputs, all );
                case EXCEPT:
                    return LogicalMinus.create( inputs, all );
                case INTERSECT:
                    return LogicalIntersect.create( inputs, all );
                default:
                    throw new AssertionError( "not a set op: " + kind );
            }
        }

    }


    /**
     * Can create a {@link LogicalAggregate} of the appropriate type for this rule's calling convention.
     */
    public interface AggregateFactory {

        /**
         * Creates an aggregate.
         */
        AlgNode createAggregate(
                AlgNode input,
                boolean indicator,
                ImmutableBitSet groupSet,
                ImmutableList<ImmutableBitSet> groupSets,
                List<AggregateCall> aggCalls );

    }


    /**
     * Implementation of {@link AlgFactories.AggregateFactory} that returns a vanilla {@link LogicalAggregate}.
     */
    private static class AggregateFactoryImpl implements AggregateFactory {

        @Override
        @SuppressWarnings("deprecation")
        public AlgNode createAggregate(
                AlgNode input,
                boolean indicator,
                ImmutableBitSet groupSet,
                ImmutableList<ImmutableBitSet> groupSets,
                List<AggregateCall> aggCalls ) {
            return LogicalAggregate.create( input, indicator, groupSet, groupSets, aggCalls );
        }

    }


    /**
     * Can create a {@link LogicalFilter} of the appropriate type for this rule's calling convention.
     */
    public interface FilterFactory {

        /**
         * Creates a filter.
         */
        AlgNode createFilter( AlgNode input, RexNode condition );

    }


    /**
     * Implementation of {@link AlgFactories.FilterFactory} that returns a vanilla {@link LogicalFilter}.
     */
    private static class FilterFactoryImpl implements FilterFactory {

        @Override
        public AlgNode createFilter( AlgNode input, RexNode condition ) {
            return LogicalFilter.create( input, condition );
        }

    }


    /**
     * Can create a join of the appropriate type for a rule's calling convention.
     *
     * The result is typically a {@link Join}.
     */
    public interface JoinFactory {

        /**
         * Creates a join.
         *
         * @param left Left input
         * @param right Right input
         * @param condition Join condition
         * @param variablesSet Set of variables that are set by the LHS and used by the RHS and are not available to nodes above this LogicalJoin in the tree
         * @param joinType Join type
         * @param semiJoinDone Whether this join has been translated to a semi-join
         */
        AlgNode createJoin(
                AlgNode left,
                AlgNode right,
                RexNode condition,
                Set<CorrelationId> variablesSet,
                JoinAlgType joinType,
                boolean semiJoinDone );

    }


    /**
     * Implementation of {@link JoinFactory} that returns a vanilla {@link LogicalJoin}.
     */
    private static class JoinFactoryImpl implements JoinFactory {

        @Override
        public AlgNode createJoin(
                AlgNode left,
                AlgNode right,
                RexNode condition,
                Set<CorrelationId> variablesSet,
                JoinAlgType joinType,
                boolean semiJoinDone ) {
            return LogicalJoin.create(
                    left,
                    right,
                    condition,
                    variablesSet,
                    joinType,
                    semiJoinDone,
                    ImmutableList.of() );
        }

    }


    /**
     * Can create a correlate of the appropriate type for a rule's calling convention.
     *
     * The result is typically a {@link Correlate}.
     */
    public interface CorrelateFactory {

        /**
         * Creates a correlate.
         *
         * @param left Left input
         * @param right Right input
         * @param correlationId Variable name for the row of left input
         * @param requiredColumns Required columns
         * @param joinType Join type
         */
        AlgNode createCorrelate(
                AlgNode left,
                AlgNode right,
                CorrelationId correlationId,
                ImmutableBitSet requiredColumns,
                SemiJoinType joinType );

    }


    /**
     * Implementation of {@link CorrelateFactory} that returns a vanilla {@link LogicalCorrelate}.
     */
    private static class CorrelateFactoryImpl implements CorrelateFactory {

        @Override
        public AlgNode createCorrelate(
                AlgNode left,
                AlgNode right,
                CorrelationId correlationId,
                ImmutableBitSet requiredColumns,
                SemiJoinType joinType ) {
            return LogicalCorrelate.create(
                    left,
                    right,
                    correlationId,
                    requiredColumns,
                    joinType );
        }

    }


    /**
     * Can create a semi-join of the appropriate type for a rule's calling convention.
     */
    public interface SemiJoinFactory {

        /**
         * Creates a semi-join.
         *
         * @param left Left input
         * @param right Right input
         * @param condition Join condition
         */
        AlgNode createSemiJoin( AlgNode left, AlgNode right, RexNode condition );

    }


    /**
     * Implementation of {@link SemiJoinFactory} that returns a vanilla {@link SemiJoin}.
     */
    private static class SemiJoinFactoryImpl implements SemiJoinFactory {

        @Override
        public AlgNode createSemiJoin( AlgNode left, AlgNode right, RexNode condition ) {
            final JoinInfo joinInfo = JoinInfo.of( left, right, condition );
            return SemiJoin.create( left, right, condition, joinInfo.leftKeys, joinInfo.rightKeys );
        }

    }


    /**
     * Can create a {@link Values} of the appropriate type for a rule's calling convention.
     */
    public interface ValuesFactory {

        /**
         * Creates a Values.
         */
        AlgNode createValues( AlgOptCluster cluster, AlgDataType rowType, List<ImmutableList<RexLiteral>> tuples );

    }


    /**
     * Implementation of {@link ValuesFactory} that returns a {@link LogicalValues}.
     */
    private static class ValuesFactoryImpl implements ValuesFactory {

        @Override
        public AlgNode createValues( AlgOptCluster cluster, AlgDataType rowType, List<ImmutableList<RexLiteral>> tuples ) {
            return LogicalValues.create( cluster, rowType, ImmutableList.copyOf( tuples ) );
        }

    }


    public interface DocumentsFactory {

        AlgNode createDocuments(
                AlgOptCluster cluster,
                ImmutableList<BsonValue> tuples,
                AlgDataType rowType,
                ImmutableList<ImmutableList<RexLiteral>> normalizedTuple );

    }


    private static class DocumentsFactoryImpl implements DocumentsFactory {

        @Override
        public AlgNode createDocuments(
                AlgOptCluster cluster,
                ImmutableList<BsonValue> tuples,
                AlgDataType rowType,
                ImmutableList<ImmutableList<RexLiteral>> normalizedTuple ) {
            return LogicalDocuments.create(
                    cluster,
                    ImmutableList.copyOf( tuples ),
                    rowType,
                    ImmutableList.copyOf( normalizedTuple ) );
        }

    }


    /**
     * Can create a {@link TableScan} of the appropriate type for a rule's calling convention.
     */
    public interface TableScanFactory {

        /**
         * Creates a {@link TableScan}.
         */
        AlgNode createScan( AlgOptCluster cluster, AlgOptTable table );

    }


    /**
     * Implementation of {@link TableScanFactory} that returns a {@link LogicalTableScan}.
     */
    private static class TableScanFactoryImpl implements TableScanFactory {

        @Override
        public AlgNode createScan( AlgOptCluster cluster, AlgOptTable table ) {

            // Check if RelOptTable contains a View, in this case a LogicalViewTableScan needs to be created
            if ( (((AlgOptTableImpl) table).getTable()) instanceof LogicalTable ) {
                Catalog catalog = Catalog.getInstance();
                long idLogical = ((LogicalTable) ((AlgOptTableImpl) table).getTable()).getTableId();
                CatalogTable catalogTable = catalog.getTable( idLogical );
                if ( catalogTable.tableType == TableType.VIEW ) {
                    return LogicalViewScan.create( cluster, table );
                } else {
                    return LogicalTableScan.create( cluster, table );
                }
            } else {
                return LogicalTableScan.create( cluster, table );
            }
        }

    }


    /**
     * Creates a {@link TableScanFactory} that can expand {@link TranslatableTable} instances.
     *
     * @param tableScanFactory Factory for non-translatable tables
     * @return Table scan factory
     */
    @Nonnull
    public static TableScanFactory expandingScanFactory( @Nonnull TableScanFactory tableScanFactory ) {
        return ( cluster, table ) -> {
            final TranslatableTable translatableTable = table.unwrap( TranslatableTable.class );
            if ( translatableTable != null ) {
                final ToAlgContext toAlgContext = () -> cluster;
                return translatableTable.toAlg( toAlgContext, table );
            }
            return tableScanFactory.createScan( cluster, table );
        };
    }


    /**
     * Can create a {@link Match} of the appropriate type for a rule's calling convention.
     */
    public interface MatchFactory {

        /**
         * Creates a {@link Match}.
         */
        AlgNode createMatch(
                AlgNode input,
                RexNode pattern,
                AlgDataType rowType,
                boolean strictStart,
                boolean strictEnd,
                Map<String, RexNode> patternDefinitions,
                Map<String, RexNode> measures,
                RexNode after,
                Map<String, ? extends SortedSet<String>> subsets,
                boolean allRows,
                List<RexNode> partitionKeys,
                AlgCollation orderKeys,
                RexNode interval );

    }


    /**
     * Implementation of {@link MatchFactory} that returns a {@link LogicalMatch}.
     */
    private static class MatchFactoryImpl implements MatchFactory {

        @Override
        public AlgNode createMatch(
                AlgNode input,
                RexNode pattern,
                AlgDataType rowType,
                boolean strictStart,
                boolean strictEnd,
                Map<String, RexNode> patternDefinitions,
                Map<String, RexNode> measures,
                RexNode after,
                Map<String, ? extends SortedSet<String>> subsets,
                boolean allRows,
                List<RexNode> partitionKeys,
                AlgCollation orderKeys,
                RexNode interval ) {
            return LogicalMatch.create(
                    input,
                    rowType,
                    pattern,
                    strictStart,
                    strictEnd,
                    patternDefinitions,
                    measures,
                    after,
                    subsets,
                    allRows,
                    partitionKeys,
                    orderKeys,
                    interval );
        }

    }


    /**
     * Can create a {@link ConditionalExecute} of the appropriate type for a rule's calling convention.
     */
    public interface ConditionalExecuteFactory {

        /**
         * Creates a {@link ConditionalExecute}.
         */
        AlgNode createConditionalExecute(
                AlgNode left,
                AlgNode right,
                Condition condition,
                Class<? extends Exception> exceptionClass,
                String exceptionMessage );

    }


    /**
     * Implementation of {@link MatchFactory} that returns a {@link LogicalMatch}.
     */
    private static class ConditionalExecuteFactoryImpl implements ConditionalExecuteFactory {

        @Override
        public AlgNode createConditionalExecute(
                AlgNode left,
                AlgNode right,
                Condition condition,
                Class<? extends Exception> exceptionClass,
                String exceptionMessage ) {
            return LogicalConditionalExecute.create(
                    left,
                    right,
                    condition,
                    exceptionClass,
                    exceptionMessage );
        }

    }

}

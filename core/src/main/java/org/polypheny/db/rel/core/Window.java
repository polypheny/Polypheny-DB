/*
 * Copyright 2019-2020 The Polypheny Project
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

package ch.unibas.dmi.dbis.polyphenydb.rel.core;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollations;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelWriter;
import ch.unibas.dmi.dbis.polyphenydb.rel.SingleRel;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexCall;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexChecker;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLocalRef;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexSlot;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexWindowBound;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlAggFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorImpl;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableIntList;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.linq4j.Ord;


/**
 * A relational expression representing a set of window aggregates.
 *
 * A Window can handle several window aggregate functions, over several partitions, with pre- and post-expressions, and an optional post-filter.
 * Each of the partitions is defined by a partition key (zero or more columns) and a range (logical or physical). The partitions expect the data to be sorted correctly on input to the relational expression.
 *
 * Each {@link Window.Group} has a set of {@link ch.unibas.dmi.dbis.polyphenydb.rex.RexOver} objects.
 *
 * Created by {@link ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectToWindowRule}.
 */
public abstract class Window extends SingleRel {

    public final ImmutableList<Group> groups;
    public final ImmutableList<RexLiteral> constants;


    /**
     * Creates a window relational expression.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param input Input relational expression
     * @param constants List of constants that are additional inputs
     * @param rowType Output row type
     * @param groups Windows
     */
    public Window( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<RexLiteral> constants, RelDataType rowType, List<Group> groups ) {
        super( cluster, traitSet, input );
        this.constants = ImmutableList.copyOf( constants );
        assert rowType != null;
        this.rowType = rowType;
        this.groups = ImmutableList.copyOf( groups );
    }


    @Override
    public boolean isValid( Litmus litmus, Context context ) {
        // In the window specifications, an aggregate call such as 'SUM(RexInputRef #10)' refers to expression #10 of inputProgram. (Not its projections.)
        final RelDataType childRowType = getInput().getRowType();

        final int childFieldCount = childRowType.getFieldCount();
        final int inputSize = childFieldCount + constants.size();
        final List<RelDataType> inputTypes =
                new AbstractList<RelDataType>() {
                    @Override
                    public RelDataType get( int index ) {
                        return index < childFieldCount
                                ? childRowType.getFieldList().get( index ).getType()
                                : constants.get( index - childFieldCount ).getType();
                    }


                    @Override
                    public int size() {
                        return inputSize;
                    }
                };

        final RexChecker checker = new RexChecker( inputTypes, context, litmus );
        int count = 0;
        for ( Group group : groups ) {
            for ( RexWinAggCall over : group.aggCalls ) {
                ++count;
                if ( !checker.isValid( over ) ) {
                    return litmus.fail( null );
                }
            }
        }
        if ( count == 0 ) {
            return litmus.fail( "empty" );
        }
        return litmus.succeed();
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        super.explainTerms( pw );
        for ( Ord<Group> window : Ord.zip( groups ) ) {
            pw.item( "window#" + window.i, window.e.toString() );
        }
        return pw;
    }


    public static ImmutableIntList getProjectOrdinals( final List<RexNode> exprs ) {
        return ImmutableIntList.copyOf(
                new AbstractList<Integer>() {
                    @Override
                    public Integer get( int index ) {
                        return ((RexSlot) exprs.get( index )).getIndex();
                    }


                    @Override
                    public int size() {
                        return exprs.size();
                    }
                } );
    }


    public static RelCollation getCollation( final List<RexFieldCollation> collations ) {
        return RelCollations.of(
                new AbstractList<RelFieldCollation>() {
                    @Override
                    public RelFieldCollation get( int index ) {
                        final RexFieldCollation collation = collations.get( index );
                        return new RelFieldCollation(
                                ((RexLocalRef) collation.left).getIndex(),
                                collation.getDirection(),
                                collation.getNullDirection() );
                    }


                    @Override
                    public int size() {
                        return collations.size();
                    }
                } );
    }


    /**
     * Returns constants that are additional inputs of current relation.
     *
     * @return constants that are additional inputs of current relation
     */
    public List<RexLiteral> getConstants() {
        return constants;
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        // Cost is proportional to the number of rows and the number of components (groups and aggregate functions). There is no I/O cost.
        //
        // TODO #1. Add memory cost.
        // TODO #2. MIN and MAX have higher CPU cost than SUM and COUNT.
        final double rowsIn = mq.getRowCount( getInput() );
        int count = groups.size();
        for ( Group group : groups ) {
            count += group.aggCalls.size();
        }
        return planner.getCostFactory().makeCost( rowsIn, rowsIn * count, 0 );
    }


    /**
     * Group of windowed aggregate calls that have the same window specification.
     *
     * The specification is defined by an upper and lower bound, and also has zero or more partitioning columns.
     *
     * A window is either logical or physical. A physical window is measured in terms of row count. A logical window is measured in terms of rows within a certain distance from the current sort key.
     *
     * For example:
     *
     * <ul>
     * <li><code>ROWS BETWEEN 10 PRECEDING and 5 FOLLOWING</code> is a physical window with an upper and lower bound;</li>
     * <li><code>RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND UNBOUNDED FOLLOWING</code> is a logical window with only a lower bound;</li>
     * <li><code>RANGE INTERVAL '10' MINUTES PRECEDING</code> (which is equivalent to <code>RANGE BETWEEN INTERVAL '10' MINUTES PRECEDING AND CURRENT ROW</code>) is a logical window with an upper and lower bound.</li>
     * </ul>
     */
    public static class Group {

        public final ImmutableBitSet keys;
        public final boolean isRows;
        public final RexWindowBound lowerBound;
        public final RexWindowBound upperBound;
        public final RelCollation orderKeys;
        private final String digest;

        /**
         * List of {@link Window.RexWinAggCall} objects, each of which is a call to a {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlAggFunction}.
         */
        public final ImmutableList<RexWinAggCall> aggCalls;


        public Group( ImmutableBitSet keys, boolean isRows, RexWindowBound lowerBound, RexWindowBound upperBound, RelCollation orderKeys, List<RexWinAggCall> aggCalls ) {
            assert orderKeys != null : "precondition: ordinals != null";
            assert keys != null;
            this.keys = keys;
            this.isRows = isRows;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.orderKeys = orderKeys;
            this.aggCalls = ImmutableList.copyOf( aggCalls );
            this.digest = computeString();
        }


        public String toString() {
            return digest;
        }


        private String computeString() {
            final StringBuilder buf = new StringBuilder();
            buf.append( "window(partition " );
            buf.append( keys );
            buf.append( " order by " );
            buf.append( orderKeys );
            buf.append( isRows ? " rows " : " range " );
            if ( lowerBound != null ) {
                if ( upperBound != null ) {
                    buf.append( "between " );
                    buf.append( lowerBound );
                    buf.append( " and " );
                    buf.append( upperBound );
                } else {
                    buf.append( lowerBound );
                }
            } else if ( upperBound != null ) {
                buf.append( upperBound );
            }
            buf.append( " aggs " );
            buf.append( aggCalls );
            buf.append( ")" );
            return buf.toString();
        }


        @Override
        public boolean equals( Object obj ) {
            return this == obj || obj instanceof Group && this.digest.equals( ((Group) obj).digest );
        }


        @Override
        public int hashCode() {
            return digest.hashCode();
        }


        public RelCollation collation() {
            return orderKeys;
        }


        /**
         * Returns if the window is guaranteed to have rows. This is useful to refine data type of window aggregates. For instance sum(non-nullable) over (empty window) is NULL.
         *
         * @return true when the window is non-empty
         * @see ch.unibas.dmi.dbis.polyphenydb.sql.SqlWindow#isAlwaysNonEmpty()
         * @see ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorBinding#getGroupCount()
         * @see SqlValidatorImpl#resolveWindow(ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode, ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope, boolean)
         */
        public boolean isAlwaysNonEmpty() {
            int lowerKey = lowerBound.getOrderKey();
            int upperKey = upperBound.getOrderKey();
            return lowerKey > -1 && lowerKey <= upperKey;
        }


        /**
         * Presents a view of the {@link RexWinAggCall} list as a list of {@link AggregateCall}.
         */
        public List<AggregateCall> getAggregateCalls( Window windowRel ) {
            final List<String> fieldNames = Util.skip( windowRel.getRowType().getFieldNames(), windowRel.getInput().getRowType().getFieldCount() );
            return new AbstractList<AggregateCall>() {
                @Override
                public int size() {
                    return aggCalls.size();
                }


                @Override
                public AggregateCall get( int index ) {
                    final RexWinAggCall aggCall = aggCalls.get( index );
                    final SqlAggFunction op = (SqlAggFunction) aggCall.getOperator();
                    return AggregateCall.create(
                            op,
                            aggCall.distinct,
                            false,
                            getProjectOrdinals( aggCall.getOperands() ),
                            -1,
                            RelCollations.EMPTY,
                            aggCall.getType(),
                            fieldNames.get( aggCall.ordinal ) );
                }
            };
        }
    }


    /**
     * A call to a windowed aggregate function.
     *
     * Belongs to a {@link Window.Group}.
     *
     * It's a bastard son of a {@link ch.unibas.dmi.dbis.polyphenydb.rex.RexCall}; similar enough that it gets visited by a {@link ch.unibas.dmi.dbis.polyphenydb.rex.RexVisitor}, but it also has some extra data members.
     */
    public static class RexWinAggCall extends RexCall {

        /**
         * Ordinal of this aggregate within its partition.
         */
        public final int ordinal;

        /**
         * Whether to eliminate duplicates before applying aggregate function.
         */
        public final boolean distinct;


        /**
         * Creates a RexWinAggCall.
         *
         * @param aggFun Aggregate function
         * @param type Result type
         * @param operands Operands to call
         * @param ordinal Ordinal within its partition
         * @param distinct Eliminate duplicates before applying aggregate function
         */
        public RexWinAggCall( SqlAggFunction aggFun, RelDataType type, List<RexNode> operands, int ordinal, boolean distinct ) {
            super( type, aggFun, operands );
            this.ordinal = ordinal;
            this.distinct = distinct;
        }


        /**
         * {@inheritDoc}
         *
         * Override {@link RexCall}, defining equality based on identity.
         */
        @Override
        public boolean equals( Object obj ) {
            return this == obj;
        }


        @Override
        public int hashCode() {
            return Objects.hash( digest, ordinal, distinct );
        }


        @Override
        public RexCall clone( RelDataType type, List<RexNode> operands ) {
            throw new UnsupportedOperationException();
        }
    }
}


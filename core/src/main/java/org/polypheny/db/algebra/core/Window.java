/*
 * Copyright 2019-2024 The Polypheny Project
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
import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexChecker;
import org.polypheny.db.rex.RexFieldCollation;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexSlot;
import org.polypheny.db.rex.RexWindowBound;
import org.polypheny.db.runtime.ComparableList;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Util;


/**
 * A relational expression representing a set of window aggregates.
 *
 * A Window can handle several window aggregate functions, over several partitions, with pre- and post-expressions, and an optional post-filter.
 * Each of the partitions is defined by a partition key (zero or more columns) and a range (logical or physical). The partitions expect the data to be sorted correctly on input to the relational expression.
 *
 * Each {@link Window.Group} has a set of {@link org.polypheny.db.rex.RexOver} objects.
 *
 * Created by {@link org.polypheny.db.algebra.rules.ProjectToWindowRule}.
 */
public abstract class Window extends SingleAlg {

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
    public Window( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, List<RexLiteral> constants, AlgDataType rowType, List<Group> groups ) {
        super( cluster, traitSet, input );
        this.constants = ImmutableList.copyOf( constants );
        assert rowType != null;
        this.rowType = rowType;
        this.groups = ImmutableList.copyOf( groups );
    }


    @Override
    public boolean isValid( Litmus litmus, Context context ) {
        // In the window specifications, an aggregate call such as 'SUM(RexInputRef #10)' refers to expression #10 of inputProgram. (Not its projections.)
        final AlgDataType childRowType = getInput().getTupleType();

        final int childFieldCount = childRowType.getFieldCount();
        final int inputSize = childFieldCount + constants.size();
        final List<AlgDataType> inputTypes =
                new AbstractList<AlgDataType>() {
                    @Override
                    public AlgDataType get( int index ) {
                        return index < childFieldCount
                                ? childRowType.getFields().get( index ).getType()
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
    public AlgWriter explainTerms( AlgWriter pw ) {
        super.explainTerms( pw );
        for ( Ord<Group> window : Ord.zip( groups ) ) {
            pw.item( "window#" + window.i, window.e.toString() );
        }
        return pw;
    }


    public static ComparableList<Integer> getProjectOrdinals( final List<RexNode> exprs ) {
        return ComparableList.copyOf(
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


    public static AlgCollation getCollation( final List<RexFieldCollation> collations ) {
        return AlgCollations.of(
                new AbstractList<>() {
                    @Override
                    public AlgFieldCollation get( int index ) {
                        final RexFieldCollation collation = collations.get( index );
                        return new AlgFieldCollation(
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
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        // Cost is proportional to the number of rows and the number of components (groups and aggregate functions). There is no I/O cost.
        //
        // TODO #1. Add memory cost.
        // TODO #2. MIN and MAX have higher CPU cost than SUM and COUNT.
        final double rowsIn = mq.getTupleCount( getInput() );
        int count = groups.size();
        for ( Group group : groups ) {
            count += group.aggCalls.size();
        }
        return planner.getCostFactory().makeCost( rowsIn, rowsIn * count, 0 );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                (constants != null ? constants.stream().map( RexLiteral::hashCode ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "") + "$" +
                rowType.toString() + "$" +
                (groups != null ? groups.hashCode() : "") + "&";

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
        public final AlgCollation orderKeys;
        private final String digest;

        /**
         * List of {@link Window.RexWinAggCall} objects, each of which is a call to a {@link AggFunction}.
         */
        public final ImmutableList<RexWinAggCall> aggCalls;


        public Group( ImmutableBitSet keys, boolean isRows, RexWindowBound lowerBound, RexWindowBound upperBound, AlgCollation orderKeys, List<RexWinAggCall> aggCalls ) {
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


        public AlgCollation collation() {
            return orderKeys;
        }


        /**
         * Returns if the window is guaranteed to have rows. This is useful to refine data type of window aggregates. For instance sum(non-nullable) over (empty window) is NULL.
         *
         * @return true when the window is non-empty
         * #@see org.polypheny.db.sql.SqlWindow#isAlwaysNonEmpty()
         * #@see org.polypheny.db.sql.SqlOperatorBinding#getGroupCount()
         * #@see SqlValidatorImpl#resolveWindow(org.polypheny.db.sql.SqlNode, org.polypheny.db.sql.validate.SqlValidatorScope, boolean)
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
            final List<String> fieldNames = Util.skip( windowRel.getTupleType().getFieldNames(), windowRel.getInput().getTupleType().getFieldCount() );
            return new AbstractList<AggregateCall>() {
                @Override
                public int size() {
                    return aggCalls.size();
                }


                @Override
                public AggregateCall get( int index ) {
                    final RexWinAggCall aggCall = aggCalls.get( index );
                    final AggFunction op = (AggFunction) aggCall.getOperator();
                    return AggregateCall.create(
                            op,
                            aggCall.distinct,
                            false,
                            getProjectOrdinals( aggCall.getOperands() ),
                            -1,
                            AlgCollations.EMPTY,
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
     * It's a bastard son of a {@link org.polypheny.db.rex.RexCall}; similar enough that it gets visited by a {@link org.polypheny.db.rex.RexVisitor}, but it also has some extra data members.
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
        public RexWinAggCall( Operator aggFun, AlgDataType type, List<RexNode> operands, int ordinal, boolean distinct ) {
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
        public RexCall clone( AlgDataType type, List<RexNode> operands ) {
            throw new UnsupportedOperationException();
        }

    }

}


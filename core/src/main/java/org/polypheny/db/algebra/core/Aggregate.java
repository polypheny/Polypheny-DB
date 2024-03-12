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


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.rules.AggregateExpandDistinctAggregatesRule;
import org.polypheny.db.algebra.rules.AggregateProjectPullUpConstantsRule;
import org.polypheny.db.algebra.rules.AggregateReduceFunctionsRule;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Function;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.nodes.validate.ValidatorException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.runtime.Resources;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Bug;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Relational operator that eliminates duplicates and computes totals.
 * <p>
 * It corresponds to the {@code GROUP BY} operator in a SQL query statement, together with the aggregate functions in
 * the {@code SELECT} clause.
 * <p>
 * Rules:
 *
 * <ul>
 * <li>{@link AggregateProjectPullUpConstantsRule}</li>
 * <li>{@link AggregateExpandDistinctAggregatesRule}</li>
 * <li>{@link AggregateReduceFunctionsRule}</li>
 * </ul>
 */
public abstract class Aggregate extends SingleAlg {

    /**
     * @see Bug#CALCITE_461_FIXED
     */
    public static boolean isSimple( Aggregate aggregate ) {
        return aggregate.getGroupType() == Group.SIMPLE;
    }


    /**
     * Whether there are indicator fields.
     * <p>
     * We strongly discourage the use indicator fields, because they cause the output row type of GROUPING SETS queries to be different from regular GROUP BY queries,
     * and recommend that you set this field to {@code false}.
     */
    public final boolean indicator;
    protected final List<AggregateCall> aggCalls;

    @Getter
    protected final ImmutableBitSet groupSet;

    @Getter
    public final ImmutableList<ImmutableBitSet> groupSets;


    /**
     * Creates an Aggregate.
     * <p>
     * All members of {@code groupSets} must be sub-sets of {@code groupSet}. For a simple {@code GROUP BY}, {@code groupSets} is a singleton list containing {@code groupSet}.
     * <p>
     * If {@code GROUP BY} is not specified, or equivalently if {@code GROUP BY ()} is specified, {@code groupSet} will be the empty set, and {@code groupSets}
     * will have one element, that empty set.
     * <p>
     * If {@code CUBE}, {@code ROLLUP} or {@code GROUPING SETS} are specified, {@code groupSets} will have additional elements, but they must each be a subset of {@code groupSet},
     * and they must be sorted by inclusion: {@code (0, 1, 2), (1), (0, 2), (0), ()}.
     *
     * @param cluster Cluster
     * @param traits Traits
     * @param child Child
     * @param indicator Whether row type should include indicator fields to indicate which grouping set is active; true is deprecated
     * @param groupSet Bit set of grouping fields
     * @param groupSets List of all grouping sets; null for just {@code groupSet}
     * @param aggCalls Collection of calls to aggregate functions
     */
    protected Aggregate( AlgCluster cluster, AlgTraitSet traits, AlgNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        super( cluster, traits, child );
        this.indicator = indicator; // true is allowed, but discouraged
        this.aggCalls = ImmutableList.copyOf( aggCalls );
        this.groupSet = Objects.requireNonNull( groupSet );
        if ( groupSets == null ) {
            this.groupSets = ImmutableList.of( groupSet );
        } else {
            this.groupSets = ImmutableList.copyOf( groupSets );
            assert ImmutableBitSet.ORDERING.isStrictlyOrdered( groupSets ) : groupSets;
            for ( ImmutableBitSet set : groupSets ) {
                assert groupSet.contains( set );
            }
        }
        assert groupSet.length() <= child.getTupleType().getFieldCount();
        for ( AggregateCall aggCall : aggCalls ) {
            assert typeMatchesInferred( aggCall, Litmus.THROW );
            Preconditions.checkArgument( aggCall.filterArg < 0 || isPredicate( child, aggCall.filterArg ), "filter must be BOOLEAN NOT NULL" );
        }
    }


    public static boolean isNotGrandTotal( Aggregate aggregate ) {
        return aggregate.getGroupCount() > 0;
    }


    public static boolean noIndicator( Aggregate aggregate ) {
        return !aggregate.indicator;
    }


    private boolean isPredicate( AlgNode input, int index ) {
        final AlgDataType type = input.getTupleType().getFields().get( index ).getType();
        return type.getPolyType() == PolyType.BOOLEAN && !type.isNullable();
    }


    @Override
    public final AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return copy( traitSet, sole( inputs ), indicator, groupSet, groupSets, aggCalls );
    }


    /**
     * Creates a copy of this aggregate.
     *
     * @param traitSet Traits
     * @param input Input
     * @param indicator Whether row type should include indicator fields to indicate which grouping set is active; must be true if aggregate is not simple
     * @param groupSet Bit set of grouping fields
     * @param groupSets List of all grouping sets; null for just {@code groupSet}
     * @param aggCalls Collection of calls to aggregate functions
     * @return New {@code Aggregate} if any parameter differs from the value of this {@code Aggregate}, or just {@code this} if all the parameters are the same
     * @see #copy(AlgTraitSet, java.util.List)
     */
    public abstract Aggregate copy( AlgTraitSet traitSet, AlgNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls );


    /**
     * Returns a list of calls to aggregate functions.
     *
     * @return list of calls to aggregate functions
     */
    public List<AggregateCall> getAggCallList() {
        return aggCalls;
    }


    /**
     * Returns a list of calls to aggregate functions together with their output field names.
     *
     * @return list of calls to aggregate functions and their output field names
     */
    public List<Pair<AggregateCall, String>> getNamedAggCalls() {
        final int offset = getGroupCount() + getIndicatorCount();
        return Pair.zip( aggCalls, Util.skip( getTupleType().getFieldNames(), offset ) );
    }


    /**
     * Returns the number of grouping fields.
     * These grouping fields are the leading fields in both the input and output records.
     * <p>
     * NOTE: The {@link #getGroupSet()} data structure allows for the grouping fields to not be on the leading edge. New code should, if possible, assume that grouping fields are
     * in arbitrary positions in the input relational expression.
     *
     * @return number of grouping fields
     */
    public int getGroupCount() {
        return groupSet.cardinality();
    }


    /**
     * Returns the number of indicator fields.
     * <p>
     * This is the same as {@link #getGroupCount()} if {@link #indicator} is true, zero if {@code indicator} is false.
     * <p>
     * The offset of the first aggregate call in the output record is always <i>groupCount + indicatorCount</i>.
     *
     * @return number of indicator fields
     */
    public int getIndicatorCount() {
        return indicator ? getGroupCount() : 0;
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        // We skip the "groups" element if it is a singleton of "group".
        super.explainTerms( pw )
                .item( "group", groupSet )
                .itemIf( "groups", groupSets, getGroupType() != Group.SIMPLE )
                .itemIf( "indicator", indicator, indicator )
                .itemIf( "aggs", aggCalls, pw.nest() );
        if ( !pw.nest() ) {
            for ( Ord<AggregateCall> ord : Ord.zip( aggCalls ) ) {
                pw.item( Util.first( ord.e.name, "agg#" + ord.i ), ord.e );
            }
        }
        return pw;
    }


    @Override
    public double estimateTupleCount( AlgMetadataQuery mq ) {
        // Assume that each sort column has 50% of the value count.
        // Therefore, one sort column has .5 * rowCount, 2 sort columns give .75 * rowCount. Zero sort columns yields 1 row (or 0 if the input is empty).
        final int groupCount = groupSet.cardinality();
        if ( groupCount == 0 ) {
            return 1;
        } else {
            double rowCount = super.estimateTupleCount( mq );
            rowCount *= 1.0 - Math.pow( .5, groupCount );
            return rowCount;
        }
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        // REVIEW jvs:  This is bogus, but no more bogus than what's currently in Join.
        double rowCount = mq.getTupleCount( this );
        // Aggregates with more aggregate functions cost a bit more
        float multiplier = 1f + (float) aggCalls.size() * 0.125f;
        for ( AggregateCall aggCall : aggCalls ) {
            if ( aggCall.getAggregation().getName().equals( "SUM" ) ) {
                // Pretend that SUM costs a bit more than $SUM0, to make things deterministic.
                multiplier += 0.0125f;
            }
        }
        return planner.getCostFactory().makeCost( rowCount * multiplier, 0, 0 );
    }


    @Override
    protected AlgDataType deriveRowType() {
        return deriveRowType( getCluster().getTypeFactory(), getInput().getTupleType(), indicator, groupSet, groupSets, aggCalls );
    }


    /**
     * Computes the row type of {@code Aggregate} before it exists.
     *
     * @param typeFactory Type factory
     * @param inputRowType Input row type
     * @param indicator Whether row type should include indicator fields to indicate which grouping set is active; must be true if aggregate is not simple
     * @param groupSet Bit set of grouping fields
     * @param groupSets List of all grouping sets; null for just {@code groupSet}
     * @param aggCalls Collection of calls to aggregate functions
     * @return Row type of the aggregate
     */
    public static AlgDataType deriveRowType( AlgDataTypeFactory typeFactory, final AlgDataType inputRowType, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, final List<AggregateCall> aggCalls ) {
        final List<Integer> groupList = groupSet.asList();
        assert groupList.size() == groupSet.cardinality();
        final AlgDataTypeFactory.Builder builder = typeFactory.builder();
        final List<AlgDataTypeField> fieldList = inputRowType.getFields();
        final Set<String> containedNames = new HashSet<>();
        for ( int groupKey : groupList ) {
            final AlgDataTypeField field = fieldList.get( groupKey );
            containedNames.add( field.getName() );
            builder.add( field );
            if ( groupSets != null && !allContain( groupSets, groupKey ) ) {
                builder.nullable( true );
            }
        }
        if ( indicator ) {
            for ( int groupKey : groupList ) {
                final AlgDataType booleanType = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.BOOLEAN ), false );
                final String base = "i$" + fieldList.get( groupKey ).getName();
                String name = base;
                int i = 0;
                while ( containedNames.contains( name ) ) {
                    name = base + "_" + i++;
                }
                containedNames.add( name );
                builder.add( null, name, null, booleanType );
            }
        }
        for ( Ord<AggregateCall> aggCall : Ord.zip( aggCalls ) ) {
            final String base;
            base = Objects.requireNonNullElseGet( aggCall.e.name, () -> "$f" + (groupList.size() + aggCall.i) );
            String name = base;
            int i = 0;
            while ( containedNames.contains( name ) ) {
                name = base + "_" + i++;
            }
            containedNames.add( name );
            builder.add( null, name, null, aggCall.e.type );
        }
        return builder.build();
    }


    private static boolean allContain( List<ImmutableBitSet> groupSets, int groupKey ) {
        for ( ImmutableBitSet groupSet : groupSets ) {
            if ( !groupSet.get( groupKey ) ) {
                return false;
            }
        }
        return true;
    }


    @Override
    public boolean isValid( Litmus litmus, Context context ) {
        return super.isValid( litmus, context ) && litmus.check( Util.isDistinct( getTupleType().getFieldNames() ), "distinct field names: {}", getTupleType() );
    }


    /**
     * Returns whether the inferred type of {@link AggregateCall} matches the type it was given when it was created.
     *
     * @param aggCall Aggregate call
     * @param litmus What to do if an error is detected (types do not match)
     * @return Whether the inferred and declared types match
     */
    private boolean typeMatchesInferred( final AggregateCall aggCall, final Litmus litmus ) {
        Function aggFunction = aggCall.getAggregation();
        AggCallBinding callBinding = aggCall.createBinding( this );
        AlgDataType type = aggFunction.inferReturnType( callBinding );
        AlgDataType expectedType = aggCall.type;
        return AlgOptUtil.eq( "aggCall type", expectedType, "inferred type", type, litmus );
    }


    /**
     * Returns whether any of the aggregates are DISTINCT.
     *
     * @return Whether any of the aggregates are DISTINCT
     */
    public boolean containsDistinctCall() {
        for ( AggregateCall call : aggCalls ) {
            if ( call.isDistinct() ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Returns the type of roll-up.
     *
     * @return Type of roll-up
     */
    public Group getGroupType() {
        return Group.induce( groupSet, groupSets );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                (aggCalls != null ? aggCalls.stream().map( Objects::toString ).collect( Collectors.joining( " $ " ) ) : "") + "$" +
                (groupSet != null ? groupSet.toString() : "") + "$" +
                (groupSets != null ? groupSets.stream().map( Objects::toString ).collect( Collectors.joining( " $ " ) ) : "") + "$" +
                indicator + "&";
    }


    /**
     * What kind of roll-up is it?
     */
    public enum Group {
        SIMPLE,
        ROLLUP,
        CUBE,
        OTHER;


        public static Group induce( ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets ) {
            if ( !ImmutableBitSet.ORDERING.isStrictlyOrdered( groupSets ) ) {
                throw new IllegalArgumentException( "must be sorted: " + groupSets );
            }
            if ( groupSets.size() == 1 && groupSets.get( 0 ).equals( groupSet ) ) {
                return SIMPLE;
            }
            if ( groupSets.size() == IntMath.pow( 2, groupSet.cardinality() ) ) {
                return CUBE;
            }
            checkRollup:
            if ( groupSets.size() == groupSet.cardinality() + 1 ) {
                ImmutableBitSet g = groupSet;
                for ( ImmutableBitSet bitSet : groupSets ) {
                    if ( !bitSet.equals( g ) ) {
                        break checkRollup;
                    }
                    g = g.clear( g.length() - 1 );
                }
                assert g.isEmpty();
                return ROLLUP;
            }
            return OTHER;
        }
    }


    /**
     * Implementation of the {@link OperatorBinding} interface for an {@link AggregateCall aggregate call} applied to a set of operands in the context of
     * a {@link LogicalRelAggregate}.
     */
    public static class AggCallBinding extends OperatorBinding {

        private final List<AlgDataType> operands;
        private final int groupCount;
        private final boolean filter;


        /**
         * Creates an AggCallBinding
         *
         * @param typeFactory Type factory
         * @param aggFunction Aggregate function
         * @param operands Data types of operands
         * @param groupCount Number of columns in the GROUP BY clause
         * @param filter Whether the aggregate function has a FILTER clause
         */
        public AggCallBinding( AlgDataTypeFactory typeFactory, AggFunction aggFunction, List<AlgDataType> operands, int groupCount, boolean filter ) {
            super( typeFactory, aggFunction );
            this.operands = operands;
            this.groupCount = groupCount;
            this.filter = filter;
            assert operands != null : "operands of aggregate call should not be null";
            assert groupCount >= 0 : "number of group by columns should be greater than zero in aggregate call. Got " + groupCount;
        }


        @Override
        public int getGroupCount() {
            return groupCount;
        }


        @Override
        public boolean hasFilter() {
            return filter;
        }


        @Override
        public int getOperandCount() {
            return operands.size();
        }


        @Override
        public AlgDataType getOperandType( int ordinal ) {
            return operands.get( ordinal );
        }


        @Override
        public PolyphenyDbException newError( Resources.ExInst<ValidatorException> e ) {
            return CoreUtil.newContextException( ParserPos.ZERO, e );
        }

    }

}


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

package org.polypheny.db.algebra.rules;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.fun.SplittableAggFunction;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.Bug;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.mapping.Mapping;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Planner rule that pushes an {@link org.polypheny.db.algebra.core.Aggregate} past a {@link org.polypheny.db.algebra.core.Join}.
 */
public class AggregateJoinTransposeRule extends AlgOptRule {

    public static final AggregateJoinTransposeRule INSTANCE = new AggregateJoinTransposeRule( LogicalRelAggregate.class, LogicalRelJoin.class, AlgFactories.LOGICAL_BUILDER, false );

    /**
     * Extended instance of the rule that can push down aggregate functions.
     */
    public static final AggregateJoinTransposeRule EXTENDED = new AggregateJoinTransposeRule( LogicalRelAggregate.class, LogicalRelJoin.class, AlgFactories.LOGICAL_BUILDER, true );

    private final boolean allowFunctions;


    /**
     * Creates an AggregateJoinTransposeRule.
     */
    public AggregateJoinTransposeRule( Class<? extends Aggregate> aggregateClass, Class<? extends Join> joinClass, AlgBuilderFactory algBuilderFactory, boolean allowFunctions ) {
        super(
                operand(
                        aggregateClass,
                        null,
                        agg -> isAggregateSupported( agg, allowFunctions ),
                        operand( joinClass, null, join -> join.getJoinType() == JoinAlgType.INNER, any() ) ),
                algBuilderFactory, null );
        this.allowFunctions = allowFunctions;
    }


    private static boolean isAggregateSupported( Aggregate aggregate, boolean allowFunctions ) {
        if ( !allowFunctions && !aggregate.getAggCallList().isEmpty() ) {
            return false;
        }
        if ( aggregate.getGroupType() != Aggregate.Group.SIMPLE ) {
            return false;
        }
        // If any aggregate functions do not support splitting, bail out
        // If any aggregate call has a filter or is distinct, bail out
        for ( AggregateCall aggregateCall : aggregate.getAggCallList() ) {
            if ( aggregateCall.getAggregation().unwrap( SplittableAggFunction.class ).isEmpty() ) {
                return false;
            }
            if ( aggregateCall.filterArg >= 0 || aggregateCall.isDistinct() ) {
                return false;
            }
        }
        return true;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Aggregate aggregate = call.alg( 0 );
        final Join join = call.alg( 1 );
        final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
        final AlgBuilder algBuilder = call.builder();

        // Do the columns used by the join appear in the output of the aggregate?
        final ImmutableBitSet aggregateColumns = aggregate.getGroupSet();
        final AlgMetadataQuery mq = call.getMetadataQuery();
        final ImmutableBitSet keyColumns = keyColumns( aggregateColumns, mq.getPulledUpPredicates( join ).pulledUpPredicates );
        final ImmutableBitSet joinColumns = AlgOptUtil.InputFinder.bits( join.getCondition() );
        final boolean allColumnsInAggregate = keyColumns.contains( joinColumns );
        final ImmutableBitSet belowAggregateColumns = aggregateColumns.union( joinColumns );

        // Split join condition
        final List<Integer> leftKeys = new ArrayList<>();
        final List<Integer> rightKeys = new ArrayList<>();
        final List<Boolean> filterNulls = new ArrayList<>();
        RexNode nonEquiConj = AlgOptUtil.splitJoinCondition( join.getLeft(), join.getRight(), join.getCondition(), leftKeys, rightKeys, filterNulls );
        // If it contains non-equi join conditions, we bail out
        if ( !nonEquiConj.isAlwaysTrue() ) {
            return;
        }

        // Push each aggregate function down to each side that contains all of its arguments. Note that COUNT(*), because it has no arguments, can go to both sides.
        final Map<Integer, Integer> map = new HashMap<>();
        final List<Side> sides = new ArrayList<>();
        int uniqueCount = 0;
        int offset = 0;
        int belowOffset = 0;
        for ( int s = 0; s < 2; s++ ) {
            final Side side = new Side();
            final AlgNode joinInput = join.getInput( s );
            int fieldCount = joinInput.getTupleType().getFieldCount();
            final ImmutableBitSet fieldSet = ImmutableBitSet.range( offset, offset + fieldCount );
            final ImmutableBitSet belowAggregateKeyNotShifted = belowAggregateColumns.intersect( fieldSet );
            for ( Ord<Integer> c : Ord.zip( belowAggregateKeyNotShifted ) ) {
                map.put( c.e, belowOffset + c.i );
            }
            final Mappings.TargetMapping mapping = s == 0
                    ? Mappings.createIdentity( fieldCount )
                    : Mappings.createShiftMapping( fieldCount + offset, 0, offset, fieldCount );
            final ImmutableBitSet belowAggregateKey = belowAggregateKeyNotShifted.shift( -offset );
            final boolean unique;
            if ( !allowFunctions ) {
                assert aggregate.getAggCallList().isEmpty();
                // If there are no functions, it doesn't matter as much whether we aggregate the inputs before the join, because there will not be any functions experiencing a cartesian product effect.
                //
                // But finding out whether the input is already unique requires a call to areColumnsUnique that currently (until [POLYPHENYDB-1048] "Make metadata more robust" is fixed) places a heavy load on the metadata system.
                //
                // So we choose to imagine the input is already unique, which is untrue but harmless.
                //
                Util.discard( Bug.CALCITE_1048_FIXED );
                unique = true;
            } else {
                final Boolean unique0 = mq.areColumnsUnique( joinInput, belowAggregateKey );
                unique = unique0 != null && unique0;
            }
            if ( unique ) {
                ++uniqueCount;
                side.aggregate = false;
                algBuilder.push( joinInput );
                final List<RexNode> projects = new ArrayList<>();
                for ( Integer i : belowAggregateKey ) {
                    projects.add( algBuilder.field( i ) );
                }
                for ( Ord<AggregateCall> aggCall : Ord.zip( aggregate.getAggCallList() ) ) {
                    final AggFunction aggregation = aggCall.e.getAggregation();
                    final SplittableAggFunction splitter = aggregation.unwrap( SplittableAggFunction.class ).orElseThrow();
                    if ( !aggCall.e.getArgList().isEmpty() && fieldSet.contains( ImmutableBitSet.of( aggCall.e.getArgList() ) ) ) {
                        final RexNode singleton = splitter.singleton( rexBuilder, joinInput.getTupleType(), aggCall.e.transform( mapping ) );

                        if ( singleton instanceof RexIndexRef ) {
                            final int index = ((RexIndexRef) singleton).getIndex();
                            if ( !belowAggregateKey.get( index ) ) {
                                projects.add( singleton );
                            }
                            side.split.put( aggCall.i, index );
                        } else {
                            projects.add( singleton );
                            side.split.put( aggCall.i, projects.size() - 1 );
                        }
                    }
                }
                algBuilder.project( projects );
                side.newInput = algBuilder.build();
            } else {
                side.aggregate = true;
                List<AggregateCall> belowAggCalls = new ArrayList<>();
                final SplittableAggFunction.Registry<AggregateCall> belowAggCallRegistry = registry( belowAggCalls );
                final int oldGroupKeyCount = aggregate.getGroupCount();
                final int newGroupKeyCount = belowAggregateKey.cardinality();
                for ( Ord<AggregateCall> aggCall : Ord.zip( aggregate.getAggCallList() ) ) {
                    final AggFunction aggregation = aggCall.e.getAggregation();
                    final SplittableAggFunction splitter = aggregation.unwrap( SplittableAggFunction.class ).orElseThrow();
                    final AggregateCall call1;
                    if ( fieldSet.contains( ImmutableBitSet.of( aggCall.e.getArgList() ) ) ) {
                        final AggregateCall splitCall = splitter.split( aggCall.e, mapping );
                        call1 = splitCall.adaptTo( joinInput, splitCall.getArgList(), splitCall.filterArg, oldGroupKeyCount, newGroupKeyCount );
                    } else {
                        call1 = splitter.other( rexBuilder.getTypeFactory(), aggCall.e );
                    }
                    if ( call1 != null ) {
                        side.split.put( aggCall.i, belowAggregateKey.cardinality() + belowAggCallRegistry.register( call1 ) );
                    }
                }
                side.newInput = algBuilder.push( joinInput )
                        .aggregate( algBuilder.groupKey( belowAggregateKey ), belowAggCalls )
                        .build();
            }
            offset += fieldCount;
            belowOffset += side.newInput.getTupleType().getFieldCount();
            sides.add( side );
        }

        if ( uniqueCount == 2 ) {
            // Both inputs to the join are unique. There is nothing to be gained by this rule. In fact, this aggregate+join may be the result of a previous invocation of this rule; if we continue we might loop forever.
            return;
        }

        // Update condition
        final Mapping mapping = (Mapping) Mappings.target( map::get, join.getTupleType().getFieldCount(), belowOffset );
        final RexNode newCondition = RexUtil.apply( mapping, join.getCondition() );

        // Create new join
        algBuilder.push( sides.get( 0 ).newInput )
                .push( sides.get( 1 ).newInput )
                .join( join.getJoinType(), newCondition );

        // Aggregate above to sum up the sub-totals
        final List<AggregateCall> newAggCalls = new ArrayList<>();
        final int groupIndicatorCount = aggregate.getGroupCount() + aggregate.getIndicatorCount();
        final int newLeftWidth = sides.get( 0 ).newInput.getTupleType().getFieldCount();
        final List<RexNode> projects = new ArrayList<>( rexBuilder.identityProjects( algBuilder.peek().getTupleType() ) );
        for ( Ord<AggregateCall> aggCall : Ord.zip( aggregate.getAggCallList() ) ) {
            final AggFunction aggregation = aggCall.e.getAggregation();
            final SplittableAggFunction splitter = aggregation.unwrap( SplittableAggFunction.class ).orElseThrow();
            final Integer leftSubTotal = sides.get( 0 ).split.get( aggCall.i );
            final Integer rightSubTotal = sides.get( 1 ).split.get( aggCall.i );
            newAggCalls.add(
                    splitter.topSplit(
                            rexBuilder,
                            registry( projects ),
                            groupIndicatorCount,
                            algBuilder.peek().getTupleType(),
                            aggCall.e,
                            leftSubTotal == null ? -1 : leftSubTotal,
                            rightSubTotal == null ? -1 : rightSubTotal + newLeftWidth ) );
        }

        algBuilder.project( projects );

        boolean aggConvertedToProjects = false;
        if ( allColumnsInAggregate ) {
            // let's see if we can convert aggregate into projects
            List<RexNode> projects2 = new ArrayList<>();
            for ( int key : Mappings.apply( mapping, aggregate.getGroupSet() ) ) {
                projects2.add( algBuilder.field( key ) );
            }
            for ( AggregateCall newAggCall : newAggCalls ) {
                Optional<SplittableAggFunction> oSplitter = newAggCall.getAggregation().unwrap( SplittableAggFunction.class );
                if ( oSplitter.isPresent() ) {
                    final AlgDataType rowType = algBuilder.peek().getTupleType();
                    projects2.add( oSplitter.get().singleton( rexBuilder, rowType, newAggCall ) );
                }
            }
            if ( projects2.size() == aggregate.getGroupSet().cardinality() + newAggCalls.size() ) {
                // We successfully converted agg calls into projects.
                algBuilder.project( projects2 );
                aggConvertedToProjects = true;
            }
        }

        if ( !aggConvertedToProjects ) {
            algBuilder.aggregate(
                    algBuilder.groupKey( Mappings.apply( mapping, aggregate.getGroupSet() ), Mappings.apply2( mapping, aggregate.getGroupSets() ) ),
                    newAggCalls );
        }

        call.transformTo( algBuilder.build() );
    }


    /**
     * Computes the closure of a set of columns according to a given list of constraints. Each 'x = y' constraint causes bit y to be set if bit x is set, and vice versa.
     */
    private static ImmutableBitSet keyColumns( ImmutableBitSet aggregateColumns, ImmutableList<RexNode> predicates ) {
        SortedMap<Integer, BitSet> equivalence = new TreeMap<>();
        for ( RexNode predicate : predicates ) {
            populateEquivalences( equivalence, predicate );
        }
        ImmutableBitSet keyColumns = aggregateColumns;
        for ( Integer aggregateColumn : aggregateColumns ) {
            final BitSet bitSet = equivalence.get( aggregateColumn );
            if ( bitSet != null ) {
                keyColumns = keyColumns.union( bitSet );
            }
        }
        return keyColumns;
    }


    private static void populateEquivalences( Map<Integer, BitSet> equivalence, RexNode predicate ) {
        if ( Objects.requireNonNull( predicate.getKind() ) == Kind.EQUALS ) {
            RexCall call = (RexCall) predicate;
            final List<RexNode> operands = call.getOperands();
            if ( operands.get( 0 ) instanceof RexIndexRef ) {
                final RexIndexRef ref0 = (RexIndexRef) operands.get( 0 );
                if ( operands.get( 1 ) instanceof RexIndexRef ) {
                    final RexIndexRef ref1 = (RexIndexRef) operands.get( 1 );
                    populateEquivalence( equivalence, ref0.getIndex(), ref1.getIndex() );
                    populateEquivalence( equivalence, ref1.getIndex(), ref0.getIndex() );
                }
            }
        }
    }


    private static void populateEquivalence( Map<Integer, BitSet> equivalence, int i0, int i1 ) {
        BitSet bitSet = equivalence.get( i0 );
        if ( bitSet == null ) {
            bitSet = new BitSet();
            equivalence.put( i0, bitSet );
        }
        bitSet.set( i1 );
    }


    /**
     * Creates a {@link SplittableAggFunction.Registry} that is a view of a list.
     */
    private static <E> SplittableAggFunction.Registry<E> registry( final List<E> list ) {
        return e -> {
            int i = list.indexOf( e );
            if ( i < 0 ) {
                i = list.size();
                list.add( e );
            }
            return i;
        };
    }


    /**
     * Work space for an input to a join.
     */
    private static class Side {

        final Map<Integer, Integer> split = new HashMap<>();
        AlgNode newInput;
        boolean aggregate;

    }

}

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
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;


/**
 * Planner rule to flatten a tree of {@link LogicalRelJoin}s into a single {@link MultiJoin} with N inputs.
 *
 * An input is not flattened if the input is a null generating input in an outer join, i.e., either input in a full outer join, the right hand side of a left outer join, or the left hand
 * side of a right outer join.
 *
 * Join conditions are also pulled up from the inputs into the topmost {@link MultiJoin}, unless the input corresponds to a null generating input in an outer join,
 *
 * Outer join information is also stored in the {@link MultiJoin}. A boolean flag indicates if the join is a full outer join, and in the case of left and right outer joins,
 * the join type and outer join conditions are stored in arrays in the {@link MultiJoin}. This outer join information is associated with the null generating input in the outer join.
 * So, in the case of a a left outer join between A and B, the information is associated with B, not A.
 *
 * Here are examples of the {@link MultiJoin}s constructed after this rule has been applied on following join trees.
 *
 * <ul>
 * <li>A JOIN B &rarr; MJ(A, B)</li>
 * <li>A JOIN B JOIN C &rarr; MJ(A, B, C)</li>
 * <li>A LEFT JOIN B &rarr; MJ(A, B), left outer join on input#1</li>
 * <li>A RIGHT JOIN B &rarr; MJ(A, B), right outer join on input#0</li>
 * <li>A FULL JOIN B &rarr; MJ[full](A, B)</li>
 * <li>A LEFT JOIN (B JOIN C) &rarr; MJ(A, MJ(B, C))), left outer join on input#1 in the outermost MultiJoin</li>
 * <li>(A JOIN B) LEFT JOIN C &rarr; MJ(A, B, C), left outer join on input#2</li>
 * <li>(A LEFT JOIN B) JOIN C &rarr; MJ(MJ(A, B), C), left outer join on input#1 of the inner MultiJoin</li>        TODO
 * <li>A LEFT JOIN (B FULL JOIN C) &rarr; MJ(A, MJ[full](B, C)), left outer join on input#1 in the outermost MultiJoin</li>
 * <li>(A LEFT JOIN B) FULL JOIN (C RIGHT JOIN D) &rarr; MJ[full](MJ(A, B), MJ(C, D)), left outer join on input #1 in the first inner MultiJoin and right outer join on input#0 in the second inner MultiJoin</li>
 * </ul>
 *
 * The constructor is parameterized to allow any sub-class of {@link org.polypheny.db.algebra.core.Join}, not just {@link LogicalRelJoin}.
 *
 * @see org.polypheny.db.algebra.rules.FilterMultiJoinMergeRule
 * @see ProjectMultiJoinMergeRule
 */
public class JoinToMultiJoinRule extends AlgOptRule {

    public static final JoinToMultiJoinRule INSTANCE = new JoinToMultiJoinRule( LogicalRelJoin.class, AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a JoinToMultiJoinRule.
     */
    public JoinToMultiJoinRule( Class<? extends Join> clazz, AlgBuilderFactory algBuilderFactory ) {
        super(
                operand( clazz, operand( AlgNode.class, any() ), operand( AlgNode.class, any() ) ),
                algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Join origJoin = call.alg( 0 );
        final AlgNode left = call.alg( 1 );
        final AlgNode right = call.alg( 2 );

        // combine the children MultiJoin inputs into an array of inputs for the new MultiJoin
        final List<ImmutableBitSet> projFieldsList = new ArrayList<>();
        final List<int[]> joinFieldRefCountsList = new ArrayList<>();
        final List<AlgNode> newInputs =
                combineInputs(
                        origJoin,
                        left,
                        right,
                        projFieldsList,
                        joinFieldRefCountsList );

        // combine the outer join information from the left and right inputs, and include the outer join information from the current join, if it's a left/right outer join
        final List<Pair<JoinAlgType, RexNode>> joinSpecs = new ArrayList<>();
        combineOuterJoins( origJoin, newInputs, left, right, joinSpecs );

        // pull up the join filters from the children MultiJoinRels and combine them with the join filter associated with this LogicalJoin to form the join filter for the new MultiJoin
        List<RexNode> newJoinFilters = combineJoinFilters( origJoin, left, right );

        // add on the join field reference counts for the join condition associated with this LogicalJoin
        final ImmutableMap<Integer, ImmutableList<Integer>> newJoinFieldRefCountsMap =
                addOnJoinFieldRefCounts(
                        newInputs,
                        origJoin.getTupleType().getFieldCount(),
                        origJoin.getCondition(),
                        joinFieldRefCountsList );

        List<RexNode> newPostJoinFilters = combinePostJoinFilters( origJoin, left, right );

        final RexBuilder rexBuilder = origJoin.getCluster().getRexBuilder();
        AlgNode multiJoin =
                new MultiJoin(
                        origJoin.getCluster(),
                        newInputs,
                        RexUtil.composeConjunction( rexBuilder, newJoinFilters ),
                        origJoin.getTupleType(),
                        origJoin.getJoinType() == JoinAlgType.FULL,
                        Pair.right( joinSpecs ),
                        Pair.left( joinSpecs ),
                        projFieldsList,
                        newJoinFieldRefCountsMap,
                        RexUtil.composeConjunction( rexBuilder, newPostJoinFilters, true ) );

        call.transformTo( multiJoin );
    }


    /**
     * Combines the inputs into a LogicalJoin into an array of inputs.
     *
     * @param join original join
     * @param left left input into join
     * @param right right input into join
     * @param projFieldsList returns a list of the new combined projection fields
     * @param joinFieldRefCountsList returns a list of the new combined join field reference counts
     * @return combined left and right inputs in an array
     */
    private List<AlgNode> combineInputs( Join join, AlgNode left, AlgNode right, List<ImmutableBitSet> projFieldsList, List<int[]> joinFieldRefCountsList ) {
        final List<AlgNode> newInputs = new ArrayList<>();

        // leave the null generating sides of an outer join intact; don't pull up those children inputs into the array we're constructing
        if ( canCombine( left, join.getJoinType().generatesNullsOnLeft() ) ) {
            final MultiJoin leftMultiJoin = (MultiJoin) left;
            for ( int i = 0; i < left.getInputs().size(); i++ ) {
                newInputs.add( leftMultiJoin.getInput( i ) );
                projFieldsList.add( leftMultiJoin.getProjFields().get( i ) );
                joinFieldRefCountsList.add( leftMultiJoin.getJoinFieldRefCountsMap().get( i ).stream().mapToInt( j -> j ).toArray() );
            }
        } else {
            newInputs.add( left );
            projFieldsList.add( null );
            joinFieldRefCountsList.add( new int[left.getTupleType().getFieldCount()] );
        }

        if ( canCombine( right, join.getJoinType().generatesNullsOnRight() ) ) {
            final MultiJoin rightMultiJoin = (MultiJoin) right;
            for ( int i = 0; i < right.getInputs().size(); i++ ) {
                newInputs.add( rightMultiJoin.getInput( i ) );
                projFieldsList.add( rightMultiJoin.getProjFields().get( i ) );
                joinFieldRefCountsList.add( rightMultiJoin.getJoinFieldRefCountsMap().get( i ).stream().mapToInt( j -> j ).toArray() );
            }
        } else {
            newInputs.add( right );
            projFieldsList.add( null );
            joinFieldRefCountsList.add( new int[right.getTupleType().getFieldCount()] );
        }

        return newInputs;
    }


    /**
     * Combines the outer join conditions and join types from the left and right join inputs. If the join itself is either a left or right outer join, then the join condition corresponding to the join is also set in the
     * position corresponding to the null-generating input into the join. The join type is also set.
     *
     * @param joinRel join rel
     * @param combinedInputs the combined inputs to the join
     * @param left left child of the joinrel
     * @param right right child of the joinrel
     * @param joinSpecs the list where the join types and conditions will be copied
     */
    private void combineOuterJoins( Join joinRel, List<AlgNode> combinedInputs, AlgNode left, AlgNode right, List<Pair<JoinAlgType, RexNode>> joinSpecs ) {
        JoinAlgType joinType = joinRel.getJoinType();
        boolean leftCombined = canCombine( left, joinType.generatesNullsOnLeft() );
        boolean rightCombined = canCombine( right, joinType.generatesNullsOnRight() );
        switch ( joinType ) {
            case LEFT:
                if ( leftCombined ) {
                    copyOuterJoinInfo(
                            (MultiJoin) left,
                            joinSpecs,
                            0,
                            null,
                            null );
                } else {
                    joinSpecs.add( Pair.of( JoinAlgType.INNER, (RexNode) null ) );
                }
                joinSpecs.add( Pair.of( joinType, joinRel.getCondition() ) );
                break;
            case RIGHT:
                joinSpecs.add( Pair.of( joinType, joinRel.getCondition() ) );
                if ( rightCombined ) {
                    copyOuterJoinInfo(
                            (MultiJoin) right,
                            joinSpecs,
                            left.getTupleType().getFieldCount(),
                            right.getTupleType().getFields(),
                            joinRel.getTupleType().getFields() );
                } else {
                    joinSpecs.add( Pair.of( JoinAlgType.INNER, (RexNode) null ) );
                }
                break;
            default:
                if ( leftCombined ) {
                    copyOuterJoinInfo(
                            (MultiJoin) left,
                            joinSpecs,
                            0,
                            null,
                            null );
                } else {
                    joinSpecs.add( Pair.of( JoinAlgType.INNER, (RexNode) null ) );
                }
                if ( rightCombined ) {
                    copyOuterJoinInfo(
                            (MultiJoin) right,
                            joinSpecs,
                            left.getTupleType().getFieldCount(),
                            right.getTupleType().getFields(),
                            joinRel.getTupleType().getFields() );
                } else {
                    joinSpecs.add( Pair.of( JoinAlgType.INNER, (RexNode) null ) );
                }
        }
    }


    /**
     * Copies outer join data from a source MultiJoin to a new set of arrays. Also adjusts the conditions to reflect the new position of an input if that input ends up being shifted to the right.
     *
     * @param multiJoin the source MultiJoin
     * @param destJoinSpecs the list where the join types and conditions will be copied
     * @param adjustmentAmount if &gt; 0, the amount the RexInputRefs in the join conditions need to be adjusted by
     * @param srcFields the source fields that the original join conditions are referencing
     * @param destFields the destination fields that the new join conditions
     */
    private void copyOuterJoinInfo( MultiJoin multiJoin, List<Pair<JoinAlgType, RexNode>> destJoinSpecs, int adjustmentAmount, List<AlgDataTypeField> srcFields, List<AlgDataTypeField> destFields ) {
        final List<Pair<JoinAlgType, RexNode>> srcJoinSpecs = Pair.zip( multiJoin.getJoinTypes(), multiJoin.getOuterJoinConditions() );

        if ( adjustmentAmount == 0 ) {
            destJoinSpecs.addAll( srcJoinSpecs );
        } else {
            assert srcFields != null;
            assert destFields != null;
            int nFields = srcFields.size();
            int[] adjustments = new int[nFields];
            for ( int idx = 0; idx < nFields; idx++ ) {
                adjustments[idx] = adjustmentAmount;
            }
            for ( Pair<JoinAlgType, RexNode> src : srcJoinSpecs ) {
                destJoinSpecs.add(
                        Pair.of(
                                src.left,
                                src.right == null
                                        ? null
                                        : src.right.accept(
                                                new AlgOptUtil.RexInputConverter(
                                                        multiJoin.getCluster().getRexBuilder(),
                                                        srcFields, destFields, adjustments ) ) ) );
            }
        }
    }


    /**
     * Combines the join filters from the left and right inputs (if they are MultiJoinRels) with the join filter in the joinrel into a single AND'd join filter, unless the inputs correspond to null
     * generating inputs in an outer join
     *
     * @param joinRel join rel
     * @param left left child of the join
     * @param right right child of the join
     * @return combined join filters AND-ed together
     */
    private List<RexNode> combineJoinFilters( Join joinRel, AlgNode left, AlgNode right ) {
        JoinAlgType joinType = joinRel.getJoinType();

        // AND the join condition if this isn't a left or right outer join; in those cases, the outer join condition is already tracked separately
        final List<RexNode> filters = new ArrayList<>();
        if ( (joinType != JoinAlgType.LEFT) && (joinType != JoinAlgType.RIGHT) ) {
            filters.add( joinRel.getCondition() );
        }
        if ( canCombine( left, joinType.generatesNullsOnLeft() ) ) {
            filters.add( ((MultiJoin) left).getJoinFilter() );
        }
        // Need to adjust the RexInputs of the right child, since those need to shift over to the right
        if ( canCombine( right, joinType.generatesNullsOnRight() ) ) {
            MultiJoin multiJoin = (MultiJoin) right;
            filters.add( shiftRightFilter( joinRel, left, multiJoin, multiJoin.getJoinFilter() ) );
        }

        return filters;
    }


    /**
     * Returns whether an input can be merged into a given relational expression without changing semantics.
     *
     * @param input input into a join
     * @param nullGenerating true if the input is null generating
     * @return true if the input can be combined into a parent MultiJoin
     */
    private boolean canCombine( AlgNode input, boolean nullGenerating ) {
        return input instanceof MultiJoin
                && !((MultiJoin) input).isFullOuterJoin()
                && !((MultiJoin) input).containsOuter()
                && !nullGenerating;
    }


    /**
     * Shifts a filter originating from the right child of the LogicalJoin to the right, to reflect the filter now being applied on the resulting MultiJoin.
     *
     * @param joinRel the original LogicalJoin
     * @param left the left child of the LogicalJoin
     * @param right the right child of the LogicalJoin
     * @param rightFilter the filter originating from the right child
     * @return the adjusted right filter
     */
    private RexNode shiftRightFilter( Join joinRel, AlgNode left, MultiJoin right, RexNode rightFilter ) {
        if ( rightFilter == null ) {
            return null;
        }

        int nFieldsOnLeft = left.getTupleType().getFields().size();
        int nFieldsOnRight = right.getTupleType().getFields().size();
        int[] adjustments = new int[nFieldsOnRight];
        for ( int i = 0; i < nFieldsOnRight; i++ ) {
            adjustments[i] = nFieldsOnLeft;
        }
        rightFilter =
                rightFilter.accept(
                        new AlgOptUtil.RexInputConverter(
                                joinRel.getCluster().getRexBuilder(),
                                right.getTupleType().getFields(),
                                joinRel.getTupleType().getFields(),
                                adjustments ) );
        return rightFilter;
    }


    /**
     * Adds on to the existing join condition reference counts the references from the new join condition.
     *
     * @param multiJoinInputs inputs into the new MultiJoin
     * @param nTotalFields total number of fields in the MultiJoin
     * @param joinCondition the new join condition
     * @param origJoinFieldRefCounts existing join condition reference counts
     * @return Map containing the new join condition
     */
    private ImmutableMap<Integer, ImmutableList<Integer>> addOnJoinFieldRefCounts( List<AlgNode> multiJoinInputs, int nTotalFields, RexNode joinCondition, List<int[]> origJoinFieldRefCounts ) {
        // count the input references in the join condition
        int[] joinCondRefCounts = new int[nTotalFields];
        joinCondition.accept( new InputReferenceCounter( joinCondRefCounts ) );

        // first, make a copy of the ref counters
        final Map<Integer, int[]> refCountsMap = new HashMap<>();
        int nInputs = multiJoinInputs.size();
        int currInput = 0;
        for ( int[] origRefCounts : origJoinFieldRefCounts ) {
            refCountsMap.put( currInput, origRefCounts.clone() );
            currInput++;
        }

        // add on to the counts for each input into the MultiJoin the reference counts computed for the current join condition
        currInput = -1;
        int startField = 0;
        int nFields = 0;
        for ( int i = 0; i < nTotalFields; i++ ) {
            if ( joinCondRefCounts[i] == 0 ) {
                continue;
            }
            while ( i >= (startField + nFields) ) {
                startField += nFields;
                currInput++;
                assert currInput < nInputs;
                nFields = multiJoinInputs.get( currInput ).getTupleType().getFieldCount();
            }
            int[] refCounts = refCountsMap.get( currInput );
            refCounts[i - startField] += joinCondRefCounts[i];
        }

        final ImmutableMap.Builder<Integer, ImmutableList<Integer>> builder = ImmutableMap.builder();
        for ( Map.Entry<Integer, int[]> entry : refCountsMap.entrySet() ) {
            builder.put( entry.getKey(), ImmutableList.copyOf( Arrays.stream( entry.getValue() ).boxed().collect( Collectors.toList() ) ) );
        }
        return builder.build();
    }


    /**
     * Combines the post-join filters from the left and right inputs (if they are MultiJoinRels) into a single AND'd filter.
     *
     * @param joinRel the original LogicalJoin
     * @param left left child of the LogicalJoin
     * @param right right child of the LogicalJoin
     * @return combined post-join filters AND'd together
     */
    private List<RexNode> combinePostJoinFilters( Join joinRel, AlgNode left, AlgNode right ) {
        final List<RexNode> filters = new ArrayList<>();
        if ( right instanceof MultiJoin ) {
            final MultiJoin multiRight = (MultiJoin) right;
            filters.add( shiftRightFilter( joinRel, left, multiRight, multiRight.getPostJoinFilter() ) );
        }

        if ( left instanceof MultiJoin ) {
            filters.add( ((MultiJoin) left).getPostJoinFilter() );
        }

        return filters;
    }


    /**
     * Visitor that keeps a reference count of the inputs used by an expression.
     */
    private class InputReferenceCounter extends RexVisitorImpl<Void> {

        private final int[] refCounts;


        InputReferenceCounter( int[] refCounts ) {
            super( true );
            this.refCounts = refCounts;
        }


        @Override
        public Void visitIndexRef( RexIndexRef inputRef ) {
            refCounts[inputRef.getIndex()]++;
            return null;
        }

    }

}


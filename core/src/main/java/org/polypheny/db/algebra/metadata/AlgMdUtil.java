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

package org.polypheny.db.algebra.metadata;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Minus;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ImmutableBitSet.Builder;
import org.polypheny.db.util.NumberUtil;


/**
 * {@link AlgMdUtil} provides utility methods used by the metadata provider methods.
 */
public class AlgMdUtil {

    public static final Operator ARTIFICIAL_SELECTIVITY_FUNC = new ArtificialSelectivityOperator();


    private AlgMdUtil() {
    }


    /**
     * Creates a RexNode that stores a selectivity value corresponding to the selectivity of a semijoin. This can be added to a filter to simulate the effect of the semijoin during costing,
     * but should never appear in a real plan since it has no physical implementation.
     *
     * @param alg the semijoin of interest
     * @return constructed rexnode
     */
    public static RexNode makeSemiJoinSelectivityRexNode( AlgMetadataQuery mq, SemiJoin alg ) {
        RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        double selectivity = computeSemiJoinSelectivity( mq, alg.getLeft(), alg.getRight(), alg );
        return rexBuilder.makeCall( ARTIFICIAL_SELECTIVITY_FUNC, rexBuilder.makeApproxLiteral( new BigDecimal( selectivity ) ) );
    }


    /**
     * Returns the selectivity value stored in a call.
     *
     * @param artificialSelectivityFuncNode Call containing the selectivity value
     * @return selectivity value
     */
    public static double getSelectivityValue( RexNode artificialSelectivityFuncNode ) {
        assert artificialSelectivityFuncNode instanceof RexCall;
        RexCall call = (RexCall) artificialSelectivityFuncNode;
        assert call.getOperator().equals( ARTIFICIAL_SELECTIVITY_FUNC );
        RexNode operand = call.getOperands().get( 0 );
        return ((RexLiteral) operand).value.asDouble().value;//.getValue( Double.class );
    }


    /**
     * Computes the selectivity of a semijoin filter if it is applied on a fact table. The computation is based on the selectivity of the dimension table/columns and the
     * number of distinct values in the fact table columns.
     *
     * @param alg semijoin rel
     * @return calculated selectivity
     */
    public static double computeSemiJoinSelectivity( AlgMetadataQuery mq, SemiJoin alg ) {
        return computeSemiJoinSelectivity( mq, alg.getLeft(), alg.getRight(), alg.getLeftKeys(), alg.getRightKeys() );
    }


    /**
     * Computes the selectivity of a semijoin filter if it is applied on a fact table. The computation is based on the selectivity of the dimension table/columns and the
     * number of distinct values in the fact table columns.
     *
     * @param factRel fact table participating in the semijoin
     * @param dimRel dimension table participating in the semijoin
     * @param alg semijoin rel
     * @return calculated selectivity
     */
    public static double computeSemiJoinSelectivity( AlgMetadataQuery mq, AlgNode factRel, AlgNode dimRel, SemiJoin alg ) {
        return computeSemiJoinSelectivity( mq, factRel, dimRel, alg.getLeftKeys(), alg.getRightKeys() );
    }


    /**
     * Computes the selectivity of a semijoin filter if it is applied on a fact table. The computation is based on the selectivity of the dimension table/columns and the
     * number of distinct values in the fact table columns.
     *
     * @param factRel fact table participating in the semijoin
     * @param dimRel dimension table participating in the semijoin
     * @param factKeyList LHS keys used in the filter
     * @param dimKeyList RHS keys used in the filter
     * @return calculated selectivity
     */
    public static double computeSemiJoinSelectivity( AlgMetadataQuery mq, AlgNode factRel, AlgNode dimRel, List<Integer> factKeyList, List<Integer> dimKeyList ) {
        Builder factKeys = ImmutableBitSet.builder();
        for ( int factCol : factKeyList ) {
            factKeys.set( factCol );
        }
        ImmutableBitSet.Builder dimKeyBuilder = ImmutableBitSet.builder();
        for ( int dimCol : dimKeyList ) {
            dimKeyBuilder.set( dimCol );
        }
        final ImmutableBitSet dimKeys = dimKeyBuilder.build();

        Double factPop = mq.getPopulationSize( factRel, factKeys.build() );
        if ( factPop == null ) {
            // use the dimension population if the fact population is unavailable; since we're filtering the fact table, that's
            // the population we ideally want to use
            factPop = mq.getPopulationSize( dimRel, dimKeys );
        }

        // if cardinality and population are available, use them; otherwise use percentage original rows
        Double selectivity;
        Double dimCard = mq.getDistinctRowCount( dimRel, dimKeys, null );
        if ( (dimCard != null) && (factPop != null) ) {
            // to avoid division by zero
            if ( factPop < 1.0 ) {
                factPop = 1.0;
            }
            selectivity = dimCard / factPop;
        } else {
            selectivity = mq.getPercentageOriginalRows( dimRel );
        }

        if ( selectivity == null ) {
            // set a default selectivity based on the number of semijoin keys
            selectivity = Math.pow( 0.1, dimKeys.cardinality() );
        } else if ( selectivity > 1.0 ) {
            selectivity = 1.0;
        }
        return selectivity;
    }


    /**
     * Returns true if the columns represented in a bit mask are definitely known to form a unique column set.
     *
     * @param alg the relational expression that the column mask corresponds to
     * @param colMask bit mask containing columns that will be tested for uniqueness
     * @return true if bit mask represents a unique column set; false if not (or if no metadata is available)
     */
    public static boolean areColumnsDefinitelyUnique( AlgMetadataQuery mq, AlgNode alg, ImmutableBitSet colMask ) {
        Boolean b = mq.areColumnsUnique( alg, colMask, false );
        return b != null && b;
    }


    public static Boolean areColumnsUnique( AlgMetadataQuery mq, AlgNode alg, List<RexIndexRef> columnRefs ) {
        ImmutableBitSet.Builder colMask = ImmutableBitSet.builder();
        for ( RexIndexRef columnRef : columnRefs ) {
            colMask.set( columnRef.getIndex() );
        }
        return mq.areColumnsUnique( alg, colMask.build() );
    }


    public static boolean areColumnsDefinitelyUnique( AlgMetadataQuery mq, AlgNode alg, List<RexIndexRef> columnRefs ) {
        Boolean b = areColumnsUnique( mq, alg, columnRefs );
        return b != null && b;
    }


    /**
     * Returns true if the columns represented in a bit mask are definitely known to form a unique column set, when nulls have been filtered from the columns.
     *
     * @param alg the relational expression that the column mask corresponds to
     * @param colMask bit mask containing columns that will be tested for uniqueness
     * @return true if bit mask represents a unique column set; false if not (or if no metadata is available)
     */
    public static boolean areColumnsDefinitelyUniqueWhenNullsFiltered( AlgMetadataQuery mq, AlgNode alg, ImmutableBitSet colMask ) {
        Boolean b = mq.areColumnsUnique( alg, colMask, true );
        if ( b == null ) {
            return false;
        }
        return b;
    }


    public static Boolean areColumnsUniqueWhenNullsFiltered( AlgMetadataQuery mq, AlgNode alg, List<RexIndexRef> columnRefs ) {
        ImmutableBitSet.Builder colMask = ImmutableBitSet.builder();

        for ( RexIndexRef columnRef : columnRefs ) {
            colMask.set( columnRef.getIndex() );
        }

        return mq.areColumnsUnique( alg, colMask.build(), true );
    }


    public static boolean areColumnsDefinitelyUniqueWhenNullsFiltered( AlgMetadataQuery mq, AlgNode alg, List<RexIndexRef> columnRefs ) {
        Boolean b = areColumnsUniqueWhenNullsFiltered( mq, alg, columnRefs );
        if ( b == null ) {
            return false;
        }
        return b;
    }


    /**
     * Separates a bit-mask representing a join into masks representing the left and right inputs into the join.
     *
     * @param groupKey original bit-mask
     * @param leftMask left bit-mask to be set
     * @param rightMask right bit-mask to be set
     * @param nFieldsOnLeft number of fields in the left input
     */
    public static void setLeftRightBitmaps( ImmutableBitSet groupKey, ImmutableBitSet.Builder leftMask, ImmutableBitSet.Builder rightMask, int nFieldsOnLeft ) {
        for ( int bit : groupKey ) {
            if ( bit < nFieldsOnLeft ) {
                leftMask.set( bit );
            } else {
                rightMask.set( bit - nFieldsOnLeft );
            }
        }
    }


    /**
     * Returns the number of distinct values provided numSelected are selected where there are domainSize distinct values.
     *
     * Note that in the case where domainSize == numSelected, it's not true that the return value should be domainSize. If you pick 100 random values between 1 and 100, you'll most likely end up with fewer than 100 distinct
     * values, because you'll pick some values more than once.
     *
     * @param domainSize number of distinct values in the domain
     * @param numSelected number selected from the domain
     * @return number of distinct values for subset selected
     */
    public static Double numDistinctVals( Double domainSize, Double numSelected ) {
        if ( (domainSize == null) || (numSelected == null) ) {
            return null;
        }

        // Cap the input sizes at MAX_VALUE to ensure that the calculations using these values return meaningful values
        double dSize = capInfinity( domainSize );
        double numSel = capInfinity( numSelected );

        // The formula for this is:
        // 1. Assume we pick 80 random values between 1 and 100.
        // 2. The chance we skip any given value is .99 ^ 80
        // 3. Thus, on average we will skip .99 ^ 80 percent of the values in the domain
        // 4. Generalized, we skip ( (n-1)/n ) ^ k values where n is the number of possible values and k is the number we are selecting
        // 5. This can be rewritten via approximation (if you want to know why approximation is called for here, ask Bill Keese):
        //  ((n-1)/n) ^ k
        //  = e ^ ln( ((n-1)/n) ^ k )
        //  = e ^ (k * ln ((n-1)/n))
        //  = e ^ (k * ln (1-1/n))
        // ~= e ^ (k * (-1/n))  because ln(1+x) ~= x for small x
        //  = e ^ (-k/n)
        // 6. Flipping it from number skipped to number visited, we get:
        double res = (dSize > 0) ? ((1.0 - Math.exp( -1 * numSel / dSize )) * dSize) : 0;

        // fix the boundary cases
        if ( res > dSize ) {
            res = dSize;
        }

        if ( res > numSel ) {
            res = numSel;
        }

        if ( res < 0 ) {
            res = 0;
        }

        return res;
    }


    /**
     * Caps a double value at Double.MAX_VALUE if it's currently infinity
     *
     * @param d the Double object
     * @return the double value if it's not infinity; else Double.MAX_VALUE
     */
    public static double capInfinity( Double d ) {
        return d.isInfinite() ? Double.MAX_VALUE : d;
    }


    /**
     * Returns default estimates for selectivities, in the absence of stats.
     *
     * @param predicate predicate for which selectivity will be computed; null means true, so gives selectity of 1.0
     * @return estimated selectivity
     */
    public static double guessSelectivity( RexNode predicate ) {
        return guessSelectivity( predicate, false );
    }


    /**
     * Returns default estimates for selectivities, in the absence of stats.
     *
     * @param predicate predicate for which selectivity will be computed; null means true, so gives selectity of 1.0
     * @param artificialOnly return only the selectivity contribution from artificial nodes
     * @return estimated selectivity
     */
    public static double guessSelectivity( RexNode predicate, boolean artificialOnly ) {
        double sel = 1.0;
        if ( (predicate == null) || predicate.isAlwaysTrue() ) {
            return sel;
        }

        double artificialSel = 1.0;

        for ( RexNode pred : AlgOptUtil.conjunctions( predicate ) ) {
            if ( pred.getKind() == Kind.IS_NOT_NULL ) {
                sel *= .9;
            } else if ( (pred instanceof RexCall) && (((RexCall) pred).getOperator().equals( AlgMdUtil.ARTIFICIAL_SELECTIVITY_FUNC )) ) {
                artificialSel *= AlgMdUtil.getSelectivityValue( pred );
            } else if ( pred.isA( Kind.EQUALS ) ) {
                sel *= .15;
            } else if ( pred.isA( Kind.COMPARISON ) ) {
                sel *= .5;
            } else {

                sel *= .25;
            }
        }

        if ( artificialOnly ) {
            return artificialSel;
        } else {
            return sel * artificialSel;
        }
    }


    /**
     * AND's two predicates together, either of which may be null, removing redundant filters.
     *
     * @param rexBuilder rexBuilder used to construct AND'd RexNode
     * @param pred1 first predicate
     * @param pred2 second predicate
     * @return AND'd predicate or individual predicates if one is null
     */
    public static RexNode unionPreds( RexBuilder rexBuilder, RexNode pred1, RexNode pred2 ) {
        final Set<RexNode> unionList = new LinkedHashSet<>();
        unionList.addAll( AlgOptUtil.conjunctions( pred1 ) );
        unionList.addAll( AlgOptUtil.conjunctions( pred2 ) );
        return RexUtil.composeConjunction( rexBuilder, unionList, true );
    }


    /**
     * Takes the difference between two predicates, removing from the first any predicates also in the second
     *
     * @param rexBuilder rexBuilder used to construct AND'd RexNode
     * @param pred1 first predicate
     * @param pred2 second predicate
     * @return MINUS'd predicate list
     */
    public static RexNode minusPreds( RexBuilder rexBuilder, RexNode pred1, RexNode pred2 ) {
        final List<RexNode> minusList = new ArrayList<>( AlgOptUtil.conjunctions( pred1 ) );
        minusList.removeAll( AlgOptUtil.conjunctions( pred2 ) );
        return RexUtil.composeConjunction( rexBuilder, minusList, true );
    }


    /**
     * Takes a bitmap representing a set of input references and extracts the ones that reference the group by columns in an aggregate.
     *
     * @param groupKey the original bitmap
     * @param aggRel the aggregate
     * @param childKey sets bits from groupKey corresponding to group by columns
     */
    public static void setAggChildKeys( ImmutableBitSet groupKey, Aggregate aggRel, ImmutableBitSet.Builder childKey ) {
        List<AggregateCall> aggCalls = aggRel.getAggCallList();
        for ( int bit : groupKey ) {
            if ( bit < aggRel.getGroupCount() ) {
                // group by column
                childKey.set( bit );
            } else {
                // aggregate column -- set a bit for each argument being aggregated
                AggregateCall agg = aggCalls.get( bit - (aggRel.getGroupCount() + aggRel.getIndicatorCount()) );
                for ( Integer arg : agg.getArgList() ) {
                    childKey.set( arg );
                }
            }
        }
    }


    /**
     * Forms two bitmaps by splitting the columns in a bitmap according to whether or not the column references the child input or is an expression
     *
     * @param projExprs Project expressions
     * @param groupKey Bitmap whose columns will be split
     * @param baseCols Bitmap representing columns from the child input
     * @param projCols Bitmap representing non-child columns
     */
    public static void splitCols( List<RexNode> projExprs, ImmutableBitSet groupKey, ImmutableBitSet.Builder baseCols, ImmutableBitSet.Builder projCols ) {
        for ( int bit : groupKey ) {
            final RexNode e = projExprs.get( bit );
            if ( e instanceof RexIndexRef ) {
                baseCols.set( ((RexIndexRef) e).getIndex() );
            } else {
                projCols.set( bit );
            }
        }
    }


    /**
     * Computes the cardinality of a particular expression from the projection list.
     *
     * @param alg {@link AlgNode} corresponding to the project
     * @param expr projection expression
     * @return cardinality
     */
    public static Double cardOfProjExpr( AlgMetadataQuery mq, Project alg, RexNode expr ) {
        return expr.accept( new CardOfProjExpr( mq, alg ) );
    }


    /**
     * Computes the population size for a set of keys returned from a join
     *
     * @param joinRel the join rel
     * @param groupKey keys to compute the population for
     * @return computed population size
     */
    public static Double getJoinPopulationSize( AlgMetadataQuery mq, AlgNode joinRel, ImmutableBitSet groupKey ) {
        ImmutableBitSet.Builder leftMask = ImmutableBitSet.builder();
        ImmutableBitSet.Builder rightMask = ImmutableBitSet.builder();
        AlgNode left = joinRel.getInputs().get( 0 );
        AlgNode right = joinRel.getInputs().get( 1 );

        // separate the mask into masks for the left and right
        AlgMdUtil.setLeftRightBitmaps( groupKey, leftMask, rightMask, left.getTupleType().getFieldCount() );

        Double population =
                NumberUtil.multiply(
                        mq.getPopulationSize( left, leftMask.build() ),
                        mq.getPopulationSize( right, rightMask.build() ) );

        return numDistinctVals( population, mq.getTupleCount( joinRel ) );
    }


    /**
     * Computes the number of distinct rows for a set of keys returned from a join. Also known as NDV (number of distinct values).
     *
     * @param joinRel {@link AlgNode} representing the join
     * @param joinType type of join
     * @param groupKey keys that the distinct row count will be computed for
     * @param predicate join predicate
     * @param useMaxNdv If true use formula <code>max(left NDV, right NDV)</code>, otherwise use <code>left NDV * right NDV</code>.
     * @return number of distinct rows
     */
    public static Double getJoinDistinctRowCount( AlgMetadataQuery mq, AlgNode joinRel, JoinAlgType joinType, ImmutableBitSet groupKey, RexNode predicate, boolean useMaxNdv ) {
        Double distRowCount;
        ImmutableBitSet.Builder leftMask = ImmutableBitSet.builder();
        ImmutableBitSet.Builder rightMask = ImmutableBitSet.builder();
        AlgNode left = joinRel.getInputs().get( 0 );
        AlgNode right = joinRel.getInputs().get( 1 );

        AlgMdUtil.setLeftRightBitmaps( groupKey, leftMask, rightMask, left.getTupleType().getFieldCount() );

        // determine which filters apply to the left vs right
        RexNode leftPred = null;
        RexNode rightPred = null;
        if ( predicate != null ) {
            final List<RexNode> leftFilters = new ArrayList<>();
            final List<RexNode> rightFilters = new ArrayList<>();
            final List<RexNode> joinFilters = new ArrayList<>();
            final List<RexNode> predList = AlgOptUtil.conjunctions( predicate );

            AlgOptUtil.classifyFilters(
                    joinRel,
                    predList,
                    joinType,
                    joinType == JoinAlgType.INNER,
                    !joinType.generatesNullsOnLeft(),
                    !joinType.generatesNullsOnRight(),
                    joinFilters,
                    leftFilters,
                    rightFilters );

            RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
            leftPred = RexUtil.composeConjunction( rexBuilder, leftFilters, true );
            rightPred = RexUtil.composeConjunction( rexBuilder, rightFilters, true );
        }

        if ( useMaxNdv ) {
            distRowCount = Math.max(
                    mq.getDistinctRowCount( left, leftMask.build(), leftPred ),
                    mq.getDistinctRowCount( right, rightMask.build(), rightPred ) );
        } else {
            distRowCount =
                    NumberUtil.multiply(
                            mq.getDistinctRowCount( left, leftMask.build(), leftPred ),
                            mq.getDistinctRowCount( right, rightMask.build(), rightPred ) );
        }

        return AlgMdUtil.numDistinctVals( distRowCount, mq.getTupleCount( joinRel ) );
    }


    /**
     * Returns an estimate of the number of rows returned by a {@link Union} (before duplicates are eliminated).
     */
    public static double getUnionAllRowCount( AlgMetadataQuery mq, Union alg ) {
        double rowCount = 0;
        for ( AlgNode input : alg.getInputs() ) {
            rowCount += mq.getTupleCount( input );
        }
        return rowCount;
    }


    /**
     * Returns an estimate of the number of rows returned by a {@link Minus}.
     */
    public static double getMinusRowCount( AlgMetadataQuery mq, Minus minus ) {
        // REVIEW jvs:  I just pulled this out of a hat.
        final List<AlgNode> inputs = minus.getInputs();
        double dRows = mq.getTupleCount( inputs.get( 0 ) );
        for ( int i = 1; i < inputs.size(); i++ ) {
            dRows -= 0.5 * mq.getTupleCount( inputs.get( i ) );
        }
        if ( dRows < 0 ) {
            dRows = 0;
        }
        return dRows;
    }


    /**
     * Returns an estimate of the number of rows returned by a {@link Join}.
     */
    public static Double getJoinRowCount( AlgMetadataQuery mq, Join join, RexNode condition ) {
        // Row count estimates of 0 will be rounded up to 1. So, use maxRowCount where the product is very small.
        final Double left = mq.getTupleCount( join.getLeft() );
        final Double right = mq.getTupleCount( join.getRight() );
        if ( left == null || right == null ) {
            return null;
        }
        if ( left <= 1D || right <= 1D ) {
            Double max = mq.getMaxRowCount( join );
            if ( max != null && max <= 1D ) {
                return max;
            }
        }
        double product = left * right;

        // TODO: correlation factor
        return product * mq.getSelectivity( join, condition );
    }


    /**
     * Returns an estimate of the number of rows returned by a {@link SemiJoin}.
     */
    public static Double getSemiJoinRowCount( AlgMetadataQuery mq, AlgNode left, AlgNode right, JoinAlgType joinType, RexNode condition ) {
        // TODO: correlation factor
        final Double leftCount = mq.getTupleCount( left );
        if ( leftCount == null ) {
            return null;
        }
        return leftCount * RexUtil.getSelectivity( condition );
    }


    public static double estimateFilteredRows( AlgNode child, RexProgram program, AlgMetadataQuery mq ) {
        // convert the program's RexLocalRef condition to an expanded RexNode
        RexLocalRef programCondition = program.getCondition();
        RexNode condition;
        if ( programCondition == null ) {
            condition = null;
        } else {
            condition = program.expandLocalRef( programCondition );
        }
        return estimateFilteredRows( child, condition, mq );
    }


    public static double estimateFilteredRows( AlgNode child, RexNode condition, AlgMetadataQuery mq ) {
        return mq.getTupleCount( child ) * mq.getSelectivity( child, condition );
    }


    /**
     * Returns a point on a line.
     *
     * The result is always a value between {@code minY} and {@code maxY}, even if {@code x} is not between {@code minX} and {@code maxX}.
     *
     * Examples:
     * <ul>
     * <li>{@code linear(0, 0, 10, 100, 200}} returns 100 because 0 is minX</li>
     * <li>{@code linear(5, 0, 10, 100, 200}} returns 150 because 5 is mid-way between minX and maxX</li>
     * <li>{@code linear(5, 0, 10, 100, 200}} returns 160</li>
     * <li>{@code linear(10, 0, 10, 100, 200}} returns 200 because 10 is maxX</li>
     * <li>{@code linear(-2, 0, 10, 100, 200}} returns 100 because -2 is less than minX and is therefore treated as minX</li>
     * <li>{@code linear(12, 0, 10, 100, 200}} returns 100 because 12 is greater than maxX and is therefore treated as maxX</li>
     * </ul>
     */
    public static double linear( int x, int minX, int maxX, double minY, double maxY ) {
        Preconditions.checkArgument( minX < maxX );
        Preconditions.checkArgument( minY < maxY );
        if ( x < minX ) {
            return minY;
        }
        if ( x > maxX ) {
            return maxY;
        }
        return minY + (double) (x - minX) / (double) (maxX - minX) * (maxY - minY);
    }


    /**
     * Visitor that walks over a scalar expression and computes the cardinality of its result.
     */
    private static class CardOfProjExpr extends RexVisitorImpl<Double> {

        private final AlgMetadataQuery mq;
        private Project alg;


        CardOfProjExpr( AlgMetadataQuery mq, Project alg ) {
            super( true );
            this.mq = mq;
            this.alg = alg;
        }


        @Override
        public Double visitIndexRef( RexIndexRef var ) {
            int index = var.getIndex();
            ImmutableBitSet col = ImmutableBitSet.of( index );
            Double distinctRowCount = mq.getDistinctRowCount( alg.getInput(), col, null );
            if ( distinctRowCount == null ) {
                return null;
            } else {
                return numDistinctVals( distinctRowCount, mq.getTupleCount( alg ) );
            }
        }


        @Override
        public Double visitLiteral( RexLiteral literal ) {
            return numDistinctVals( 1.0, mq.getTupleCount( alg ) );
        }


        @Override
        public Double visitCall( RexCall call ) {
            Double distinctRowCount;
            Double rowCount = mq.getTupleCount( alg );
            if ( call.isA( Kind.MINUS_PREFIX ) ) {
                distinctRowCount = cardOfProjExpr( mq, alg, call.getOperands().get( 0 ) );
            } else if ( call.isA( ImmutableList.of( Kind.PLUS, Kind.MINUS ) ) ) {
                Double card0 = cardOfProjExpr( mq, alg, call.getOperands().get( 0 ) );
                if ( card0 == null ) {
                    return null;
                }
                Double card1 = cardOfProjExpr( mq, alg, call.getOperands().get( 1 ) );
                if ( card1 == null ) {
                    return null;
                }
                distinctRowCount = Math.max( card0, card1 );
            } else if ( call.isA( ImmutableList.of( Kind.TIMES, Kind.DIVIDE ) ) ) {
                distinctRowCount =
                        NumberUtil.multiply(
                                cardOfProjExpr( mq, alg, call.getOperands().get( 0 ) ),
                                cardOfProjExpr( mq, alg, call.getOperands().get( 1 ) ) );

                // TODO zfong 6/21/06 - Broadbase has code to handle date functions like year, month, day; E.g., cardinality of Month() is 12
            } else {
                if ( call.getOperands().size() == 1 ) {
                    distinctRowCount = cardOfProjExpr( mq, alg, call.getOperands().get( 0 ) );
                } else {
                    distinctRowCount = rowCount / 10;
                }
            }

            return numDistinctVals( distinctRowCount, rowCount );
        }

    }


    /**
     * Returns whether a relational expression is already sorted and has fewer rows than the sum of offset and limit.
     *
     * If this is the case, it is safe to push down a {@link Sort} with limit and optional offset.
     */
    public static boolean checkInputForCollationAndLimit( AlgMetadataQuery mq, AlgNode input, AlgCollation collation, RexNode offset, RexNode fetch ) {
        // Check if the input is already sorted
        boolean alreadySorted = collation.getFieldCollations().isEmpty();
        for ( AlgCollation inputCollation : mq.collations( input ) ) {
            if ( inputCollation.satisfies( collation ) ) {
                alreadySorted = true;
                break;
            }
        }
        // Check if we are not reducing the number of tuples
        boolean alreadySmaller = true;
        final Double rowCount = mq.getMaxRowCount( input );
        if ( rowCount != null && fetch != null ) {
            final int offsetVal = offset == null ? 0 : RexLiteral.intValue( offset );
            final int limit = RexLiteral.intValue( fetch );
            if ( (double) offsetVal + (double) limit < rowCount ) {
                alreadySmaller = false;
            }
        }
        return alreadySorted && alreadySmaller;
    }

}

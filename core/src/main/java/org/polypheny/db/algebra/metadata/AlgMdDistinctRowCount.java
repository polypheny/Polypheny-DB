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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Exchange;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.util.Bug;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.NumberUtil;


/**
 * RelMdDistinctRowCount supplies a default implementation of {@link AlgMetadataQuery#getDistinctRowCount} for the standard logical algebra.
 */
public class AlgMdDistinctRowCount implements MetadataHandler<BuiltInMetadata.DistinctRowCount> {

    public static final AlgMetadataProvider SOURCE = ReflectiveAlgMetadataProvider.reflectiveSource( new AlgMdDistinctRowCount(), BuiltInMethod.DISTINCT_ROW_COUNT.method );


    protected AlgMdDistinctRowCount() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.DistinctRowCount> getDef() {
        return BuiltInMetadata.DistinctRowCount.DEF;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.DistinctRowCount#getDistinctRowCount(ImmutableBitSet, RexNode)}, invoked using reflection.
     *
     * @see AlgMetadataQuery#getDistinctRowCount(AlgNode, ImmutableBitSet, RexNode)
     */
    public Double getDistinctRowCount( AlgNode alg, AlgMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate ) {
        // REVIEW zfong: Broadbase code does not take into consideration selectivity of predicates passed in.  Also, they assume the rows are unique even if the table is not
        boolean uniq = AlgMdUtil.areColumnsDefinitelyUnique( mq, alg, groupKey );
        if ( uniq ) {
            return NumberUtil.multiply( mq.getTupleCount( alg ), mq.getSelectivity( alg, predicate ) );
        }
        return null;
    }


    public Double getDistinctRowCount( Union alg, AlgMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate ) {
        double rowCount = 0.0;
        int[] adjustments = new int[alg.getTupleType().getFieldCount()];
        RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        for ( AlgNode input : alg.getInputs() ) {
            // convert the predicate to reference the types of the union child
            RexNode modifiedPred;
            if ( predicate == null ) {
                modifiedPred = null;
            } else {
                modifiedPred =
                        predicate.accept(
                                new AlgOptUtil.RexInputConverter(
                                        rexBuilder,
                                        null,
                                        input.getTupleType().getFields(),
                                        adjustments ) );
            }
            Double partialRowCount = mq.getDistinctRowCount( input, groupKey, modifiedPred );
            if ( partialRowCount == null ) {
                return null;
            }
            rowCount += partialRowCount;
        }
        return rowCount;
    }


    public Double getDistinctRowCount( Sort alg, AlgMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate ) {
        return mq.getDistinctRowCount( alg.getInput(), groupKey, predicate );
    }


    public Double getDistinctRowCount( Exchange alg, AlgMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate ) {
        return mq.getDistinctRowCount( alg.getInput(), groupKey, predicate );
    }


    public Double getDistinctRowCount( Filter alg, AlgMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate ) {
        if ( predicate == null || predicate.isAlwaysTrue() ) {
            if ( groupKey.isEmpty() ) {
                return 1D;
            }
        }
        // REVIEW zfong: In the Broadbase code, duplicates are not removed from the two filter lists.  However, the code below is doing so.
        RexNode unionPreds = AlgMdUtil.unionPreds( alg.getCluster().getRexBuilder(), predicate, alg.getCondition() );

        return mq.getDistinctRowCount( alg.getInput(), groupKey, unionPreds );
    }


    public Double getDistinctRowCount( Join alg, AlgMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate ) {
        if ( predicate == null || predicate.isAlwaysTrue() ) {
            if ( groupKey.isEmpty() ) {
                return 1D;
            }
        }
        return AlgMdUtil.getJoinDistinctRowCount( mq, alg, alg.getJoinType(), groupKey, predicate, false );
    }


    public Double getDistinctRowCount( SemiJoin alg, AlgMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate ) {
        if ( predicate == null || predicate.isAlwaysTrue() ) {
            if ( groupKey.isEmpty() ) {
                return 1D;
            }
        }
        // create a RexNode representing the selectivity of the semijoin filter and pass it to getDistinctRowCount
        RexNode newPred = AlgMdUtil.makeSemiJoinSelectivityRexNode( mq, alg );
        if ( predicate != null ) {
            RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
            newPred = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), newPred, predicate );
        }

        return mq.getDistinctRowCount( alg.getLeft(), groupKey, newPred );
    }


    public Double getDistinctRowCount( Aggregate alg, AlgMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate ) {
        if ( predicate == null || predicate.isAlwaysTrue() ) {
            if ( groupKey.isEmpty() ) {
                return 1D;
            }
        }
        // determine which predicates can be applied on the child of the aggregate
        final List<RexNode> notPushable = new ArrayList<>();
        final List<RexNode> pushable = new ArrayList<>();
        AlgOptUtil.splitFilters( alg.getGroupSet(), predicate, pushable, notPushable );
        final RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        RexNode childPreds = RexUtil.composeConjunction( rexBuilder, pushable, true );

        // set the bits as they correspond to the child input
        ImmutableBitSet.Builder childKey = ImmutableBitSet.builder();
        AlgMdUtil.setAggChildKeys( groupKey, alg, childKey );

        Double distinctRowCount = mq.getDistinctRowCount( alg.getInput(), childKey.build(), childPreds );
        if ( distinctRowCount == null ) {
            return null;
        } else if ( notPushable.isEmpty() ) {
            return distinctRowCount;
        } else {
            RexNode preds = RexUtil.composeConjunction( rexBuilder, notPushable, true );
            return distinctRowCount * AlgMdUtil.guessSelectivity( preds );
        }
    }


    public Double getDistinctRowCount( Values alg, AlgMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate ) {
        if ( predicate == null || predicate.isAlwaysTrue() ) {
            if ( groupKey.isEmpty() ) {
                return 1D;
            }
        }
        double selectivity = AlgMdUtil.guessSelectivity( predicate );

        // assume half the rows are duplicates
        double nRows = alg.estimateTupleCount( mq ) / 2;
        return AlgMdUtil.numDistinctVals( nRows, nRows * selectivity );
    }


    public Double getDistinctRowCount( Project alg, AlgMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate ) {
        if ( predicate == null || predicate.isAlwaysTrue() ) {
            if ( groupKey.isEmpty() ) {
                return 1D;
            }
        }
        ImmutableBitSet.Builder baseCols = ImmutableBitSet.builder();
        ImmutableBitSet.Builder projCols = ImmutableBitSet.builder();
        List<RexNode> projExprs = alg.getProjects();
        AlgMdUtil.splitCols( projExprs, groupKey, baseCols, projCols );

        final List<RexNode> notPushable = new ArrayList<>();
        final List<RexNode> pushable = new ArrayList<>();
        AlgOptUtil.splitFilters( ImmutableBitSet.range( alg.getTupleType().getFieldCount() ), predicate, pushable, notPushable );
        final RexBuilder rexBuilder = alg.getCluster().getRexBuilder();

        // get the distinct row count of the child input, passing in the columns and filters that only reference the child; convert the filter to reference the children projection expressions
        RexNode childPred = RexUtil.composeConjunction( rexBuilder, pushable, true );
        RexNode modifiedPred;
        if ( childPred == null ) {
            modifiedPred = null;
        } else {
            modifiedPred = AlgOptUtil.pushPastProject( childPred, alg );
        }
        Double distinctRowCount = mq.getDistinctRowCount( alg.getInput(), baseCols.build(), modifiedPred );

        if ( distinctRowCount == null ) {
            return null;
        } else if ( !notPushable.isEmpty() ) {
            RexNode preds = RexUtil.composeConjunction( rexBuilder, notPushable, true );
            distinctRowCount *= AlgMdUtil.guessSelectivity( preds );
        }

        // No further computation required if the projection expressions are all column references
        if ( projCols.cardinality() == 0 ) {
            return distinctRowCount;
        }

        // multiply by the cardinality of the non-child projection expressions
        for ( int bit : projCols.build() ) {
            Double subRowCount = AlgMdUtil.cardOfProjExpr( mq, alg, projExprs.get( bit ) );
            if ( subRowCount == null ) {
                return null;
            }
            distinctRowCount *= subRowCount;
        }

        return AlgMdUtil.numDistinctVals( distinctRowCount, mq.getTupleCount( alg ) );
    }


    public Double getDistinctRowCount( AlgSubset alg, AlgMetadataQuery mq, ImmutableBitSet groupKey, RexNode predicate ) {
        final AlgNode best = alg.getBest();
        if ( best != null ) {
            return mq.getDistinctRowCount( best, groupKey, predicate );
        }
        if ( !Bug.CALCITE_1048_FIXED ) {
            return getDistinctRowCount( (AlgNode) alg, mq, groupKey, predicate );
        }
        Double d = null;
        for ( AlgNode r2 : alg.getAlgs() ) {
            try {
                Double d2 = mq.getDistinctRowCount( r2, groupKey, predicate );
                d = NumberUtil.min( d, d2 );
            } catch ( CyclicMetadataException e ) {
                // Ignore this algebra expression; there will be non-cyclic ones in this set.
            }
        }
        return d;
    }

}

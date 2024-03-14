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
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * RelMdSelectivity supplies a default implementation of {@link AlgMetadataQuery#getSelectivity} for the standard logical algebra.
 */
public class AlgMdSelectivity implements MetadataHandler<BuiltInMetadata.Selectivity> {

    public static final AlgMetadataProvider SOURCE = ReflectiveAlgMetadataProvider.reflectiveSource( new AlgMdSelectivity(), BuiltInMethod.SELECTIVITY.method );


    protected AlgMdSelectivity() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.Selectivity> getDef() {
        return BuiltInMetadata.Selectivity.DEF;
    }


    public Double getSelectivity( Union alg, AlgMetadataQuery mq, RexNode predicate ) {
        if ( (alg.getInputs().size() == 0) || (predicate == null) ) {
            return 1.0;
        }

        double sumRows = 0.0;
        double sumSelectedRows = 0.0;
        int[] adjustments = new int[alg.getTupleType().getFieldCount()];
        RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        for ( AlgNode input : alg.getInputs() ) {
            Double nRows = mq.getTupleCount( input );
            if ( nRows == null ) {
                return null;
            }

            // convert the predicate to reference the types of the union child
            RexNode modifiedPred = predicate.accept( new AlgOptUtil.RexInputConverter( rexBuilder, null, input.getTupleType().getFields(), adjustments ) );
            double sel = mq.getSelectivity( input, modifiedPred );

            sumRows += nRows;
            sumSelectedRows += nRows * sel;
        }

        if ( sumRows < 1.0 ) {
            sumRows = 1.0;
        }
        return sumSelectedRows / sumRows;
    }


    public Double getSelectivity( Sort alg, AlgMetadataQuery mq, RexNode predicate ) {
        return mq.getSelectivity( alg.getInput(), predicate );
    }


    public Double getSelectivity( Filter alg, AlgMetadataQuery mq, RexNode predicate ) {
        // Take the difference between the predicate passed in and the predicate in the filter's condition, so we don't apply the selectivity of the filter twice.
        // If no predicate is passed in, use the filter's condition.
        if ( predicate != null ) {
            return mq.getSelectivity(
                    alg.getInput(),
                    AlgMdUtil.minusPreds(
                            alg.getCluster().getRexBuilder(),
                            predicate,
                            alg.getCondition() ) );
        } else {
            return mq.getSelectivity( alg.getInput(), alg.getCondition() );
        }
    }


    public Double getSelectivity( SemiJoin alg, AlgMetadataQuery mq, RexNode predicate ) {
        // create a RexNode representing the selectivity of the semijoin filter and pass it to getSelectivity
        RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        RexNode newPred = AlgMdUtil.makeSemiJoinSelectivityRexNode( mq, alg );
        if ( predicate != null ) {
            newPred = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), newPred, predicate );
        }

        return mq.getSelectivity( alg.getLeft(), newPred );
    }


    public Double getSelectivity( Aggregate alg, AlgMetadataQuery mq, RexNode predicate ) {
        final List<RexNode> notPushable = new ArrayList<>();
        final List<RexNode> pushable = new ArrayList<>();
        AlgOptUtil.splitFilters( alg.getGroupSet(), predicate, pushable, notPushable );
        final RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        RexNode childPred = RexUtil.composeConjunction( rexBuilder, pushable, true );

        Double selectivity = mq.getSelectivity( alg.getInput(), childPred );
        if ( selectivity == null ) {
            return null;
        } else {
            RexNode pred = RexUtil.composeConjunction( rexBuilder, notPushable, true );
            return selectivity * AlgMdUtil.guessSelectivity( pred );
        }
    }


    public Double getSelectivity( Project alg, AlgMetadataQuery mq, RexNode predicate ) {
        final List<RexNode> notPushable = new ArrayList<>();
        final List<RexNode> pushable = new ArrayList<>();
        AlgOptUtil.splitFilters( ImmutableBitSet.range( alg.getTupleType().getFieldCount() ), predicate, pushable, notPushable );
        final RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        RexNode childPred = RexUtil.composeConjunction( rexBuilder, pushable, true );

        RexNode modifiedPred;
        if ( childPred == null ) {
            modifiedPred = null;
        } else {
            modifiedPred = AlgOptUtil.pushPastProject( childPred, alg );
        }
        Double selectivity = mq.getSelectivity( alg.getInput(), modifiedPred );
        if ( selectivity == null ) {
            return null;
        } else {
            RexNode pred = RexUtil.composeConjunction( rexBuilder, notPushable, true );
            return selectivity * AlgMdUtil.guessSelectivity( pred );
        }
    }


    // Catch-all rule when none of the others apply.
    public Double getSelectivity( AlgNode alg, AlgMetadataQuery mq, RexNode predicate ) {
        return AlgMdUtil.guessSelectivity( predicate );
    }

}


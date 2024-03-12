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


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.enumerable.EnumerableInterpreter;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.util.BuiltInMethod;


/**
 * RelMdPercentageOriginalRows supplies a default implementation of {@link AlgMetadataQuery#getPercentageOriginalRows} for the standard logical algebra.
 */
public class AlgMdPercentageOriginalRows implements MetadataHandler<BuiltInMetadata.PercentageOriginalRows> {

    private static final AlgMdPercentageOriginalRows INSTANCE = new AlgMdPercentageOriginalRows();

    public static final AlgMetadataProvider SOURCE =
            ChainedAlgMetadataProvider.of(
                    ImmutableList.of(
                            ReflectiveAlgMetadataProvider.reflectiveSource( INSTANCE, BuiltInMethod.PERCENTAGE_ORIGINAL_ROWS.method ),
                            ReflectiveAlgMetadataProvider.reflectiveSource( INSTANCE, BuiltInMethod.CUMULATIVE_COST.method ),
                            ReflectiveAlgMetadataProvider.reflectiveSource( INSTANCE, BuiltInMethod.NON_CUMULATIVE_COST.method ) ) );


    private AlgMdPercentageOriginalRows() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.PercentageOriginalRows> getDef() {
        return BuiltInMetadata.PercentageOriginalRows.DEF;
    }


    public Double getPercentageOriginalRows( Aggregate alg, AlgMetadataQuery mq ) {
        // REVIEW jvs: The assumption here seems to be that aggregation does not apply any filtering, so it does not modify the percentage. That's very much oversimplified.
        return mq.getPercentageOriginalRows( alg.getInput() );
    }


    public Double getPercentageOriginalRows( Union alg, AlgMetadataQuery mq ) {
        double numerator = 0.0;
        double denominator = 0.0;

        // Ignore alg.isDistinct() because it's the same as an aggregate.

        // REVIEW jvs: The original Broadbase formula was broken. It was multiplying percentage into the numerator term rather than than dividing it out of the denominator term, which would be OK if
        // there weren't summation going on.  Probably the cause of the error was the desire to avoid division by zero, which I don't know how to handle so I punt, meaning we return a totally wrong answer in the
        // case where a huge table has been completely filtered away.

        for ( AlgNode input : alg.getInputs() ) {
            double rowCount = mq.getTupleCount( input );
            double percentage = mq.getPercentageOriginalRows( input );
            if ( percentage != 0.0 ) {
                denominator += rowCount / percentage;
                numerator += rowCount;
            }
        }

        return quotientForPercentage( numerator, denominator );
    }


    public Double getPercentageOriginalRows( Join alg, AlgMetadataQuery mq ) {
        // Assume any single-table filter conditions have already been pushed down.

        // REVIEW jvs: As with aggregation, this is oversimplified.

        // REVIEW jvs:  need any special casing for SemiJoin?

        double left = mq.getPercentageOriginalRows( alg.getLeft() );
        double right = mq.getPercentageOriginalRows( alg.getRight() );
        return left * right;
    }


    // Catch-all rule when none of the others apply.
    public Double getPercentageOriginalRows( AlgNode alg, AlgMetadataQuery mq ) {
        if ( alg.getInputs().size() > 1 ) {
            // No generic formula available for multiple inputs.
            return null;
        }

        if ( alg.getInputs().isEmpty() ) {
            // Assume no filtering happening at leaf.
            return 1.0;
        }

        AlgNode child = alg.getInputs().get( 0 );

        Double childPercentage = mq.getPercentageOriginalRows( child );
        if ( childPercentage == null ) {
            return null;
        }

        // Compute product of percentage filtering from this alg (assuming any filtering is the effect of single-table filters) with the percentage filtering performed by the child.
        Double algPercentage =
                quotientForPercentage( mq.getTupleCount( alg ), mq.getTupleCount( child ) );
        if ( algPercentage == null ) {
            return null;
        }
        double percent = algPercentage * childPercentage;

        // this check is needed in cases where this method is called on a physical rel
        if ( (percent < 0.0) || (percent > 1.0) ) {
            return null;
        }
        return algPercentage * childPercentage;
    }


    // Ditto for getNonCumulativeCost
    public AlgOptCost getCumulativeCost( AlgNode alg, AlgMetadataQuery mq ) {
        AlgOptCost cost = mq.getNonCumulativeCost( alg );
        List<AlgNode> inputs = alg.getInputs();
        for ( AlgNode input : inputs ) {
            cost = cost.plus( mq.getCumulativeCost( input ) );
        }
        return cost;
    }


    public AlgOptCost getCumulativeCost( EnumerableInterpreter alg, AlgMetadataQuery mq ) {
        return mq.getNonCumulativeCost( alg );
    }


    // Ditto for getNonCumulativeCost
    public AlgOptCost getNonCumulativeCost( AlgNode alg, AlgMetadataQuery mq ) {
        return alg.computeSelfCost( alg.getCluster().getPlanner(), mq );
    }


    private static Double quotientForPercentage( Double numerator, Double denominator ) {
        if ( (numerator == null) || (denominator == null) ) {
            return null;
        }

        // may need epsilon instead
        if ( denominator == 0.0 ) {
            // cap at 100%
            return 1.0;
        } else {
            return numerator / denominator;
        }
    }

}


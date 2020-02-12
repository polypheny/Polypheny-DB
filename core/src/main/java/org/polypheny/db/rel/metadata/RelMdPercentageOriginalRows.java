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

package org.polypheny.db.rel.metadata;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.adapter.enumerable.EnumerableInterpreter;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Aggregate;
import org.polypheny.db.rel.core.Join;
import org.polypheny.db.rel.core.Union;
import org.polypheny.db.util.BuiltInMethod;


/**
 * RelMdPercentageOriginalRows supplies a default implementation of {@link RelMetadataQuery#getPercentageOriginalRows} for the standard logical algebra.
 */
public class RelMdPercentageOriginalRows implements MetadataHandler<BuiltInMetadata.PercentageOriginalRows> {

    private static final RelMdPercentageOriginalRows INSTANCE = new RelMdPercentageOriginalRows();

    public static final RelMetadataProvider SOURCE =
            ChainedRelMetadataProvider.of(
                    ImmutableList.of(
                            ReflectiveRelMetadataProvider.reflectiveSource( BuiltInMethod.PERCENTAGE_ORIGINAL_ROWS.method, INSTANCE ),
                            ReflectiveRelMetadataProvider.reflectiveSource( BuiltInMethod.CUMULATIVE_COST.method, INSTANCE ),
                            ReflectiveRelMetadataProvider.reflectiveSource( BuiltInMethod.NON_CUMULATIVE_COST.method, INSTANCE ) ) );


    private RelMdPercentageOriginalRows() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.PercentageOriginalRows> getDef() {
        return BuiltInMetadata.PercentageOriginalRows.DEF;
    }


    public Double getPercentageOriginalRows( Aggregate rel, RelMetadataQuery mq ) {
        // REVIEW jvs: The assumption here seems to be that aggregation does not apply any filtering, so it does not modify the percentage. That's very much oversimplified.
        return mq.getPercentageOriginalRows( rel.getInput() );
    }


    public Double getPercentageOriginalRows( Union rel, RelMetadataQuery mq ) {
        double numerator = 0.0;
        double denominator = 0.0;

        // Ignore rel.isDistinct() because it's the same as an aggregate.

        // REVIEW jvs: The original Broadbase formula was broken. It was multiplying percentage into the numerator term rather than than dividing it out of the denominator term, which would be OK if
        // there weren't summation going on.  Probably the cause of the error was the desire to avoid division by zero, which I don't know how to handle so I punt, meaning we return a totally wrong answer in the
        // case where a huge table has been completely filtered away.

        for ( RelNode input : rel.getInputs() ) {
            double rowCount = mq.getRowCount( input );
            double percentage = mq.getPercentageOriginalRows( input );
            if ( percentage != 0.0 ) {
                denominator += rowCount / percentage;
                numerator += rowCount;
            }
        }

        return quotientForPercentage( numerator, denominator );
    }


    public Double getPercentageOriginalRows( Join rel, RelMetadataQuery mq ) {
        // Assume any single-table filter conditions have already been pushed down.

        // REVIEW jvs: As with aggregation, this is oversimplified.

        // REVIEW jvs:  need any special casing for SemiJoin?

        double left = mq.getPercentageOriginalRows( rel.getLeft() );
        double right = mq.getPercentageOriginalRows( rel.getRight() );
        return left * right;
    }


    // Catch-all rule when none of the others apply.
    public Double getPercentageOriginalRows( RelNode rel, RelMetadataQuery mq ) {
        if ( rel.getInputs().size() > 1 ) {
            // No generic formula available for multiple inputs.
            return null;
        }

        if ( rel.getInputs().size() == 0 ) {
            // Assume no filtering happening at leaf.
            return 1.0;
        }

        RelNode child = rel.getInputs().get( 0 );

        Double childPercentage = mq.getPercentageOriginalRows( child );
        if ( childPercentage == null ) {
            return null;
        }

        // Compute product of percentage filtering from this rel (assuming any filtering is the effect of single-table filters) with the percentage filtering performed by the child.
        Double relPercentage =
                quotientForPercentage( mq.getRowCount( rel ), mq.getRowCount( child ) );
        if ( relPercentage == null ) {
            return null;
        }
        double percent = relPercentage * childPercentage;

        // this check is needed in cases where this method is called on a physical rel
        if ( (percent < 0.0) || (percent > 1.0) ) {
            return null;
        }
        return relPercentage * childPercentage;
    }


    // Ditto for getNonCumulativeCost
    public RelOptCost getCumulativeCost( RelNode rel, RelMetadataQuery mq ) {
        RelOptCost cost = mq.getNonCumulativeCost( rel );
        List<RelNode> inputs = rel.getInputs();
        for ( RelNode input : inputs ) {
            cost = cost.plus( mq.getCumulativeCost( input ) );
        }
        return cost;
    }


    public RelOptCost getCumulativeCost( EnumerableInterpreter rel, RelMetadataQuery mq ) {
        return mq.getNonCumulativeCost( rel );
    }


    // Ditto for getNonCumulativeCost
    public RelOptCost getNonCumulativeCost( RelNode rel, RelMetadataQuery mq ) {
        return rel.computeSelfCost( rel.getCluster().getPlanner(), mq );
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


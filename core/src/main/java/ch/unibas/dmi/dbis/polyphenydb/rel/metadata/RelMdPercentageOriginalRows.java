/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.metadata;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableInterpreter;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCost;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Union;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import com.google.common.collect.ImmutableList;
import java.util.List;


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
        // REVIEW jvs 28-Mar-2006: The assumption here seems to be that aggregation does not apply any filtering, so it does not modify the percentage.  That's very much oversimplified.
        return mq.getPercentageOriginalRows( rel.getInput() );
    }


    public Double getPercentageOriginalRows( Union rel, RelMetadataQuery mq ) {
        double numerator = 0.0;
        double denominator = 0.0;

        // Ignore rel.isDistinct() because it's the same as an aggregate.

        // REVIEW jvs 28-Mar-2006: The original Broadbase formula was broken. It was multiplying percentage into the numerator term rather than than dividing it out of the denominator term, which would be OK if
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

        // REVIEW jvs 28-Mar-2006: As with aggregation, this is oversimplified.

        // REVIEW jvs 28-Mar-2006:  need any special casing for SemiJoin?

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


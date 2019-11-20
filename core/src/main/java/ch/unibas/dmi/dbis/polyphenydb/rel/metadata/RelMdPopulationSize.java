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


import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Exchange;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.SemiJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Union;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Values;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import java.util.List;


/**
 * RelMdPopulationSize supplies a default implementation of {@link RelMetadataQuery#getPopulationSize} for the standard logical algebra.
 */
public class RelMdPopulationSize implements MetadataHandler<BuiltInMetadata.PopulationSize> {

    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.POPULATION_SIZE.method,
                    new RelMdPopulationSize() );


    private RelMdPopulationSize() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.PopulationSize> getDef() {
        return BuiltInMetadata.PopulationSize.DEF;
    }


    public Double getPopulationSize( Filter rel, RelMetadataQuery mq, ImmutableBitSet groupKey ) {
        return mq.getPopulationSize( rel.getInput(), groupKey );
    }


    public Double getPopulationSize( Sort rel, RelMetadataQuery mq, ImmutableBitSet groupKey ) {
        return mq.getPopulationSize( rel.getInput(), groupKey );
    }


    public Double getPopulationSize( Exchange rel, RelMetadataQuery mq, ImmutableBitSet groupKey ) {
        return mq.getPopulationSize( rel.getInput(), groupKey );
    }


    public Double getPopulationSize( Union rel, RelMetadataQuery mq, ImmutableBitSet groupKey ) {
        double population = 0.0;
        for ( RelNode input : rel.getInputs() ) {
            Double subPop = mq.getPopulationSize( input, groupKey );
            if ( subPop == null ) {
                return null;
            }
            population += subPop;
        }
        return population;
    }


    public Double getPopulationSize( Join rel, RelMetadataQuery mq, ImmutableBitSet groupKey ) {
        return RelMdUtil.getJoinPopulationSize( mq, rel, groupKey );
    }


    public Double getPopulationSize( SemiJoin rel, RelMetadataQuery mq, ImmutableBitSet groupKey ) {
        return mq.getPopulationSize( rel.getLeft(), groupKey );
    }


    public Double getPopulationSize( Aggregate rel, RelMetadataQuery mq, ImmutableBitSet groupKey ) {
        ImmutableBitSet.Builder childKey = ImmutableBitSet.builder();
        RelMdUtil.setAggChildKeys( groupKey, rel, childKey );
        return mq.getPopulationSize( rel.getInput(), childKey.build() );
    }


    public Double getPopulationSize( Values rel, RelMetadataQuery mq, ImmutableBitSet groupKey ) {
        // assume half the rows are duplicates
        return rel.estimateRowCount( mq ) / 2;
    }


    public Double getPopulationSize( Project rel, RelMetadataQuery mq, ImmutableBitSet groupKey ) {
        ImmutableBitSet.Builder baseCols = ImmutableBitSet.builder();
        ImmutableBitSet.Builder projCols = ImmutableBitSet.builder();
        List<RexNode> projExprs = rel.getProjects();
        RelMdUtil.splitCols( projExprs, groupKey, baseCols, projCols );

        Double population = mq.getPopulationSize( rel.getInput(), baseCols.build() );
        if ( population == null ) {
            return null;
        }

        // No further computation required if the projection expressions are all column references
        if ( projCols.cardinality() == 0 ) {
            return population;
        }

        for ( int bit : projCols.build() ) {
            Double subRowCount = RelMdUtil.cardOfProjExpr( mq, rel, projExprs.get( bit ) );
            if ( subRowCount == null ) {
                return null;
            }
            population *= subRowCount;
        }

        // REVIEW zfong: Broadbase did not have the call to numDistinctVals.  This is needed; otherwise, population can be larger than the number of rows in the RelNode.
        return RelMdUtil.numDistinctVals( population, mq.getRowCount( rel ) );
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.PopulationSize#getPopulationSize(ImmutableBitSet)}, invoked using reflection.
     *
     * @see ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery#getPopulationSize(RelNode, ImmutableBitSet)
     */
    public Double getPopulationSize( RelNode rel, RelMetadataQuery mq, ImmutableBitSet groupKey ) {
        // if the keys are unique, return the row count; otherwise, we have no further information on which to return any legitimate value

        // REVIEW zfong: Broadbase code returns the product of each unique key, which would result in the population being larger than the total rows in the relnode
        boolean uniq = RelMdUtil.areColumnsDefinitelyUnique( mq, rel, groupKey );
        if ( uniq ) {
            return mq.getRowCount( rel );
        }

        return null;
    }
}


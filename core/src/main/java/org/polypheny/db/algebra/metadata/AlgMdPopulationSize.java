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
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * RelMdPopulationSize supplies a default implementation of {@link AlgMetadataQuery#getPopulationSize} for the standard logical algebra.
 */
public class AlgMdPopulationSize implements MetadataHandler<BuiltInMetadata.PopulationSize> {

    public static final AlgMetadataProvider SOURCE =
            ReflectiveAlgMetadataProvider.reflectiveSource(
                    new AlgMdPopulationSize(),
                    BuiltInMethod.POPULATION_SIZE.method );


    private AlgMdPopulationSize() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.PopulationSize> getDef() {
        return BuiltInMetadata.PopulationSize.DEF;
    }


    public Double getPopulationSize( Filter alg, AlgMetadataQuery mq, ImmutableBitSet groupKey ) {
        return mq.getPopulationSize( alg.getInput(), groupKey );
    }


    public Double getPopulationSize( Sort alg, AlgMetadataQuery mq, ImmutableBitSet groupKey ) {
        return mq.getPopulationSize( alg.getInput(), groupKey );
    }


    public Double getPopulationSize( Exchange alg, AlgMetadataQuery mq, ImmutableBitSet groupKey ) {
        return mq.getPopulationSize( alg.getInput(), groupKey );
    }


    public Double getPopulationSize( Union alg, AlgMetadataQuery mq, ImmutableBitSet groupKey ) {
        double population = 0.0;
        for ( AlgNode input : alg.getInputs() ) {
            Double subPop = mq.getPopulationSize( input, groupKey );
            if ( subPop == null ) {
                return null;
            }
            population += subPop;
        }
        return population;
    }


    public Double getPopulationSize( Join alg, AlgMetadataQuery mq, ImmutableBitSet groupKey ) {
        return AlgMdUtil.getJoinPopulationSize( mq, alg, groupKey );
    }


    public Double getPopulationSize( SemiJoin alg, AlgMetadataQuery mq, ImmutableBitSet groupKey ) {
        return mq.getPopulationSize( alg.getLeft(), groupKey );
    }


    public Double getPopulationSize( Aggregate alg, AlgMetadataQuery mq, ImmutableBitSet groupKey ) {
        ImmutableBitSet.Builder childKey = ImmutableBitSet.builder();
        AlgMdUtil.setAggChildKeys( groupKey, alg, childKey );
        return mq.getPopulationSize( alg.getInput(), childKey.build() );
    }


    public Double getPopulationSize( Values alg, AlgMetadataQuery mq, ImmutableBitSet groupKey ) {
        // assume half the rows are duplicates
        return alg.estimateTupleCount( mq ) / 2;
    }


    public Double getPopulationSize( Project alg, AlgMetadataQuery mq, ImmutableBitSet groupKey ) {
        ImmutableBitSet.Builder baseCols = ImmutableBitSet.builder();
        ImmutableBitSet.Builder projCols = ImmutableBitSet.builder();
        List<RexNode> projExprs = alg.getProjects();
        AlgMdUtil.splitCols( projExprs, groupKey, baseCols, projCols );

        Double population = mq.getPopulationSize( alg.getInput(), baseCols.build() );
        if ( population == null ) {
            return null;
        }

        // No further computation required if the projection expressions are all column references
        if ( projCols.cardinality() == 0 ) {
            return population;
        }

        for ( int bit : projCols.build() ) {
            Double subRowCount = AlgMdUtil.cardOfProjExpr( mq, alg, projExprs.get( bit ) );
            if ( subRowCount == null ) {
                return null;
            }
            population *= subRowCount;
        }

        // REVIEW zfong: Broadbase did not have the call to numDistinctVals.  This is needed; otherwise, population can be larger than the number of rows in the AlgNode.
        return AlgMdUtil.numDistinctVals( population, mq.getTupleCount( alg ) );
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.PopulationSize#getPopulationSize(ImmutableBitSet)}, invoked using reflection.
     *
     * @see AlgMetadataQuery#getPopulationSize(AlgNode, ImmutableBitSet)
     */
    public Double getPopulationSize( AlgNode alg, AlgMetadataQuery mq, ImmutableBitSet groupKey ) {
        // if the keys are unique, return the row count; otherwise, we have no further information on which to return any legitimate value

        // REVIEW zfong: Broadbase code returns the product of each unique key, which would result in the population being larger than the total rows in the relnode
        boolean uniq = AlgMdUtil.areColumnsDefinitelyUnique( mq, alg, groupKey );
        if ( uniq ) {
            return mq.getTupleCount( alg );
        }

        return null;
    }

}


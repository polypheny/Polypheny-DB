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

package org.polypheny.db.algebra.enumerable;


import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMdDistribution;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;


/**
 * Implementation of {@link Filter} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableFilter extends Filter implements EnumerableAlg {

    /**
     * Creates an EnumerableFilter.
     *
     * Use {@link #create} unless you know what you're doing.
     */
    public EnumerableFilter( AlgCluster cluster, AlgTraitSet traitSet, AlgNode child, RexNode condition ) {
        super( cluster, traitSet, child, condition );
        assert getConvention() instanceof EnumerableConvention;
    }


    /**
     * Creates an EnumerableFilter.
     */
    public static EnumerableFilter create( final AlgNode input, RexNode condition ) {
        final AlgCluster cluster = input.getCluster();
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet =
                cluster.traitSetOf( EnumerableConvention.INSTANCE )
                        .replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.filter( mq, input ) )
                        .replaceIf( AlgDistributionTraitDef.INSTANCE, () -> AlgMdDistribution.filter( mq, input ) );
        return new EnumerableFilter( cluster, traitSet, input, condition );
    }


    @Override
    public EnumerableFilter copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        return new EnumerableFilter( getCluster(), traitSet, input, condition );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        // EnumerableCalc is always better
        throw new UnsupportedOperationException();
    }

}


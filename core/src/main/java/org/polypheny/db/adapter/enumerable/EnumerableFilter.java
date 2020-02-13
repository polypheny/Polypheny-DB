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

package org.polypheny.db.adapter.enumerable;


import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelCollationTraitDef;
import org.polypheny.db.rel.RelDistributionTraitDef;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.metadata.RelMdCollation;
import org.polypheny.db.rel.metadata.RelMdDistribution;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rex.RexNode;


/**
 * Implementation of {@link Filter} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableFilter extends Filter implements EnumerableRel {

    /**
     * Creates an EnumerableFilter.
     *
     * Use {@link #create} unless you know what you're doing.
     */
    public EnumerableFilter( RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition ) {
        super( cluster, traitSet, child, condition );
        assert getConvention() instanceof EnumerableConvention;
    }


    /**
     * Creates an EnumerableFilter.
     */
    public static EnumerableFilter create( final RelNode input, RexNode condition ) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSetOf( EnumerableConvention.INSTANCE )
                        .replaceIfs( RelCollationTraitDef.INSTANCE, () -> RelMdCollation.filter( mq, input ) )
                        .replaceIf( RelDistributionTraitDef.INSTANCE, () -> RelMdDistribution.filter( mq, input ) );
        return new EnumerableFilter( cluster, traitSet, input, condition );
    }


    @Override
    public EnumerableFilter copy( RelTraitSet traitSet, RelNode input, RexNode condition ) {
        return new EnumerableFilter( getCluster(), traitSet, input, condition );
    }


    @Override
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        // EnumerableCalc is always better
        throw new UnsupportedOperationException();
    }
}


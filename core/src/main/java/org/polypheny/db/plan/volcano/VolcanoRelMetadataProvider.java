/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.plan.volcano;


import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.lang.reflect.Method;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.metadata.AlgMetadataProvider;
import org.polypheny.db.algebra.metadata.Metadata;
import org.polypheny.db.algebra.metadata.MetadataDef;
import org.polypheny.db.algebra.metadata.MetadataHandler;
import org.polypheny.db.algebra.metadata.UnboundMetadata;


/**
 * VolcanoRelMetadataProvider implements the {@link AlgMetadataProvider} interface by combining metadata from the rels making up an equivalence class.
 */
public class VolcanoRelMetadataProvider implements AlgMetadataProvider {

    @Override
    public boolean equals( Object obj ) {
        return obj instanceof VolcanoRelMetadataProvider;
    }


    @Override
    public int hashCode() {
        return 103;
    }


    @Override
    public <M extends Metadata> UnboundMetadata<M> apply( Class<? extends AlgNode> algClass, final Class<? extends M> metadataClass ) {
        if ( algClass != AlgSubset.class ) {
            // let someone else further down the chain sort it out
            return null;
        }

        return ( alg, mq ) -> {
            final AlgSubset subset = (AlgSubset) alg;
            final AlgMetadataProvider provider = alg.getCluster().getMetadataProvider();

            // REVIEW jvs: I'm not sure what the correct precedence should be here.  Letting the current best plan take the first shot is probably the right thing to do for physical estimates
            // such as row count. Dunno about others, and whether we need a way to discriminate.

            // First, try current best implementation.  If it knows how to answer this query, treat it as the most reliable.
            if ( subset.best != null ) {
                final UnboundMetadata<M> function = provider.apply( subset.best.getClass(), metadataClass );
                if ( function != null ) {
                    final M metadata = function.bind( subset.best, mq );
                    if ( metadata != null ) {
                        return metadata;
                    }
                }
            }

            // Otherwise, try rels in same logical equivalence class to see if any of them have a good answer.  We use the full logical equivalence class rather than just the subset because
            // many metadata providers only know about logical metadata.

            // Equivalence classes can get tangled up in interesting ways, so avoid an infinite loop. REVIEW: There's a chance this will cause us to fail on metadata queries which invoke other queries,
            // e.g. PercentageOriginalRows -> Selectivity.  If we implement caching at this level, we could probably kill two birds with one stone (use presence of pending cache entry to detect re-entrancy at
            // the correct granularity).
            if ( subset.set.inMetadataQuery ) {
                return null;
            }

            subset.set.inMetadataQuery = true;
            try {
                for ( AlgNode algCandidate : subset.set.algs ) {
                    final UnboundMetadata<M> function = provider.apply( algCandidate.getClass(), metadataClass );
                    if ( function != null ) {
                        final M result = function.bind( algCandidate, mq );
                        if ( result != null ) {
                            return result;
                        }
                    }
                }
            } finally {
                subset.set.inMetadataQuery = false;
            }

            // Give up.
            return null;
        };
    }


    @Override
    public <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers( MetadataDef<M> def ) {
        return ImmutableMultimap.of();
    }

}


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

package org.polypheny.db.algebra.metadata;


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.concurrent.ExecutionException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link MetadataFactory} that gets providers from a {@link AlgMetadataProvider} and stores them in a cache.
 *
 * The cache does not store metadata. It remembers which providers can provide which kinds of metadata, for which kinds of relational expressions.
 */
public class MetadataFactoryImpl implements MetadataFactory {

    public static final UnboundMetadata<Metadata> DUMMY = ( alg, mq ) -> null;

    private final LoadingCache<Pair<Class<AlgNode>, Class<Metadata>>, UnboundMetadata<Metadata>> cache;


    public MetadataFactoryImpl( AlgMetadataProvider provider ) {
        this.cache = CacheBuilder.newBuilder().build( loader( provider ) );
    }


    private static CacheLoader<Pair<Class<AlgNode>, Class<Metadata>>, UnboundMetadata<Metadata>> loader( final AlgMetadataProvider provider ) {
        return CacheLoader.from( key -> {
            final UnboundMetadata<Metadata> function = provider.apply( key.left, key.right );
            // Return DUMMY, not null, so the cache knows to not ask again.
            return function != null ? function : DUMMY;
        } );
    }


    @Override
    public <M extends Metadata> M query( AlgNode alg, AlgMetadataQuery mq, Class<M> metadataClazz ) {
        try {
            //noinspection unchecked
            final Pair<Class<AlgNode>, Class<Metadata>> key = (Pair) Pair.of( alg.getClass(), metadataClazz );
            final Metadata apply = cache.get( key ).bind( alg, mq );
            return metadataClazz.cast( apply );
        } catch ( UncheckedExecutionException | ExecutionException e ) {
            Util.throwIfUnchecked( e.getCause() );
            throw new RuntimeException( e.getCause() );
        }
    }

}


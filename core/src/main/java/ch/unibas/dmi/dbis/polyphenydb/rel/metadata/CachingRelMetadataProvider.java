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


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * Implementation of the {@link RelMetadataProvider} interface that caches results from an underlying provider.
 */
public class CachingRelMetadataProvider implements RelMetadataProvider {

    private final Map<List, CacheEntry> cache = new HashMap<>();

    private final RelMetadataProvider underlyingProvider;

    private final RelOptPlanner planner;


    public CachingRelMetadataProvider( RelMetadataProvider underlyingProvider, RelOptPlanner planner ) {
        this.underlyingProvider = underlyingProvider;
        this.planner = planner;
    }


    @Override
    public <M extends Metadata> UnboundMetadata<M> apply( Class<? extends RelNode> relClass, final Class<? extends M> metadataClass ) {
        final UnboundMetadata<M> function = underlyingProvider.apply( relClass, metadataClass );
        if ( function == null ) {
            return null;
        }

        // TODO jvs 30-Mar-2006: Use meta-metadata to decide which metadata query results can stay fresh until the next Ice Age.
        return ( rel, mq ) -> {
            final Metadata metadata = function.bind( rel, mq );
            return metadataClass.cast(
                    Proxy.newProxyInstance( metadataClass.getClassLoader(),
                            new Class[]{ metadataClass },
                            new CachingInvocationHandler( metadata ) ) );
        };
    }


    @Override
    public <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers( MetadataDef<M> def ) {
        return underlyingProvider.handlers( def );
    }


    /**
     * An entry in the cache. Consists of the cached object and the timestamp when the entry is valid. If read at a later timestamp, the entry will be invalid and will be re-computed as if it did not exist.
     * The net effect is a lazy-flushing cache.
     */
    private static class CacheEntry {

        long timestamp;
        Object result;
    }


    /**
     * Implementation of {@link InvocationHandler} for calls to a {@link CachingRelMetadataProvider}. Each request first looks in the cache;
     * if the cache entry is present and not expired, returns the cache entry, otherwise computes the value and stores in the cache.
     */
    private class CachingInvocationHandler implements InvocationHandler {

        private final Metadata metadata;


        CachingInvocationHandler( Metadata metadata ) {
            this.metadata = Objects.requireNonNull( metadata );
        }


        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable {
            // Compute hash key.
            final ImmutableList.Builder<Object> builder = ImmutableList.builder();
            builder.add( method );
            builder.add( metadata.rel() );
            if ( args != null ) {
                for ( Object arg : args ) {
                    // Replace null values because ImmutableList does not allow them.
                    builder.add( NullSentinel.mask( arg ) );
                }
            }
            List<Object> key = builder.build();

            long timestamp = planner.getRelMetadataTimestamp( metadata.rel() );

            // Perform cache lookup.
            CacheEntry entry = cache.get( key );
            if ( entry != null ) {
                if ( timestamp == entry.timestamp ) {
                    return entry.result;
                }
            }

            // Cache miss or stale.
            try {
                Object result = method.invoke( metadata, args );
                if ( result != null ) {
                    entry = new CacheEntry();
                    entry.timestamp = timestamp;
                    entry.result = result;
                    cache.put( key, entry );
                }
                return result;
            } catch ( InvocationTargetException e ) {
                throw e.getCause();
            }
        }
    }
}

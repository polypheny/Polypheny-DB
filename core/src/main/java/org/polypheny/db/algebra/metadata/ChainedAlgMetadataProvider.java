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
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Util;


/**
 * Implementation of the {@link AlgMetadataProvider} interface via the {@link org.polypheny.db.util.Glossary#CHAIN_OF_RESPONSIBILITY_PATTERN}.
 *
 * When a consumer calls the {@link #apply} method to ask for a provider for a particular type of {@link AlgNode} and {@link Metadata}, scans the list of underlying providers.
 */
public class ChainedAlgMetadataProvider implements AlgMetadataProvider {

    private final ImmutableList<AlgMetadataProvider> providers;


    /**
     * Creates a chain.
     */
    protected ChainedAlgMetadataProvider( ImmutableList<AlgMetadataProvider> providers ) {
        this.providers = providers;
        assert !providers.contains( this );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof ChainedAlgMetadataProvider
                && providers.equals( ((ChainedAlgMetadataProvider) obj).providers );
    }


    @Override
    public int hashCode() {
        return providers.hashCode();
    }


    @Override
    public <M extends Metadata> UnboundMetadata<M> apply( Class<? extends AlgNode> algClass, final Class<? extends M> metadataClass ) {
        final List<UnboundMetadata<M>> functions = new ArrayList<>();
        for ( AlgMetadataProvider provider : providers ) {
            final UnboundMetadata<M> function = provider.apply( algClass, metadataClass );
            if ( function == null ) {
                continue;
            }
            functions.add( function );
        }
        return switch ( functions.size() ) {
            case 0 -> null;
            case 1 -> functions.get( 0 );
            default -> ( alg, mq ) -> {
                final List<Metadata> metadataList = new ArrayList<>();
                for ( UnboundMetadata<M> function : functions ) {
                    final Metadata metadata = function.bind( alg, mq );
                    if ( metadata != null ) {
                        metadataList.add( metadata );
                    }
                }
                return metadataClass.cast(
                        Proxy.newProxyInstance(
                                metadataClass.getClassLoader(),
                                new Class[]{ metadataClass },
                                new ChainedInvocationHandler( metadataList ) ) );
            };
        };
    }


    @Override
    public <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers( MetadataDef<M> def ) {
        final ImmutableMultimap.Builder<Method, MetadataHandler<M>> builder = ImmutableMultimap.builder();
        for ( AlgMetadataProvider provider : providers.reverse() ) {
            builder.putAll( provider.handlers( def ) );
        }
        return builder.build();
    }


    /**
     * Creates a chain.
     */
    public static AlgMetadataProvider of( List<AlgMetadataProvider> list ) {
        return new ChainedAlgMetadataProvider( ImmutableList.copyOf( list ) );
    }


    /**
     * Invocation handler that calls a list of {@link Metadata} objects, returning the first non-null value.
     */
    private record ChainedInvocationHandler(List<Metadata> metadataList) implements InvocationHandler {

        private ChainedInvocationHandler( List<Metadata> metadataList ) {
            this.metadataList = ImmutableList.copyOf( metadataList );
        }


        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable {
            for ( Metadata metadata : metadataList ) {
                try {
                    final Object o = method.invoke( metadata, args );
                    if ( o != null ) {
                        return o;
                    }
                } catch ( InvocationTargetException e ) {
                    if ( e.getCause() instanceof CyclicMetadataException ) {
                        continue;
                    }
                    Util.throwIfUnchecked( e.getCause() );
                    throw new GenericRuntimeException( e.getCause() );
                }
            }
            return null;
        }

    }

}


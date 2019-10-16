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
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;


/**
 * Implementation of the {@link RelMetadataProvider} interface via the {@link ch.unibas.dmi.dbis.polyphenydb.util.Glossary#CHAIN_OF_RESPONSIBILITY_PATTERN}.
 *
 * When a consumer calls the {@link #apply} method to ask for a provider for a particular type of {@link RelNode} and {@link Metadata}, scans the list of underlying providers.
 */
public class ChainedRelMetadataProvider implements RelMetadataProvider {

    private final ImmutableList<RelMetadataProvider> providers;


    /**
     * Creates a chain.
     */
    protected ChainedRelMetadataProvider( ImmutableList<RelMetadataProvider> providers ) {
        this.providers = providers;
        assert !providers.contains( this );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof ChainedRelMetadataProvider
                && providers.equals( ((ChainedRelMetadataProvider) obj).providers );
    }


    @Override
    public int hashCode() {
        return providers.hashCode();
    }


    @Override
    public <M extends Metadata> UnboundMetadata<M> apply( Class<? extends RelNode> relClass, final Class<? extends M> metadataClass ) {
        final List<UnboundMetadata<M>> functions = new ArrayList<>();
        for ( RelMetadataProvider provider : providers ) {
            final UnboundMetadata<M> function = provider.apply( relClass, metadataClass );
            if ( function == null ) {
                continue;
            }
            functions.add( function );
        }
        switch ( functions.size() ) {
            case 0:
                return null;
            case 1:
                return functions.get( 0 );
            default:
                return ( rel, mq ) -> {
                    final List<Metadata> metadataList = new ArrayList<>();
                    for ( UnboundMetadata<M> function : functions ) {
                        final Metadata metadata = function.bind( rel, mq );
                        if ( metadata != null ) {
                            metadataList.add( metadata );
                        }
                    }
                    return metadataClass.cast(
                            Proxy.newProxyInstance( metadataClass.getClassLoader(),
                                    new Class[]{ metadataClass },
                                    new ChainedInvocationHandler( metadataList ) ) );
                };
        }
    }


    @Override
    public <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers( MetadataDef<M> def ) {
        final ImmutableMultimap.Builder<Method, MetadataHandler<M>> builder = ImmutableMultimap.builder();
        for ( RelMetadataProvider provider : providers.reverse() ) {
            builder.putAll( provider.handlers( def ) );
        }
        return builder.build();
    }


    /**
     * Creates a chain.
     */
    public static RelMetadataProvider of( List<RelMetadataProvider> list ) {
        return new ChainedRelMetadataProvider( ImmutableList.copyOf( list ) );
    }


    /**
     * Invocation handler that calls a list of {@link Metadata} objects, returning the first non-null value.
     */
    private static class ChainedInvocationHandler implements InvocationHandler {

        private final List<Metadata> metadataList;


        ChainedInvocationHandler( List<Metadata> metadataList ) {
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
                    throw new RuntimeException( e.getCause() );
                }
            }
            return null;
        }
    }
}


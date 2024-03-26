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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.ImmutableNullableList;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Implementation of the {@link AlgMetadataProvider} interface that dispatches metadata methods to methods on a given object via reflection.
 * <p>
 * The methods on the target object must be public and non-static, and have the same signature as the implemented metadata method except for an additional first parameter of type {@link AlgNode} or a sub-class.
 * That parameter gives this provider an indication of that relational expressions it can handle.
 * <p>
 * For an example, see {@link AlgMdColumnOrigins#SOURCE}.
 */
public class ReflectiveAlgMetadataProvider implements AlgMetadataProvider {

    private final ConcurrentMap<Class<AlgNode>, UnboundMetadata<Metadata>> map;
    private final Class<? extends Metadata> metadataClass0;
    private final ImmutableMultimap<Method, MetadataHandler<Metadata>> handlerMap;


    /**
     * Creates a ReflectiveRelMetadataProvider.
     *
     * @param map Map
     * @param metadataClass0 Metadata class
     * @param handlerMap Methods handled and the objects to call them on
     */
    protected ReflectiveAlgMetadataProvider( ConcurrentMap<Class<AlgNode>, UnboundMetadata<Metadata>> map, Class<Metadata> metadataClass0, Multimap<Method, MetadataHandler<Metadata>> handlerMap ) {
        assert !map.isEmpty() : "are your methods named wrong?";
        this.map = map;
        this.metadataClass0 = metadataClass0;
        this.handlerMap = ImmutableMultimap.copyOf( handlerMap );
    }


    /**
     * Returns an implementation of {@link AlgMetadataProvider} that scans for methods with a preceding argument.
     * <p>
     * For example, {@link BuiltInMetadata.Selectivity} has a method {@link BuiltInMetadata.Selectivity#getSelectivity(RexNode)}. A class
     *
     * <blockquote><pre><code>
     * class RelMdSelectivity {
     *   public Double getSelectivity(Union alg, RexNode predicate) { }
     *   public Double getSelectivity(Filter alg, RexNode predicate) { }
     * </code></pre></blockquote>
     *
     * provides implementations of selectivity for relational expressions that extend {@link org.polypheny.db.algebra.core.Union} or {@link org.polypheny.db.algebra.core.Filter}.
     */
    public static AlgMetadataProvider reflectiveSource( Method method, MetadataHandler<Metadata> target ) {
        return reflectiveSource( target, ImmutableList.of( method ) );
    }


    /**
     * Returns a reflective metadata provider that implements several methods.
     */
    public static AlgMetadataProvider reflectiveSource( MetadataHandler<?> target, Method... methods ) {
        return reflectiveSource( target, ImmutableList.copyOf( methods ) );
    }


    private static AlgMetadataProvider reflectiveSource( final MetadataHandler<?> target, final ImmutableList<Method> methods ) {
        final Space2 space = Space2.create( target, methods );

        // This needs to be a concurrent map since AlgMetadataProvider are cached in static fields, thus the map is subject to concurrent modifications later.
        // See map.put in org.polypheny.db.alg.metadata.ReflectiveRelMetadataProvider.apply(java.lang.Class<? extends org.polypheny.db.alg.AlgNode>)
        final ConcurrentMap<Class<AlgNode>, UnboundMetadata<Metadata>> methodsMap = new ConcurrentHashMap<>();
        for ( Class<AlgNode> key : space.classes ) {
            ImmutableNullableList.Builder<Method> builder = ImmutableNullableList.builder();
            for ( final Method method : methods ) {
                builder.add( space.find( key, method ) );
            }
            final List<Method> handlerMethods = builder.build();
            final UnboundMetadata<Metadata> function = ( alg, mq ) ->
                    (Metadata) Proxy.newProxyInstance(
                            space.metadataClass0.getClassLoader(),
                            new Class[]{ space.metadataClass0 }, ( proxy, method, args ) -> {
                                // Suppose we are an implementation of Selectivity that wraps "filter", a LogicalFilter. Then we implement
                                //   Selectivity.selectivity(rex)
                                // by calling method
                                //   new SelectivityImpl().selectivity(filter, rex)
                                if ( method.equals( BuiltInMethod.METADATA_ALG.method ) ) {
                                    return alg;
                                }
                                if ( method.equals( BuiltInMethod.OBJECT_TO_STRING.method ) ) {
                                    return space.metadataClass0.getSimpleName() + "(" + alg + ")";
                                }
                                int i = methods.indexOf( method );
                                if ( i < 0 ) {
                                    throw new AssertionError( "not handled: " + method + " for " + alg );
                                }
                                final Method handlerMethod = handlerMethods.get( i );
                                if ( handlerMethod == null ) {
                                    throw new AssertionError( "not handled: " + method + " for " + alg );
                                }
                                final Object[] args1;
                                final List<?> key1;
                                if ( args == null ) {
                                    args1 = new Object[]{ alg, mq };
                                    key1 = List.of( alg, method );
                                } else {
                                    args1 = new Object[args.length + 2];
                                    args1[0] = alg;
                                    args1[1] = mq;
                                    System.arraycopy( args, 0, args1, 2, args.length );

                                    final Object[] args2 = args1.clone();
                                    args2[1] = method; // replace RelMetadataQuery with method
                                    for ( int j = 0; j < args2.length; j++ ) {
                                        if ( args2[j] == null ) {
                                            args2[j] = NullSentinel.INSTANCE;
                                        } else if ( args2[j] instanceof RexNode ) {
                                            // Can't use RexNode.equals - it is not deep
                                            args2[j] = args2[j].toString();
                                        }
                                    }
                                    key1 = List.of( args2 );
                                }
                                if ( mq.map.put( key1, NullSentinel.INSTANCE ) != null ) {
                                    throw CyclicMetadataException.INSTANCE;
                                }
                                try {
                                    return handlerMethod.invoke( target, args1 );
                                } catch ( InvocationTargetException | UndeclaredThrowableException e ) {
                                    Util.throwIfUnchecked( e.getCause() );
                                    throw new RuntimeException( e.getCause() );
                                } finally {
                                    mq.map.remove( key1 );
                                }
                            } );
            methodsMap.put( key, function );
        }
        return new ReflectiveAlgMetadataProvider( methodsMap, space.metadataClass0, space.providers );
    }

    @Override
    public <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers( MetadataDef<M> def ) {
        final ImmutableMultimap.Builder<Method, MetadataHandler<M>> builder = ImmutableMultimap.builder();
        for ( Map.Entry<Method, MetadataHandler<Metadata>> entry : handlerMap.entries() ) {
            if ( def.methods.contains( entry.getKey() ) ) {
                //noinspection unchecked
                builder.put( entry.getKey(), (MetadataHandler<M>) entry.getValue() );
            }
        }
        return builder.build();
    }


    private static boolean couldImplement( Method handlerMethod, Method method ) {
        if ( !handlerMethod.getName().equals( method.getName() )
                || (handlerMethod.getModifiers() & Modifier.STATIC) != 0
                || (handlerMethod.getModifiers() & Modifier.PUBLIC) == 0 ) {
            return false;
        }
        final Class<?>[] parameterTypes1 = handlerMethod.getParameterTypes();
        final Class<?>[] parameterTypes = method.getParameterTypes();
        return parameterTypes1.length == parameterTypes.length + 2
                && AlgNode.class.isAssignableFrom( parameterTypes1[0] )
                && AlgMetadataQuery.class == parameterTypes1[1]
                && Arrays.asList( parameterTypes )
                .equals( Util.skip( Arrays.asList( parameterTypes1 ), 2 ) );
    }


    @Override
    public <M extends Metadata> UnboundMetadata<M> apply( Class<? extends AlgNode> algClass, Class<? extends M> metadataClass ) {
        if ( metadataClass == metadataClass0 ) {
            return apply( algClass );
        } else {
            return null;
        }
    }



    @SuppressWarnings({ "unchecked" })
    public <M extends Metadata> UnboundMetadata<M> apply( Class<? extends AlgNode> algClass ) {
        List<Class<? extends AlgNode>> newSources = new ArrayList<>();
        for ( ; ; ) {
            UnboundMetadata<Metadata> function = map.get( algClass );
            if ( function != null ) {
                for ( Class<?> clazz : newSources ) {
                    map.put( (Class<AlgNode>) clazz, function );
                }
                return (UnboundMetadata<M>) function;
            } else {
                newSources.add( algClass );
            }
            for ( Class<?> interfaceClass : algClass.getInterfaces() ) {
                if ( AlgNode.class.isAssignableFrom( interfaceClass ) ) {
                    final UnboundMetadata<Metadata> function2 = map.get( interfaceClass );
                    if ( function2 != null ) {
                        for ( Class<?> clazz : newSources ) {
                            map.put( (Class<AlgNode>) clazz, function2 );
                        }
                        return (UnboundMetadata<M>) function2;
                    }
                }
            }
            if ( AlgNode.class.isAssignableFrom( algClass.getSuperclass() ) ) {
                algClass = (Class<AlgNode>) algClass.getSuperclass();
            } else {
                return null;
            }
        }
    }


    /**
     * Workspace for computing which methods can act as handlers for given metadata methods.
     */
    static class Space {

        final Set<Class<AlgNode>> classes = new HashSet<>();
        final Map<Pair<Class<AlgNode>, Method>, Method> handlerMap = new HashMap<>();
        final ImmutableMultimap<Method, MetadataHandler<Metadata>> providers;


        Space( Multimap<Method, MetadataHandler<Metadata>> providers ) {
            this.providers = ImmutableMultimap.copyOf( providers );

            // Find the distinct set of {@link AlgNode} classes handled by this provider, ordered base-class first.
            for ( Map.Entry<Method, MetadataHandler<Metadata>> entry : providers.entries() ) {
                final Method method = entry.getKey();
                final MetadataHandler<?> provider = entry.getValue();
                for ( final Method handlerMethod : provider.getClass().getMethods() ) {
                    if ( couldImplement( handlerMethod, method ) ) {
                        @SuppressWarnings("unchecked") final Class<AlgNode> algNodeClass = (Class<AlgNode>) handlerMethod.getParameterTypes()[0];
                        classes.add( algNodeClass );
                        handlerMap.put( Pair.of( algNodeClass, method ), handlerMethod );
                    }
                }
            }
        }


        /**
         * Finds an implementation of a method for {@code AlgNodeClass} or its nearest base class. Assumes that base classes have already been added to {@code map}.
         */
        Method find( final Class<? extends AlgNode> algNodeClass, Method method ) {
            Objects.requireNonNull( algNodeClass );
            for ( Class<?> r = algNodeClass; ; ) {
                Method implementingMethod = handlerMap.get( Pair.of( r, method ) );
                if ( implementingMethod != null ) {
                    return implementingMethod;
                }
                for ( Class<?> clazz : r.getInterfaces() ) {
                    if ( AlgNode.class.isAssignableFrom( clazz ) ) {
                        implementingMethod = handlerMap.get( Pair.of( clazz, method ) );
                        if ( implementingMethod != null ) {
                            return implementingMethod;
                        }
                    }
                }
                r = r.getSuperclass();
                if ( r == null || !AlgNode.class.isAssignableFrom( r ) ) {
                    throw new IllegalArgumentException( "No handler for method [" + method + "] applied to argument of type [" + algNodeClass + "]; we recommend you create a catch-all (AlgNode) handler" );
                }
            }
        }

    }


    /**
     * Extended work space.
     */
    static class Space2 extends Space {

        private final Class<Metadata> metadataClass0;


        Space2( Class<Metadata> metadataClass0, ImmutableMultimap<Method, MetadataHandler<Metadata>> providerMap ) {
            super( providerMap );
            this.metadataClass0 = metadataClass0;
        }


        public static Space2 create( MetadataHandler<?> target, ImmutableList<Method> methods ) {
            assert !methods.isEmpty();
            final Method method0 = methods.get( 0 );
            //noinspection unchecked
            Class<Metadata> metadataClass0 = (Class<Metadata>) method0.getDeclaringClass();
            assert Metadata.class.isAssignableFrom( metadataClass0 );
            for ( Method method : methods ) {
                assert method.getDeclaringClass() == metadataClass0;
            }

            final ImmutableMultimap.Builder<Method, MetadataHandler<Metadata>> providerBuilder = ImmutableMultimap.builder();
            for ( final Method method : methods ) {
                //noinspection unchecked
                providerBuilder.put( method, (MetadataHandler<Metadata>) target );
            }
            return new Space2( metadataClass0, providerBuilder.build() );
        }

    }

}


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


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Primitive;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.codehaus.commons.compiler.ISimpleCompiler;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.enumerable.EnumerableAggregate;
import org.polypheny.db.algebra.enumerable.EnumerableFilter;
import org.polypheny.db.algebra.enumerable.EnumerableJoin;
import org.polypheny.db.algebra.enumerable.EnumerableLpgUnwind;
import org.polypheny.db.algebra.enumerable.EnumerableProject;
import org.polypheny.db.algebra.enumerable.EnumerableScan;
import org.polypheny.db.algebra.enumerable.EnumerableTransformer;
import org.polypheny.db.algebra.enumerable.common.EnumerableBatchIterator;
import org.polypheny.db.algebra.enumerable.common.EnumerableConditionalExecute;
import org.polypheny.db.algebra.enumerable.common.EnumerableModifyCollect;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.common.LogicalContextSwitcher;
import org.polypheny.db.algebra.logical.common.LogicalStreamer;
import org.polypheny.db.algebra.logical.common.LogicalTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.logical.document.LogicalDocumentTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgTransformer;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnion;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnwind;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.logical.relational.LogicalCalc;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalRelExchange;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelMinus;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelTableFunctionScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.logical.relational.LogicalWindow;
import org.polypheny.db.algebra.stream.LogicalChi;
import org.polypheny.db.algebra.stream.LogicalDelta;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.interpreter.JaninoRexCompiler;
import org.polypheny.db.plan.hep.HepAlgVertex;
import org.polypheny.db.plan.volcano.AbstractConverter;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.ControlFlowException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.SaffronProperties;
import org.polypheny.db.util.Util;


/**
 * Implementation of the {@link AlgMetadataProvider} interface that generates a class that dispatches to the underlying providers.
 */
public class JaninoRelMetadataProvider implements AlgMetadataProvider {

    private final AlgMetadataProvider provider;

    public static final JaninoRelMetadataProvider DEFAULT = JaninoRelMetadataProvider.of( DefaultAlgMetadataProvider.INSTANCE );

    private static final Set<Class<? extends AlgNode>> ALL_ALGS = new CopyOnWriteArraySet<>();

    /**
     * Cache of pre-generated handlers by provider and kind of metadata.
     * For the cache to be effective, providers should implement identity correctly.
     */
    private static final LoadingCache<Key, MetadataHandler<? extends Metadata>> HANDLERS =
            maxSize( CacheBuilder.newBuilder(), SaffronProperties.INSTANCE.metadataHandlerCacheMaximumSize().get() )
                    .build( CacheLoader.from( key -> load3( key.def, key.provider.handlers( key.def ), key.algClasses ) ) );


    // Pre-register the most common relational operators, to reduce the number of times we re-generate.
    static {
        DEFAULT.register(
                Arrays.asList(
                        AlgNode.class,
                        AbstractAlgNode.class,
                        AlgSubset.class,
                        HepAlgVertex.class,
                        ConverterImpl.class,
                        AbstractConverter.class,

                        LogicalRelAggregate.class,
                        LogicalCalc.class,
                        LogicalRelCorrelate.class,
                        LogicalRelExchange.class,
                        LogicalRelFilter.class,
                        LogicalRelIntersect.class,
                        LogicalRelJoin.class,
                        LogicalRelMinus.class,
                        LogicalRelProject.class,
                        LogicalRelSort.class,
                        LogicalRelTableFunctionScan.class,
                        LogicalRelModify.class,
                        LogicalRelScan.class,
                        LogicalRelUnion.class,
                        LogicalRelValues.class,
                        LogicalWindow.class,
                        LogicalChi.class,
                        LogicalDelta.class,

                        // Common
                        LogicalTransformer.class,
                        LogicalStreamer.class,
                        LogicalContextSwitcher.class,
                        LogicalConditionalExecute.class,
                        LogicalConstraintEnforcer.class,

                        // LPG
                        LogicalLpgScan.class,
                        LogicalLpgAggregate.class,
                        LogicalLpgFilter.class,
                        LogicalLpgMatch.class,
                        LogicalLpgModify.class,
                        LogicalLpgProject.class,
                        LogicalLpgSort.class,
                        LogicalLpgTransformer.class,
                        LogicalLpgUnion.class,
                        LogicalLpgUnwind.class,
                        LogicalLpgValues.class,

                        // Document
                        LogicalDocumentScan.class,
                        LogicalDocumentAggregate.class,
                        LogicalDocumentFilter.class,
                        LogicalDocumentProject.class,
                        LogicalDocumentModify.class,
                        LogicalDocumentSort.class,
                        LogicalDocumentTransformer.class,
                        LogicalDocumentValues.class,

                        // Enumerable
                        EnumerableAggregate.class,
                        EnumerableFilter.class,
                        EnumerableProject.class,
                        EnumerableJoin.class,
                        EnumerableScan.class,
                        EnumerableLpgUnwind.class,
                        EnumerableTransformer.class,
                        EnumerableTransformer.class,
                        EnumerableBatchIterator.class,
                        EnumerableConditionalExecute.class,
                        EnumerableModifyCollect.class ) );
    }


    /**
     * Private constructor; use {@link #of}.
     */
    private JaninoRelMetadataProvider( AlgMetadataProvider provider ) {
        this.provider = provider;
    }


    /**
     * Creates a JaninoRelMetadataProvider.
     *
     * @param provider Underlying provider
     */
    public static JaninoRelMetadataProvider of( AlgMetadataProvider provider ) {
        if ( provider instanceof JaninoRelMetadataProvider ) {
            return (JaninoRelMetadataProvider) provider;
        }
        return new JaninoRelMetadataProvider( provider );
    }


    // helper for initialization
    private static <K, V> CacheBuilder<K, V> maxSize( CacheBuilder<K, V> builder, int size ) {
        if ( size >= 0 ) {
            builder.maximumSize( size );
        }
        return builder;
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof JaninoRelMetadataProvider
                && ((JaninoRelMetadataProvider) obj).provider.equals( provider );
    }


    @Override
    public int hashCode() {
        return 109 + provider.hashCode();
    }


    @Override
    public <M extends Metadata> UnboundMetadata<M> apply( Class<? extends AlgNode> algClass, Class<? extends M> metadataClass ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <M extends Metadata> Multimap<Method, MetadataHandler<M>>
    handlers( MetadataDef<M> def ) {
        return provider.handlers( def );
    }


    private static <M extends Metadata> MetadataHandler<M> load3( MetadataDef<M> def, Multimap<Method, MetadataHandler<M>> map, ImmutableList<Class<? extends AlgNode>> algClasses ) {
        final StringBuilder buff = new StringBuilder();
        final String name = "GeneratedMetadataHandler_" + def.metadataClass.getSimpleName();
        final Set<MetadataHandler<?>> providerSet = new HashSet<>();
        final List<Pair<String, MetadataHandler<?>>> providerList = new ArrayList<>();
        //noinspection unchecked
        final ReflectiveAlgMetadataProvider.Space space = new ReflectiveAlgMetadataProvider.Space( (Multimap) map );
        for ( MetadataHandler<?> provider : space.providers.values() ) {
            if ( providerSet.add( provider ) ) {
                providerList.add( Pair.of( "provider" + (providerSet.size() - 1), provider ) );
            }
        }

        buff.append( "  private final java.util.List relClasses;\n" );
        for ( Pair<String, MetadataHandler<?>> pair : providerList ) {
            buff.append( "  public final " ).append( pair.right.getClass().getName() )
                    .append( ' ' ).append( pair.left ).append( ";\n" );
        }
        buff.append( "  public " ).append( name ).append( "(java.util.List relClasses" );
        for ( Pair<String, MetadataHandler<?>> pair : providerList ) {
            buff.append( ",\n" )
                    .append( "      " )
                    .append( pair.right.getClass().getName() )
                    .append( ' ' )
                    .append( pair.left );
        }
        buff.append( ") {\n" )
                .append( "    this.relClasses = relClasses;\n" );

        for ( Pair<String, MetadataHandler<?>> pair : providerList ) {
            buff.append( "    this." )
                    .append( pair.left )
                    .append( " = " )
                    .append( pair.left )
                    .append( ";\n" );
        }
        buff.append( "  }\n" )
                .append( "  public " )
                .append( MetadataDef.class.getName() )
                .append( " getDef() {\n" )
                .append( "    return " )
                .append( def.metadataClass.getName() )
                .append( ".DEF;\n" )
                .append( "  }\n" );
        for ( Ord<Method> method : Ord.zip( def.methods ) ) {
            buff.append( "  public " )
                    .append( method.e.getReturnType().getName() )
                    .append( " " )
                    .append( method.e.getName() )
                    .append( "(\n" )
                    .append( "      " )
                    .append( AlgNode.class.getName() )
                    .append( " r,\n" )
                    .append( "      " )
                    .append( AlgMetadataQuery.class.getName() )
                    .append( " mq" );
            paramList( buff, method.e )
                    .append( ") {\n" );
            buff.append( "    final java.util.List key = " )
                    .append( ImmutableList.class.getName() ) // this should be re-evaluated, if it is even needed anymore
                    .append( ".of(" )
                    .append( def.metadataClass.getName() );
            if ( method.i == 0 ) {
                buff.append( ".DEF" );
            } else {
                buff.append( ".DEF.methods.get(" )
                        .append( method.i )
                        .append( ")" );
            }
            buff.append( ", r" );
            safeArgList( buff, method.e )
                    .append( ");\n" )
                    .append( "    final Object v = mq.map.get(key);\n" )
                    .append( "    if (v != null) {\n" )
                    .append( "      if (v == " )
                    .append( NullSentinel.class.getName() )
                    .append( ".ACTIVE) {\n" )
                    .append( "        throw " )
                    .append( CyclicMetadataException.class.getName() )
                    .append( ".INSTANCE;\n" )
                    .append( "      }\n" )
                    .append( "      if (v == " )
                    .append( NullSentinel.class.getName() )
                    .append( ".INSTANCE) {\n" )
                    .append( "        return null;\n" )
                    .append( "      }\n" )
                    .append( "      return (" )
                    .append( method.e.getReturnType().getName() )
                    .append( ") v;\n" )
                    .append( "    }\n" )
                    .append( "    mq.map.put(key," )
                    .append( NullSentinel.class.getName() )
                    .append( ".ACTIVE);\n" )
                    .append( "    try {\n" )
                    .append( "      final " )
                    .append( method.e.getReturnType().getName() )
                    .append( " x = " )
                    .append( method.e.getName() )
                    .append( "_(r, mq" );
            argList( buff, method.e )
                    .append( ");\n" )
                    .append( "      mq.map.put(key, " )
                    .append( NullSentinel.class.getName() )
                    .append( ".mask(x));\n" )
                    .append( "      return x;\n" )
                    .append( "    } catch (" )
                    .append( Exception.class.getName() )
                    .append( " e) {\n" )
                    .append( "      mq.map.remove(key);\n" )
                    .append( "      throw e;\n" )
                    .append( "    }\n" )
                    .append( "  }\n" )
                    .append( "\n" )
                    .append( "  private " )
                    .append( method.e.getReturnType().getName() )
                    .append( " " )
                    .append( method.e.getName() )
                    .append( "_(\n" )
                    .append( "      " )
                    .append( AlgNode.class.getName() )
                    .append( " r,\n" )
                    .append( "      " )
                    .append( AlgMetadataQuery.class.getName() )
                    .append( " mq" );
            paramList( buff, method.e )
                    .append( ") {\n" );
            buff.append( "    switch (relClasses.indexOf(r.getClass())) {\n" );

            // Build a list of clauses, grouping clauses that have the same action.
            final Multimap<String, Integer> clauses = LinkedHashMultimap.create();
            final StringBuilder buf2 = new StringBuilder();
            for ( Ord<Class<? extends AlgNode>> algClass : Ord.zip( algClasses ) ) {
                if ( algClass.e == HepAlgVertex.class ) {
                    buf2.append( "      return " )
                            .append( method.e.getName() )
                            .append( "(((" )
                            .append( algClass.e.getName() )
                            .append( ") r).getCurrentAlg(), mq" );
                    argList( buf2, method.e )
                            .append( ");\n" );
                } else {
                    final Method handler = space.find( algClass.e, method.e );
                    final String v = findProvider( providerList, handler.getDeclaringClass() );
                    buf2.append( "      return " )
                            .append( v )
                            .append( "." )
                            .append( method.e.getName() )
                            .append( "((" )
                            .append( handler.getParameterTypes()[0].getName() )
                            .append( ") r, mq" );
                    argList( buf2, method.e )
                            .append( ");\n" );
                }
                clauses.put( buf2.toString(), algClass.i );
                buf2.setLength( 0 );
            }
            buf2.append( "      throw new " )
                    .append( NoHandler.class.getName() )
                    .append( "(r.getClass());\n" )
                    .append( "    }\n" )
                    .append( "  }\n" );
            clauses.put( buf2.toString(), -1 );
            for ( Map.Entry<String, Collection<Integer>> pair : clauses.asMap().entrySet() ) {
                if ( pair.getValue().contains( algClasses.indexOf( AlgNode.class ) ) ) {
                    buff.append( "    default:\n" );
                } else {
                    for ( Integer integer : pair.getValue() ) {
                        buff.append( "    case " ).append( integer ).append( ":\n" );
                    }
                }
                buff.append( pair.getKey() );
            }
        }
        final List<Object> argList = new ArrayList<>( Pair.right( providerList ) );
        argList.add( 0, ImmutableList.copyOf( algClasses ) );
        try {
            return compile( name, buff.toString(), def, argList );
        } catch ( CompileException | IOException e ) {
            throw new RuntimeException( "Error compiling:\n" + buff, e );
        }
    }


    private static String findProvider( List<Pair<String, MetadataHandler<?>>> providerList, Class<?> declaringClass ) {
        for ( Pair<String, MetadataHandler<?>> pair : providerList ) {
            if ( declaringClass.isInstance( pair.right ) ) {
                return pair.left;
            }
        }
        throw new AssertionError( "not found: " + declaringClass );
    }


    /**
     * Returns e.g. ", ignoreNulls".
     */
    private static StringBuilder argList( StringBuilder buff, Method method ) {
        for ( Ord<Class<?>> t : Ord.zip( method.getParameterTypes() ) ) {
            buff.append( ", a" ).append( t.i );
        }
        return buff;
    }


    /**
     * Returns e.g. ", ignoreNulls".
     */
    private static StringBuilder safeArgList( StringBuilder buff, Method method ) {
        for ( Ord<Class<?>> t : Ord.zip( method.getParameterTypes() ) ) {
            if ( Primitive.is( t.e ) ) {
                buff.append( ", a" ).append( t.i );
            } else if ( RexNode.class.isAssignableFrom( t.e ) ) {
                // For RexNode, convert to string, because equals does not look deep.
                //   a1 == null ? "" : a1.toString()
                buff.append( ", a" )
                        .append( t.i )
                        .append( " == null ? \"\" : a" )
                        .append( t.i )
                        .append( ".toString()" );
            } else {
                buff.append( ", " )
                        .append( NullSentinel.class.getName() )
                        .append( ".mask(a" )
                        .append( t.i )
                        .append( ")" );
            }
        }
        return buff;
    }


    /**
     * Returns e.g. ",\n boolean ignoreNulls".
     */
    private static StringBuilder paramList( StringBuilder buff, Method method ) {
        for ( Ord<Class<?>> t : Ord.zip( method.getParameterTypes() ) ) {
            buff.append( ",\n      " )
                    .append( t.e.getName() )
                    .append( " a" )
                    .append( t.i );
        }
        return buff;
    }


    static <M extends Metadata> MetadataHandler<M> compile( String className, String classBody, MetadataDef<M> def, List<Object> argList ) throws CompileException, IOException {
        final ICompilerFactory compilerFactory;
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch ( Exception e ) {
            throw new IllegalStateException( "Unable to instantiate java compiler", e );
        }

        final ISimpleCompiler compiler = compilerFactory.newSimpleCompiler();
        compiler.setParentClassLoader( JaninoRexCompiler.class.getClassLoader() );

        final String s = "public final class " + className + " implements " + def.handlerClass.getCanonicalName() + " {\n" + classBody + "\n" + "}";

        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            // Add line numbers to the generated janino class
            compiler.setDebuggingInformation( true, true, true );
            System.out.println( s );
        }

        compiler.cook( s );
        final Constructor<?> constructor;
        final Object o;
        try {
            constructor = compiler.getClassLoader().loadClass( className ).getDeclaredConstructors()[0];
            o = constructor.newInstance( argList.toArray() );
        } catch ( InstantiationException | IllegalAccessException | InvocationTargetException | ClassNotFoundException e ) {
            throw new RuntimeException( e );
        }
        return def.handlerClass.cast( o );
    }


    synchronized <M extends Metadata, H extends MetadataHandler<M>> H create( MetadataDef<M> def ) {
        try {
            final Key<?> key = new Key<>( def, provider, ImmutableList.copyOf( ALL_ALGS ) );
            //noinspection unchecked
            return (H) HANDLERS.get( key );
        } catch ( UncheckedExecutionException | ExecutionException e ) {
            Util.throwIfUnchecked( e.getCause() );
            throw new RuntimeException( e.getCause() );
        }
    }


    synchronized <M extends Metadata, H extends MetadataHandler<M>> H revise( Class<? extends AlgNode> rClass, MetadataDef<M> def ) {
        if ( ALL_ALGS.add( rClass ) ) {
            HANDLERS.invalidateAll();
        }
        return create( def );
    }


    /**
     * Registers some classes. Does not flush the providers, but next time we need to generate a provider, it will handle all of these classes.
     * So, calling this method reduces the number of times we need to re-generate.
     */
    public void register( Iterable<Class<? extends AlgNode>> classes ) {
        // Register the classes and their base classes up to AlgNode. Don't bother to remove duplicates; addAll will do that.
        final List<Class<? extends AlgNode>> list = Lists.newArrayList( classes );
        for ( int i = 0; i < list.size(); i++ ) {
            final Class<? extends AlgNode> c = list.get( i );
            final Class<?> s = c.getSuperclass();
            if ( s != null && AlgNode.class.isAssignableFrom( s ) ) {
                //noinspection unchecked
                list.add( (Class<? extends AlgNode>) s );
            }
        }
        synchronized ( this ) {
            if ( ALL_ALGS.addAll( list ) ) {
                HANDLERS.invalidateAll();
            }
        }
    }


    /**
     * Exception that indicates there should be a handler for this class but there is not. The action is probably to re-generate the handler class.
     */
    public static class NoHandler extends ControlFlowException {

        public final Class<? extends AlgNode> algClass;


        public NoHandler( Class<? extends AlgNode> algClass ) {
            this.algClass = algClass;
        }

    }


    /**
     * Key for the cache.
     */
    private static class Key<M extends Metadata> {

        public final MetadataDef<M> def;
        public final AlgMetadataProvider provider;
        public final ImmutableList<Class<? extends AlgNode>> algClasses;


        private Key( MetadataDef<M> def, AlgMetadataProvider provider, ImmutableList<Class<? extends AlgNode>> algClassList ) {
            this.def = def;
            this.provider = provider;
            this.algClasses = algClassList;
        }


        @Override
        public int hashCode() {
            return (def.hashCode() * 37 + provider.hashCode()) * 37 + algClasses.hashCode();
        }


        @Override
        public boolean equals( Object obj ) {
            return this == obj
                    || obj instanceof Key
                    && ((Key<?>) obj).def.equals( def )
                    && ((Key<?>) obj).provider.equals( provider )
                    && ((Key<?>) obj).algClasses.equals( algClasses );
        }

    }

}


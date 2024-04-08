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

package org.polypheny.db.interpreter;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.TransformedEnumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgProducingVisitor.AlgConsumingVisitor;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.rules.CalcSplitRule;
import org.polypheny.db.algebra.rules.FilterScanRule;
import org.polypheny.db.algebra.rules.ProjectScanRule;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.hep.HepPlanner;
import org.polypheny.db.plan.hep.HepProgram;
import org.polypheny.db.plan.hep.HepProgramBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Interpreter.
 * <p>
 * Contains the context for interpreting relational expressions. In particular it holds working state while the data flow graph is being assembled.
 */
@Slf4j
public class Interpreter extends AbstractEnumerable<PolyValue[]> implements AutoCloseable {

    private final Map<AlgNode, NodeInfo<PolyValue>> nodes;
    private final DataContext dataContext;
    private final AlgNode algRoot;


    /**
     * Creates an Interpreter.
     */
    public Interpreter( DataContext dataContext, AlgNode algRoot ) {
        this.dataContext = Objects.requireNonNull( dataContext );
        final AlgNode alg = optimize( algRoot );
        final CompilerImpl compiler = new Nodes.CoreCompiler( this, algRoot.getCluster() );
        Pair<AlgNode, Map<AlgNode, NodeInfo<PolyValue>>> pair = compiler.visitRoot( alg );
        this.algRoot = pair.left;
        this.nodes = ImmutableMap.copyOf( pair.right );
    }


    private AlgNode optimize( AlgNode rootAlg ) {
        final HepProgram hepProgram =
                new HepProgramBuilder()
                        .addRuleInstance( CalcSplitRule.INSTANCE )
                        .addRuleInstance( FilterScanRule.INSTANCE )
                        .addRuleInstance( FilterScanRule.INTERPRETER )
                        .addRuleInstance( ProjectScanRule.INSTANCE )
                        .addRuleInstance( ProjectScanRule.INTERPRETER )
                        .build();
        final HepPlanner planner = new HepPlanner( hepProgram );
        planner.setRoot( rootAlg );
        rootAlg = planner.findBestExp();
        return rootAlg;
    }


    @Override
    public Enumerator<PolyValue[]> enumerator() {
        start();
        final NodeInfo<PolyValue> nodeInfo = nodes.get( algRoot );
        final Enumerator<Row<PolyValue>> rows;
        if ( nodeInfo.rowEnumerable != null ) {
            rows = nodeInfo.rowEnumerable.enumerator();
        } else {
            final ArrayDeque<Row<PolyValue>> queue = Iterables.getOnlyElement( nodeInfo.sinks.values() ).list;
            rows = Linq4j.iterableEnumerator( queue );
        }

        return new TransformedEnumerator<>( rows ) {
            @Override
            protected PolyValue[] transform( Row<PolyValue> row ) {
                return row.getValues();
            }
        };
    }


    private void start() {
        // We rely on the nodes being ordered leaves first.
        for ( Map.Entry<AlgNode, NodeInfo<PolyValue>> entry : nodes.entrySet() ) {
            final NodeInfo<PolyValue> nodeInfo = entry.getValue();
            try {
                nodeInfo.node.run();
            } catch ( InterruptedException e ) {
                log.error( "Caught exception", e );
            }
        }
    }


    @Override
    public void close() {
    }


    /**
     * Information about a node registered in the data flow graph.
     */
    private static class NodeInfo<T> {

        final AlgNode alg;
        final Map<Edge, ListSink> sinks = new LinkedHashMap<>();
        final Enumerable<Row<T>> rowEnumerable;
        Node node;


        NodeInfo( AlgNode alg, Enumerable<Row<T>> rowEnumerable ) {
            this.alg = alg;
            this.rowEnumerable = rowEnumerable;
        }

    }


    /**
     * A {@link Source} that is just backed by an {@link Enumerator}. The {@link Enumerator} is closed when it is finished or by calling {@link #close()}.
     */
    private static class EnumeratorSource<T> implements Source<T> {

        private final Enumerator<Row<T>> enumerator;


        EnumeratorSource( final Enumerator<Row<T>> enumerator ) {
            this.enumerator = Objects.requireNonNull( enumerator );
        }


        @Override
        public Row<T> receive() {
            if ( enumerator.moveNext() ) {
                return enumerator.current();
            }
            // close the enumerator once we have gone through everything
            enumerator.close();
            return null;
        }


        @Override
        public void close() {
            enumerator.close();
        }

    }


    /**
     * Implementation of {@link Sink} using a {@link java.util.ArrayDeque}.
     */
    private static class ListSink implements Sink {

        final ArrayDeque<Row<PolyValue>> list;


        private ListSink( ArrayDeque<Row<PolyValue>> list ) {
            this.list = list;
        }


        @Override
        public void send( Row<PolyValue> row ) {
            list.add( row );
        }


        @Override
        public void end() throws InterruptedException {
        }

    }


    /**
     * Implementation of {@link Source} using a {@link java.util.ArrayDeque}.
     */
    private static class ListSource implements Source<PolyValue> {

        private final ArrayDeque<Row<PolyValue>> list;
        private Iterator<Row<PolyValue>> iterator = null;


        ListSource( ArrayDeque<Row<PolyValue>> list ) {
            this.list = list;
        }


        @Override
        public Row<PolyValue> receive() {
            try {
                if ( iterator == null ) {
                    iterator = list.iterator();
                }
                return iterator.next();
            } catch ( NoSuchElementException e ) {
                iterator = null;
                return null;
            }
        }


        @Override
        public void close() {
            // noop
        }

    }


    /**
     * Implementation of {@link Sink} using a {@link java.util.ArrayDeque}.
     */
    private static class DuplicatingSink implements Sink {

        private final List<ArrayDeque<Row<PolyValue>>> queues;


        private DuplicatingSink( List<ArrayDeque<Row<PolyValue>>> queues ) {
            this.queues = ImmutableList.copyOf( queues );
        }


        @Override
        public void send( Row<PolyValue> row ) {
            for ( ArrayDeque<Row<PolyValue>> queue : queues ) {
                queue.add( row );
            }
        }


        @Override
        public void end() {
        }

    }


    /**
     * Walks over a tree of {@link AlgNode} and, for each, creates a {@link Node} that can be executed in the interpreter.
     * <p>
     * The compiler looks for methods of the form "visit(XxxRel)". A "visit" method must create an appropriate {@link Node} and put it into the {@link #node} field.
     * <p>
     * If you wish to handle more kinds of relational expressions, add extra "visit" methods in this or a sub-class, and they will be found and called via reflection.
     */
    static class CompilerImpl extends AlgVisitor implements Compiler, AlgConsumingVisitor {

        final ScalarCompiler scalarCompiler;
        protected final Interpreter interpreter;
        protected AlgNode rootAlg;
        protected AlgNode alg;
        protected Node node;
        final Map<AlgNode, NodeInfo<PolyValue>> nodes = new LinkedHashMap<>();
        final Map<AlgNode, List<AlgNode>> algInputs = new HashMap<>();
        final Multimap<AlgNode, Edge> outEdges = LinkedHashMultimap.create();

        //private static final String REWRITE_METHOD_NAME = "rewrite";
        //private static final String VISIT_METHOD_NAME = "visit";


        CompilerImpl( Interpreter interpreter, AlgCluster cluster ) {
            this.interpreter = interpreter;
            this.scalarCompiler = new JaninoRexCompiler( cluster.getRexBuilder() );
        }


        @Getter
        final ImmutableMap<Class<? extends AlgNode>, Consumer<AlgNode>> handlers = ImmutableMap.of();

        @Getter
        final Consumer<AlgNode> defaultHandler = a -> {
            throw new AssertionError( "interpreter: no implementation for " + a.getClass() );
        };


        /**
         * Visits the tree, starting from the root {@code p}.
         */
        Pair<AlgNode, Map<AlgNode, NodeInfo<PolyValue>>> visitRoot( AlgNode p ) {
            rootAlg = p;
            visit( p, 0, null );
            return Pair.of( rootAlg, nodes );
        }


        @Override
        public void visit( AlgNode p, int ordinal, AlgNode parent ) {
            for ( ; ; ) {
                alg = null;
                /*boolean found = dispatcher.invokeVisitor( this, p, REWRITE_METHOD_NAME );
                if ( !found ) {
                    throw new AssertionError( "interpreter: no implementation for rewrite" ); // this was never used
                }*/
                if ( alg == null ) {
                    break;
                }
                if ( RuntimeConfig.DEBUG.getBoolean() ) {
                    log.warn( "Interpreter: rewrite " + p + " to " + alg );
                }
                p = alg;
                if ( parent != null ) {
                    List<AlgNode> inputs = algInputs.get( parent );
                    if ( inputs == null ) {
                        inputs = Lists.newArrayList( parent.getInputs() );
                        algInputs.put( parent, inputs );
                    }
                    inputs.set( ordinal, p );
                } else {
                    rootAlg = p;
                }
            }

            // rewrite children first (from left to right)
            final List<AlgNode> inputs = algInputs.get( p );
            for ( Ord<AlgNode> input : Ord.zip( Util.first( inputs, p.getInputs() ) ) ) {
                outEdges.put( input.e, new Edge( p, input.i ) );
            }
            if ( inputs != null ) {
                for ( int i = 0; i < inputs.size(); i++ ) {
                    AlgNode input = inputs.get( i );
                    visit( input, i, p );
                }
            } else {
                p.childrenAccept( this );
            }

            node = null;
            boolean found = findHandler( p.getClass() ) != null;
            if ( found ) {
                this.handle( p );
            } else {
                if ( p instanceof InterpretableAlg interpretableAlg ) {
                    node = interpretableAlg.implement( new InterpretableAlg.InterpreterImplementor( this, null ) );
                } else {
                    // Probably need to add a visit(XxxRel) method to CoreCompiler.
                    throw new AssertionError( "interpreter: no implementation for " + p.getClass() );
                }
            }
            final NodeInfo<PolyValue> nodeInfo = nodes.get( p );
            assert nodeInfo != null;
            nodeInfo.node = node;
            if ( inputs != null ) {
                for ( int i = 0; i < inputs.size(); i++ ) {
                    final AlgNode input = inputs.get( i );
                    visit( input, i, p );
                }
            }
        }


        /**
         * Fallback rewrite method.
         * <p>
         * Overriding methods (each with a different sub-class of {@link AlgNode} as its argument type) sets the {@link #alg} field if intends to rewrite.
         */
        public void rewrite( AlgNode r ) {
        }


        @Override
        public Scalar compile( List<RexNode> nodes, AlgDataType inputRowType ) {
            if ( inputRowType == null ) {
                inputRowType = interpreter.dataContext.getTypeFactory().builder().build();
            }
            return scalarCompiler.compile( nodes, inputRowType, interpreter.dataContext );
        }


        @Override
        public AlgDataType combinedRowType( List<AlgNode> inputs ) {
            final Builder builder = interpreter.dataContext.getTypeFactory().builder();
            for ( AlgNode input : inputs ) {
                builder.addAll( input.getTupleType().getFields() );
            }
            return builder.build();
        }


        @Override
        public Source<PolyValue> source( AlgNode alg, int ordinal ) {
            final AlgNode input = getInput( alg, ordinal );
            final Edge edge = new Edge( alg, ordinal );
            final Collection<Edge> edges = outEdges.get( input );
            final NodeInfo<PolyValue> nodeInfo = nodes.get( input );
            if ( nodeInfo == null ) {
                throw new AssertionError( "should be registered: " + alg );
            }
            if ( nodeInfo.rowEnumerable != null ) {
                return new EnumeratorSource<>( nodeInfo.rowEnumerable.enumerator() );
            }
            assert nodeInfo.sinks.size() == edges.size();
            final ListSink sink = nodeInfo.sinks.get( edge );
            if ( sink != null ) {
                return new ListSource( sink.list );
            }
            throw new IllegalStateException( "Got a sink " + sink + " to which there is no match source type!" );
        }


        private AlgNode getInput( AlgNode alg, int ordinal ) {
            final List<AlgNode> inputs = algInputs.get( alg );
            if ( inputs != null ) {
                return inputs.get( ordinal );
            }
            return alg.getInput( ordinal );
        }


        @Override
        public Sink sink( AlgNode alg ) {
            final Collection<Edge> edges = outEdges.get( alg );
            final Collection<Edge> edges2 =
                    edges.isEmpty()
                            ? ImmutableList.of( new Edge( null, 0 ) )
                            : edges;
            NodeInfo<PolyValue> nodeInfo = nodes.get( alg );
            if ( nodeInfo == null ) {
                nodeInfo = new NodeInfo<>( alg, null );
                nodes.put( alg, nodeInfo );
                for ( Edge edge : edges2 ) {
                    nodeInfo.sinks.put( edge, new ListSink( new ArrayDeque<>() ) );
                }
            }
            if ( edges.size() == 1 ) {
                return Iterables.getOnlyElement( nodeInfo.sinks.values() );
            } else {
                final List<ArrayDeque<Row<PolyValue>>> queues = new ArrayList<>();
                for ( ListSink sink : nodeInfo.sinks.values() ) {
                    queues.add( sink.list );
                }
                return new DuplicatingSink( queues );
            }
        }


        @Override
        public void enumerable( AlgNode alg, Enumerable<Row<PolyValue>> rowEnumerable ) {
            NodeInfo<PolyValue> nodeInfo = new NodeInfo<>( alg, rowEnumerable );
            nodes.put( alg, nodeInfo );
        }


        @Override
        public Context<PolyValue> createContext() {
            return new Context<>( getDataContext() );
        }


        @Override
        public DataContext getDataContext() {
            return interpreter.dataContext;
        }

    }


    /**
     * Edge between a {@link AlgNode} and one of its inputs.
     */
    static class Edge extends Pair<AlgNode, Integer> {

        Edge( AlgNode parent, int ordinal ) {
            super( parent, ordinal );
        }

    }


    /**
     * Converts a list of expressions to a scalar that can compute their values.
     */
    interface ScalarCompiler {

        Scalar compile( List<RexNode> nodes, AlgDataType inputRowType, DataContext dataContext );

    }

}


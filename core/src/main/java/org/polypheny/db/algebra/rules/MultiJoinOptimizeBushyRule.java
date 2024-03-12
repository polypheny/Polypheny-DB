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

package org.polypheny.db.algebra.rules;


import static org.polypheny.db.algebra.rules.LoptMultiJoin.Edge;
import static org.polypheny.db.util.mapping.Mappings.TargetMapping;

import com.google.common.collect.ImmutableList;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.metadata.AlgMdUtil;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexPermuteInputsShuttle;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.rex.RexVisitor;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Planner rule that finds an approximately optimal ordering for join operators using a heuristic algorithm.
 *
 * It is triggered by the pattern {@link LogicalRelProject} ({@link MultiJoin}).
 *
 * It is similar to {@link LoptOptimizeJoinRule}. {@code LoptOptimizeJoinRule} is only capable of producing left-deep joins; this rule is capable of producing bushy joins.
 *
 * TODO:
 * <ol>
 * <li>Join conditions that touch 1 factor.</li>
 * <li>Join conditions that touch 3 factors.</li>
 * <li>More than 1 join conditions that touch the same pair of factors, e.g. {@code t0.c1 = t1.c1 and t1.c2 = t0.c3}</li>
 * </ol>
 */
public class MultiJoinOptimizeBushyRule extends AlgOptRule {

    public static final MultiJoinOptimizeBushyRule INSTANCE = new MultiJoinOptimizeBushyRule( AlgFactories.LOGICAL_BUILDER );

    private final PrintWriter pw = RuntimeConfig.DEBUG.getBoolean()
            ? Util.printWriter( System.out )
            : null;


    /**
     * Creates an MultiJoinOptimizeBushyRule.
     */
    public MultiJoinOptimizeBushyRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( MultiJoin.class, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final MultiJoin multiJoinRel = call.alg( 0 );
        final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
        final AlgBuilder algBuilder = call.builder();
        final AlgMetadataQuery mq = call.getMetadataQuery();

        final LoptMultiJoin multiJoin = new LoptMultiJoin( multiJoinRel );

        final List<Vertex> vertexes = new ArrayList<>();
        int x = 0;
        for ( int i = 0; i < multiJoin.getNumJoinFactors(); i++ ) {
            final AlgNode alg = multiJoin.getJoinFactor( i );
            double cost = mq.getTupleCount( alg );
            vertexes.add( new LeafVertex( i, alg, cost, x ) );
            x += alg.getTupleType().getFieldCount();
        }
        assert x == multiJoin.getNumTotalFields();

        final List<Edge> unusedEdges = new ArrayList<>();
        for ( RexNode node : multiJoin.getJoinFilters() ) {
            unusedEdges.add( multiJoin.createEdge( node ) );
        }

        // Comparator that chooses the best edge. A "good edge" is one that has a large difference in the number of rows on LHS and RHS.
        final Comparator<LoptMultiJoin.Edge> edgeComparator =
                new Comparator<LoptMultiJoin.Edge>() {
                    @Override
                    public int compare( LoptMultiJoin.Edge e0, LoptMultiJoin.Edge e1 ) {
                        return Double.compare( rowCountDiff( e0 ), rowCountDiff( e1 ) );
                    }


                    private double rowCountDiff( LoptMultiJoin.Edge edge ) {
                        assert edge.factors.cardinality() == 2 : edge.factors;
                        final int factor0 = edge.factors.nextSetBit( 0 );
                        final int factor1 = edge.factors.nextSetBit( factor0 + 1 );
                        return Math.abs( vertexes.get( factor0 ).cost - vertexes.get( factor1 ).cost );
                    }
                };

        final List<Edge> usedEdges = new ArrayList<>();
        for ( ; ; ) {
            final int edgeOrdinal = chooseBestEdge( unusedEdges, edgeComparator );
            if ( pw != null ) {
                trace( vertexes, unusedEdges, usedEdges, edgeOrdinal, pw );
            }
            final int[] factors;
            if ( edgeOrdinal == -1 ) {
                // No more edges. Are there any un-joined vertexes?
                final Vertex lastVertex = Util.last( vertexes );
                final int z = lastVertex.factors.previousClearBit( lastVertex.id - 1 );
                if ( z < 0 ) {
                    break;
                }
                factors = new int[]{ z, lastVertex.id };
            } else {
                final LoptMultiJoin.Edge bestEdge = unusedEdges.get( edgeOrdinal );

                // For now, assume that the edge is between precisely two factors. 1-factor conditions have probably been pushed down, and 3-or-more-factor conditions are advanced. (TODO:)
                // Therefore, for now, the factors that are merged are exactly the factors on this edge.
                assert bestEdge.factors.cardinality() == 2;
                factors = bestEdge.factors.toArray();
            }

            // Determine which factor is to be on the LHS of the join.
            final int majorFactor;
            final int minorFactor;
            if ( vertexes.get( factors[0] ).cost <= vertexes.get( factors[1] ).cost ) {
                majorFactor = factors[0];
                minorFactor = factors[1];
            } else {
                majorFactor = factors[1];
                minorFactor = factors[0];
            }
            final Vertex majorVertex = vertexes.get( majorFactor );
            final Vertex minorVertex = vertexes.get( minorFactor );

            // Find the join conditions. All conditions whose factors are now all in the join can now be used.
            final int v = vertexes.size();
            final ImmutableBitSet newFactors =
                    majorVertex.factors
                            .rebuild()
                            .addAll( minorVertex.factors )
                            .set( v )
                            .build();

            final List<RexNode> conditions = new ArrayList<>();
            final Iterator<LoptMultiJoin.Edge> edgeIterator = unusedEdges.iterator();
            while ( edgeIterator.hasNext() ) {
                LoptMultiJoin.Edge edge = edgeIterator.next();
                if ( newFactors.contains( edge.factors ) ) {
                    conditions.add( edge.condition );
                    edgeIterator.remove();
                    usedEdges.add( edge );
                }
            }

            double cost = majorVertex.cost * minorVertex.cost * AlgMdUtil.guessSelectivity( RexUtil.composeConjunction( rexBuilder, conditions ) );
            final Vertex newVertex = new JoinVertex( v, majorFactor, minorFactor, newFactors, cost, ImmutableList.copyOf( conditions ) );
            vertexes.add( newVertex );

            // Re-compute selectivity of edges above the one just chosen. Suppose that we just chose the edge between "product" (10k rows) and "product_class" (10 rows).
            // Both of those vertices are now replaced by a new vertex "P-PC".
            // This vertex has fewer rows (1k rows) -- a fact that is critical to decisions made later. (Hence "greedy" algorithm not "simple".)
            // The adjacent edges are modified.
            final ImmutableBitSet merged = ImmutableBitSet.of( minorFactor, majorFactor );
            for ( int i = 0; i < unusedEdges.size(); i++ ) {
                final LoptMultiJoin.Edge edge = unusedEdges.get( i );
                if ( edge.factors.intersects( merged ) ) {
                    ImmutableBitSet newEdgeFactors =
                            edge.factors
                                    .rebuild()
                                    .removeAll( newFactors )
                                    .set( v )
                                    .build();
                    assert newEdgeFactors.cardinality() == 2;
                    final LoptMultiJoin.Edge newEdge = new LoptMultiJoin.Edge( edge.condition, newEdgeFactors, edge.columns );
                    unusedEdges.set( i, newEdge );
                }
            }
        }

        // We have a winner!
        List<Pair<AlgNode, TargetMapping>> algNodes = new ArrayList<>();
        for ( Vertex vertex : vertexes ) {
            if ( vertex instanceof LeafVertex ) {
                LeafVertex leafVertex = (LeafVertex) vertex;
                final Mappings.TargetMapping mapping =
                        Mappings.offsetSource(
                                Mappings.createIdentity( leafVertex.alg.getTupleType().getFieldCount() ),
                                leafVertex.fieldOffset,
                                multiJoin.getNumTotalFields() );
                algNodes.add( Pair.of( leafVertex.alg, mapping ) );
            } else {
                JoinVertex joinVertex = (JoinVertex) vertex;
                final Pair<AlgNode, Mappings.TargetMapping> leftPair = algNodes.get( joinVertex.leftFactor );
                AlgNode left = leftPair.left;
                final Mappings.TargetMapping leftMapping = leftPair.right;
                final Pair<AlgNode, Mappings.TargetMapping> rightPair = algNodes.get( joinVertex.rightFactor );
                AlgNode right = rightPair.left;
                final Mappings.TargetMapping rightMapping = rightPair.right;
                final Mappings.TargetMapping mapping = Mappings.merge( leftMapping, Mappings.offsetTarget( rightMapping, left.getTupleType().getFieldCount() ) );
                if ( pw != null ) {
                    pw.println( "left: " + leftMapping );
                    pw.println( "right: " + rightMapping );
                    pw.println( "combined: " + mapping );
                    pw.println();
                }
                final RexVisitor<RexNode> shuttle = new RexPermuteInputsShuttle( mapping, left, right );
                final RexNode condition = RexUtil.composeConjunction( rexBuilder, joinVertex.conditions );

                final AlgNode join = algBuilder.push( left )
                        .push( right )
                        .join( JoinAlgType.INNER, condition.accept( shuttle ) )
                        .build();
                algNodes.add( Pair.of( join, mapping ) );
            }
            if ( pw != null ) {
                pw.println( Util.last( algNodes ) );
            }
        }

        final Pair<AlgNode, Mappings.TargetMapping> top = Util.last( algNodes );
        algBuilder.push( top.left ).project( algBuilder.fields( top.right ) );
        call.transformTo( algBuilder.build() );
    }


    private void trace( List<Vertex> vertexes, List<LoptMultiJoin.Edge> unusedEdges, List<LoptMultiJoin.Edge> usedEdges, int edgeOrdinal, PrintWriter pw ) {
        pw.println( "bestEdge: " + edgeOrdinal );
        pw.println( "vertexes:" );
        for ( Vertex vertex : vertexes ) {
            pw.println( vertex );
        }
        pw.println( "unused edges:" );
        for ( LoptMultiJoin.Edge edge : unusedEdges ) {
            pw.println( edge );
        }
        pw.println( "edges:" );
        for ( LoptMultiJoin.Edge edge : usedEdges ) {
            pw.println( edge );
        }
        pw.println();
        pw.flush();
    }


    int chooseBestEdge( List<LoptMultiJoin.Edge> edges, Comparator<LoptMultiJoin.Edge> comparator ) {
        return minPos( edges, comparator );
    }


    /**
     * Returns the index within a list at which compares least according to a comparator.
     *
     * In the case of a tie, returns the earliest such element.
     *
     * If the list is empty, returns -1.
     */
    static <E> int minPos( List<E> list, Comparator<E> fn ) {
        if ( list.isEmpty() ) {
            return -1;
        }
        E eBest = list.get( 0 );
        int iBest = 0;
        for ( int i = 1; i < list.size(); i++ ) {
            E e = list.get( i );
            if ( fn.compare( e, eBest ) < 0 ) {
                eBest = e;
                iBest = i;
            }
        }
        return iBest;
    }


    /**
     * Participant in a join (relation or join).
     */
    abstract static class Vertex {

        final int id;

        protected final ImmutableBitSet factors;
        final double cost;


        Vertex( int id, ImmutableBitSet factors, double cost ) {
            this.id = id;
            this.factors = factors;
            this.cost = cost;
        }

    }


    /**
     * Relation participating in a join.
     */
    static class LeafVertex extends Vertex {

        private final AlgNode alg;
        final int fieldOffset;


        LeafVertex( int id, AlgNode alg, double cost, int fieldOffset ) {
            super( id, ImmutableBitSet.of( id ), cost );
            this.alg = alg;
            this.fieldOffset = fieldOffset;
        }


        @Override
        public String toString() {
            return "LeafVertex(id: " + id
                    + ", cost: " + Util.human( cost )
                    + ", factors: " + factors
                    + ", fieldOffset: " + fieldOffset
                    + ")";
        }

    }


    /**
     * Participant in a join which is itself a join.
     */
    static class JoinVertex extends Vertex {

        private final int leftFactor;
        private final int rightFactor;
        /**
         * Zero or more join conditions. All are in terms of the original input columns (not in terms of the outputs of left and right input factors).
         */
        final ImmutableList<RexNode> conditions;


        JoinVertex( int id, int leftFactor, int rightFactor, ImmutableBitSet factors, double cost, ImmutableList<RexNode> conditions ) {
            super( id, factors, cost );
            this.leftFactor = leftFactor;
            this.rightFactor = rightFactor;
            this.conditions = Objects.requireNonNull( conditions );
        }


        @Override
        public String toString() {
            return "JoinVertex(id: " + id
                    + ", cost: " + Util.human( cost )
                    + ", factors: " + factors
                    + ", leftFactor: " + leftFactor
                    + ", rightFactor: " + rightFactor
                    + ")";
        }

    }

}


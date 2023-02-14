/*
 * Copyright 2019-2023 The Polypheny Project
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
 */

package org.polypheny.db.adaptimizer.polyfierconnect.pseudo.graphs;

import lombok.Getter;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.jgrapht.traverse.RandomWalkVertexIterator;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.ConstructUtil;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

@Getter
public class DecisionGraph {
    private final Decision<?> root;
    private final Graph<Decision<?>, DefaultEdge> decisionGraph;

    public DecisionGraph() {
        this.decisionGraph = new SimpleDirectedWeightedGraph<>( DefaultEdge.class );
        root = Decision.root();
        this.decisionGraph.addVertex( root );
    }

    /**
     * Adds a Decision to the graph.
     */
    public  <T> Decision<T> addDecision( T val, double weight ) {
        return addDecision( getRoot(), val, weight );
    }

    /**
     * Adds a Layer of decisions and weights to the graph.
     */
    public <A, B> List<Decision<B>> addDecisionLayer( Decision<A> parent, List<B> decisions, List<Double> weights ) {
        return ConstructUtil.zip(decisions, weights).stream().map( pair -> addDecision(parent, pair.left, pair.right) ).collect(Collectors.toList());
    }

    /**
     * Adds a Decision to the graph.
     */
    public <A, B> Decision<B> addDecision( Decision<A> parent, B val, double weight ) {
        Decision<B> decision = new Decision<>( val );
        decisionGraph.addVertex( decision );
        DefaultEdge defaultEdge = decisionGraph.addEdge( parent, decision );
        decisionGraph.setEdgeWeight( defaultEdge, weight );
        return decision;
    }

    public <A, B> List<Decision<B>> addDecisionCross( List<Decision<A>> parents, List<B> val, List<Double> weights ) {
        return ConstructUtil.cross( parents, ConstructUtil.zip( val, weights )).stream().map(pairing ->
            addDecision( pairing.left, pairing.right.left, pairing.right.right )
        ).collect(Collectors.toList());
    }


    /**
     * Returns a RandomWalkVertexIterator that will walk through and return configurations, whose order depends on
     * how the graph was constructed.
     * @param seed          Seed for the Traversal.
     * @param weighted      Whether the Traversal should consider weights.
     * @return              Iterator.
     */
    public RandomWalkVertexIterator<Decision<?>, DefaultEdge> randomWalk( long seed, boolean weighted ) {
        return new RandomWalkVertexIterator<>( decisionGraph, getRoot(), Integer.MAX_VALUE, weighted, new Random(seed) );
    }



}

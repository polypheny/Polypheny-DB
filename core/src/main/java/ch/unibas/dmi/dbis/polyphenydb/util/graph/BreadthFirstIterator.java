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

package ch.unibas.dmi.dbis.polyphenydb.util.graph;


import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


/**
 * Iterates over the vertices in a directed graph in breadth-first order.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
 */
public class BreadthFirstIterator<V, E extends DefaultEdge> implements Iterator<V> {

    private final DirectedGraph<V, E> graph;
    private final Deque<V> deque = new ArrayDeque<>();
    private final Set<V> set = new HashSet<>();


    public BreadthFirstIterator( DirectedGraph<V, E> graph, V root ) {
        this.graph = graph;
        this.deque.add( root );
    }


    public static <V, E extends DefaultEdge> Iterable<V> of( final DirectedGraph<V, E> graph, final V root ) {
        return () -> new BreadthFirstIterator<>( graph, root );
    }


    /**
     * Populates a set with the nodes reachable from a given node.
     */
    public static <V, E extends DefaultEdge> void reachable( Set<V> set, final DirectedGraph<V, E> graph, final V root ) {
        final Deque<V> deque = new ArrayDeque<>();
        deque.add( root );
        set.add( root );
        while ( !deque.isEmpty() ) {
            V v = deque.removeFirst();
            for ( E e : graph.getOutwardEdges( v ) ) {
                @SuppressWarnings("unchecked") V target = (V) e.target;
                if ( set.add( target ) ) {
                    deque.addLast( target );
                }
            }
        }
    }


    @Override
    public boolean hasNext() {
        return !deque.isEmpty();
    }


    @Override
    public V next() {
        V v = deque.removeFirst();
        for ( E e : graph.getOutwardEdges( v ) ) {
            @SuppressWarnings("unchecked") V target = (V) e.target;
            if ( set.add( target ) ) {
                deque.addLast( target );
            }
        }
        return v;
    }


    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}


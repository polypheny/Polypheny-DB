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


import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.util.List;


/**
 * Directed graph where edges have attributes and allows multiple edges between any two vertices provided that their attributes are different.
 *
 * @param <V> Vertex type
 * @param <E> Edge type
 */
public class AttributedDirectedGraph<V, E extends DefaultEdge> extends DefaultDirectedGraph<V, E> {

    /**
     * Creates an attributed graph.
     */
    public AttributedDirectedGraph( AttributedEdgeFactory<V, E> edgeFactory ) {
        super( edgeFactory );
    }


    public static <V, E extends DefaultEdge> AttributedDirectedGraph<V, E> create( AttributedEdgeFactory<V, E> edgeFactory ) {
        return new AttributedDirectedGraph<>( edgeFactory );
    }


    /**
     * Returns the first edge between one vertex to another.
     */
    @Override
    public E getEdge( V source, V target ) {
        final VertexInfo<V, E> info = vertexMap.get( source );
        for ( E outEdge : info.outEdges ) {
            if ( outEdge.target.equals( target ) ) {
                return outEdge;
            }
        }
        return null;
    }


    /**
     * @deprecated Use {@link #addEdge(Object, Object, Object...)}.
     */
    @Override
    @Deprecated
    public E addEdge( V vertex, V targetVertex ) {
        return super.addEdge( vertex, targetVertex );
    }


    public E addEdge( V vertex, V targetVertex, Object... attributes ) {
        final VertexInfo<V, E> info = vertexMap.get( vertex );
        if ( info == null ) {
            throw new IllegalArgumentException( "no vertex " + vertex );
        }
        final VertexInfo<V, E> info2 = vertexMap.get( targetVertex );
        if ( info2 == null ) {
            throw new IllegalArgumentException( "no vertex " + targetVertex );
        }
        @SuppressWarnings("unchecked") final AttributedEdgeFactory<V, E> f = (AttributedEdgeFactory) this.edgeFactory;
        final E edge = f.createEdge( vertex, targetVertex, attributes );
        if ( edges.add( edge ) ) {
            info.outEdges.add( edge );
            return edge;
        } else {
            return null;
        }
    }


    /**
     * Returns all edges between one vertex to another.
     */
    public Iterable<E> getEdges( V source, final V target ) {
        final VertexInfo<V, E> info = vertexMap.get( source );
        return Util.filter( info.outEdges, outEdge -> outEdge.target.equals( target ) );
    }


    /**
     * Removes all edges from a given vertex to another.
     * Returns whether any were removed.
     */
    @Override
    public boolean removeEdge( V source, V target ) {
        final VertexInfo<V, E> info = vertexMap.get( source );
        List<E> outEdges = info.outEdges;
        int removeCount = 0;
        for ( int i = 0, size = outEdges.size(); i < size; i++ ) {
            E edge = outEdges.get( i );
            if ( edge.target.equals( target ) ) {
                outEdges.remove( i );
                edges.remove( edge );
                ++removeCount;
            }
        }
        return removeCount > 0;
    }


    /**
     * Factory for edges that have attributes.
     *
     * @param <V> Vertex type
     * @param <E> Edge type
     */
    public interface AttributedEdgeFactory<V, E> extends EdgeFactory<V, E> {

        E createEdge( V v0, V v1, Object... attributes );
    }
}


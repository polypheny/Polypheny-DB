/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.workflow.engine.execution.pipe;

import java.util.Iterator;
import java.util.List;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;

public class LpgInputPipe {

    private final InputPipe pipe;
    private final Iterator<List<PolyValue>> iterator;
    private PolyNode nextNode = null;
    private PolyEdge firstEdge = null;

    private boolean hasNodesStarted = false;
    private boolean hasNodesFinished = false;
    private boolean hasEdgesStarted = false;
    private PolyEdge nextEdge = null;


    public LpgInputPipe( InputPipe pipe ) {
        if ( ActivityUtils.getDataModel( pipe.getType() ) != DataModel.GRAPH ) {
            throw new IllegalArgumentException( "DataModel " + pipe.getType() + " is not supported by LpgInputPipe" );
        }
        this.pipe = pipe;
        this.iterator = pipe.iterator();
    }


    /**
     * For performance reasons, the returned iterable must be used carefully.
     * It is recommended to use a single enhanced for loop for iterating over ALL nodes.
     * Please note the following restrictions:
     * <ul>
     *     <li>Only a single iterable can ever be returned.</li>
     *     <li>Only after iterating over all nodes is it possible to iterate over the edges.</li>
     * </ul>
     */
    public Iterable<PolyNode> getNodeIterable() {
        if ( hasNodesStarted ) {
            throw new IllegalStateException( "Cannot iterate more than once over the nodes of this pipe." );
        }
        hasNodesStarted = true;
        return () -> new Iterator<>() {

            @Override
            public boolean hasNext() {
                if ( iterator.hasNext() ) {
                    PolyValue value = iterator.next().get( 0 );
                    if ( value instanceof PolyNode node ) {
                        nextNode = node;
                        return true;
                    } else if ( value instanceof PolyEdge edge ) {
                        firstEdge = edge;
                        hasNodesFinished = true;
                        return false;
                    }
                }
                hasNodesFinished = true;
                return false;
            }


            @Override
            public PolyNode next() {
                return nextNode;
            }
        };
    }


    /**
     * For performance reasons, the returned iterable must be used carefully.
     * It is recommended to use a single enhanced for loop for iterating over the edges.
     * Please note the following restrictions:
     * <ul>
     *     <li>You must first iterate over all nodes.</li>
     *     <li>Only a single iterable can ever be returned.</li>
     * </ul>
     */
    public Iterable<PolyEdge> getEdgeIterable() {
        if ( hasEdgesStarted ) {
            throw new IllegalStateException( "Cannot iterate more than once over the edges of this pipe." );
        }
        if ( !hasNodesFinished ) {
            throw new IllegalStateException( "Cannot iterate over edges if the node iteration has not yet finished" );
        }

        hasEdgesStarted = true;

        return () -> new Iterator<>() {

            @Override
            public boolean hasNext() {
                if ( nextEdge == null ) {
                    // first edge
                    if ( firstEdge == null ) {
                        return false;
                    }
                    nextEdge = firstEdge;
                    return true;
                } else if ( iterator.hasNext() ) {
                    nextEdge = (PolyEdge) iterator.next().get( 0 );
                    return true;
                }
                return false;
            }


            @Override
            public PolyEdge next() {
                return nextEdge;
            }
        };
    }


    public void finishIteration() {
        pipe.finishIteration();
    }

}

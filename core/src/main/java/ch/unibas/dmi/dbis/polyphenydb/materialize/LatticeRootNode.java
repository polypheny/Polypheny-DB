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

package ch.unibas.dmi.dbis.polyphenydb.materialize;


import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;


/**
 * Root node in a {@link Lattice}. It has no parent.
 */
public class LatticeRootNode extends LatticeNode {

    /**
     * Descendants, in prefix order. This root node is at position 0.
     */
    public final ImmutableList<LatticeNode> descendants;
    final ImmutableList<Path> paths;


    LatticeRootNode( LatticeSpace space, MutableNode mutableNode ) {
        super( space, null, mutableNode );

        final ImmutableList.Builder<LatticeNode> b = ImmutableList.builder();
        flattenTo( b );
        this.descendants = b.build();
        this.paths = createPaths( space );
    }


    private ImmutableList<Path> createPaths( LatticeSpace space ) {
        final List<Step> steps = new ArrayList<>();
        final List<Path> paths = new ArrayList<>();
        createPathsRecurse( space, steps, paths );
        assert steps.isEmpty();
        return ImmutableList.copyOf( paths );
    }


    void use( List<LatticeNode> usedNodes ) {
        if ( !usedNodes.contains( this ) ) {
            usedNodes.add( this );
        }
    }


    /**
     * Validates that nodes form a tree; each node except the first references a predecessor.
     */
    boolean isValid( Litmus litmus ) {
        for ( int i = 0; i < descendants.size(); i++ ) {
            LatticeNode node = descendants.get( i );
            if ( i == 0 ) {
                if ( node != this ) {
                    return litmus.fail( "node 0 should be root" );
                }
            } else {
                if ( !(node instanceof LatticeChildNode) ) {
                    return litmus.fail( "node after 0 should be child" );
                }
                final LatticeChildNode child = (LatticeChildNode) node;
                if ( !descendants.subList( 0, i ).contains( child.parent ) ) {
                    return litmus.fail( "parent not in preceding list" );
                }
            }
        }
        return litmus.succeed();
    }


    /**
     * Whether this node's graph is a super-set of (or equal to) another node's graph.
     */
    public boolean contains( LatticeRootNode node ) {
        return paths.containsAll( node.paths );
    }

}


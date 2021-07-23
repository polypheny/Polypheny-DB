/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.cql.utils;

import org.polypheny.cql.contextset.exceptions.UnexpectedTypeException;

/*
 * Tree is a tree node with M type nodes being internal nodes and N type
 * node being the external nodes. At any given time only one of these have a valid value.
 * However, getting the wrong value from the tree node would result in a UnexpectedTypeException.
 */
public class Tree<M, N> {

    private final M internalNode;
    private final N externalNode;

    public final Tree<M, N> left;
    public final Tree<M, N> right;

    private final boolean leaf;


    public Tree( Tree<M, N> left, M internalNode, Tree<M, N> right ) {
        this.internalNode = internalNode;
        this.externalNode = null;
        this.left = left;
        this.right = right;
        this.leaf = false;
    }


    public Tree( N externalNode ) {
        this.internalNode = null;
        this.externalNode = externalNode;
        this.left = null;
        this.right = null;
        this.leaf = true;
    }


    public boolean isLeaf() {
        return leaf;
    }


    public M getInternalNode() throws UnexpectedTypeException {
        if ( leaf ) {
            throw new UnexpectedTypeException( "Invalid call to fetch internal node value from a external node." );
        }
        return internalNode;
    }


    public N getExternalNode() throws UnexpectedTypeException {
        if ( !leaf ) {
            throw new UnexpectedTypeException( "Invalid call to fetch external node value from a internal node." );
        }
        return externalNode;
    }


    public void TraverseInPlace( Action<M, N> action ) {
        TraverseInPlaceHelper( this, action );
    }


    private void TraverseInPlaceHelper( Tree<M, N> root, Action<M, N> action ) {
        if ( root.isLeaf() ) {
            action.PerformAction( root );
        } else {
            TraverseInPlaceHelper( root.left, action );
            action.PerformAction( root );
            TraverseInPlaceHelper( root.right, action );
        }
    }

}

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

import java.util.HashMap;
import java.util.Objects;
import org.polypheny.cql.exception.UnexpectedTypeException;

/*
 * Tree is a tree node with M type nodes being internal nodes and N type
 * node being the external nodes. At any given time only one of these have a valid value.
 * However, getting the wrong value from the tree node would result in a UnexpectedTypeException.
 */
public class Tree<M, N> {

    public final Tree<M, N> left;
    public final Tree<M, N> right;
    private final M internalNode;
    private final N externalNode;
    private final boolean leaf;


    public Tree( Tree<M, N> left, M internalNode, Tree<M, N> right ) {
        this.internalNode = internalNode;
        this.externalNode = null;
        this.left = Objects.requireNonNull( left );
        this.right = Objects.requireNonNull( right );
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


    public void traverse( TraversalType traversalType, Action<M, N> action ) {
        traverseHelper( this, traversalType, action );
    }


    private void traverseHelper( Tree<M, N> root, TraversalType traversalType, Action<M, N> action ) {
        HashMap<String, Object> frame = new HashMap<>();
        boolean proceed;
        if ( root.isLeaf() ) {
            action.performAction( root, NodeType.DESTINATION_NODE, Direction.UP_UP, frame );
        } else {
            // To suppress warnings. root.left and root.right will never be null
            // for an internal node.
            Objects.requireNonNull( root.left );
            Objects.requireNonNull( root.right );

            if ( traversalType == TraversalType.PREORDER ) {
                proceed = action.performAction( root, NodeType.DESTINATION_NODE, Direction.UP_DOWN, frame );
                if ( !proceed ) {
                    return;
                }
                traverseHelper( root.left, traversalType, action );
                proceed = action.performAction( root, NodeType.ROUTE_NODE, Direction.DOWN_DOWN, frame );
                if ( !proceed ) {
                    return;
                }
                traverseHelper( root.right, traversalType, action );
                action.performAction( root, NodeType.ROUTE_NODE, Direction.DOWN_UP, frame );
            } else if ( traversalType == TraversalType.INORDER ) {
                proceed = action.performAction( root, NodeType.ROUTE_NODE, Direction.UP_DOWN, frame );
                if ( !proceed ) {
                    return;
                }
                traverseHelper( root.left, traversalType, action );
                proceed = action.performAction( root, NodeType.DESTINATION_NODE, Direction.DOWN_DOWN, frame );
                if ( !proceed ) {
                    return;
                }
                traverseHelper( root.right, traversalType, action );
                action.performAction( root, NodeType.ROUTE_NODE, Direction.DOWN_UP, frame );
            } else if ( traversalType == TraversalType.POSTORDER ) {
                proceed = action.performAction( root, NodeType.ROUTE_NODE, Direction.UP_DOWN, frame );
                if ( !proceed ) {
                    return;
                }
                traverseHelper( root.left, traversalType, action );
                proceed = action.performAction( root, NodeType.ROUTE_NODE, Direction.DOWN_DOWN, frame );
                if ( !proceed ) {
                    return;
                }
                traverseHelper( root.right, traversalType, action );
                action.performAction( root, NodeType.DESTINATION_NODE, Direction.DOWN_UP, frame );
            } else if ( traversalType == TraversalType.REV_PREORDER ) {
                proceed = action.performAction( root, NodeType.DESTINATION_NODE, Direction.UP_DOWN, frame );
                if ( !proceed ) {
                    return;
                }
                traverseHelper( root.right, traversalType, action );
                proceed = action.performAction( root, NodeType.ROUTE_NODE, Direction.DOWN_DOWN, frame );
                if ( !proceed ) {
                    return;
                }
                traverseHelper( root.left, traversalType, action );
                action.performAction( root, NodeType.ROUTE_NODE, Direction.DOWN_UP, frame );
            } else if ( traversalType == TraversalType.REV_INORDER ) {
                proceed = action.performAction( root, NodeType.ROUTE_NODE, Direction.UP_DOWN, frame );
                if ( !proceed ) {
                    return;
                }
                traverseHelper( root.right, traversalType, action );
                proceed = action.performAction( root, NodeType.DESTINATION_NODE, Direction.DOWN_DOWN, frame );
                if ( !proceed ) {
                    return;
                }
                traverseHelper( root.left, traversalType, action );
                action.performAction( root, NodeType.ROUTE_NODE, Direction.DOWN_UP, frame );
            } else {
                proceed = action.performAction( root, NodeType.ROUTE_NODE, Direction.UP_DOWN, frame );
                if ( !proceed ) {
                    return;
                }
                traverseHelper( root.right, traversalType, action );
                proceed = action.performAction( root, NodeType.ROUTE_NODE, Direction.DOWN_DOWN, frame );
                if ( !proceed ) {
                    return;
                }
                traverseHelper( root.left, traversalType, action );
                action.performAction( root, NodeType.DESTINATION_NODE, Direction.DOWN_UP, frame );
            }
        }
    }


    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append( " (" );
        if ( isLeaf() ) {
            stringBuilder.append( externalNode );
        } else {
            stringBuilder.append( left.toString() );
            stringBuilder.append( internalNode );
            stringBuilder.append( right.toString() );
        }
        stringBuilder.append( ") " );

        return stringBuilder.toString();
    }


    public enum TraversalType {
        PREORDER,
        INORDER,
        POSTORDER,
        REV_PREORDER,
        REV_INORDER,
        REV_POSTORDER
    }


    public enum NodeType {
        ROUTE_NODE,
        DESTINATION_NODE
    }


    public enum Direction {
        UP_DOWN,
        UP_UP,
        DOWN_DOWN,
        DOWN_UP
    }

}

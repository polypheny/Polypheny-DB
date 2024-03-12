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
 */

package org.polypheny.db.cql.utils;

import java.util.HashMap;
import java.util.Objects;
import lombok.Getter;
import org.polypheny.db.cql.exception.UnexpectedTypeException;


/**
 * The basic block to build a tree that has different data types in leaf nodes
 * and internal nodes.
 *
 * @param <M> Type of the internal node.
 * @param <N> Type of the external node.
 */
public class Tree<M, N> {

    public final Tree<M, N> left;
    public final Tree<M, N> right;
    private final M internalNode;
    private final N externalNode;
    @Getter
    private final boolean leaf;


    /**
     * Constructor for an internal tree node.
     *
     * @param left left subtree.
     * @param internalNode node value.
     * @param right right subtree.
     */
    public Tree( Tree<M, N> left, M internalNode, Tree<M, N> right ) {
        this.internalNode = internalNode;
        this.externalNode = null;
        this.left = Objects.requireNonNull( left );
        this.right = Objects.requireNonNull( right );
        this.leaf = false;
    }


    /**
     * Constructor for an external (leaf) tree node.
     *
     * @param externalNode node value.
     */
    public Tree( N externalNode ) {
        this.internalNode = null;
        this.externalNode = externalNode;
        this.left = null;
        this.right = null;
        this.leaf = true;
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


    /**
     * A single method to perform all types of traversals.
     * See {@link TraversalType}.
     *
     * @param traversalType Traversal Type.
     * @param action Action to perform on the node.
     */
    public void traverse( TraversalType traversalType, Action<M, N> action ) {
        traverseHelper( this, traversalType, action );
    }


    /**
     * Helper for {@link #traverse(TraversalType, Action)}.
     *
     * @param root root / current node.
     * @param traversalType Traversal Type.
     * @param action Action to perform on the node.
     */
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
            stringBuilder.append( left );
            stringBuilder.append( internalNode );
            stringBuilder.append( right );
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


    /**
     * Denotes the type of the current node in the traversal.
     * <br>
     * ROUTE_NODE is a node on the path to some other node. Whereas,
     * DESTINATION_NODE is a node we want to visit.
     * <br>
     * For example,
     * <pre>
     *                         (1)
     *                        /   \
     *                      (2)   (3)
     * </pre>
     * In traversing the above tree, when the traversalType is
     * TraversalType.INORDER, we visit (1) first (in code) while
     * traversing to visit (2) (the actual first node in INORDER
     * traversal). This makes (1) a ROUTE_NODE and (2) a DESTINATION_NODE.
     * <br>
     * This distinction allows us to perform more complex actions not only
     * at the DESTINATION_NODE, but also at the ROUTE_NODE.
     */
    public enum NodeType {
        ROUTE_NODE,
        DESTINATION_NODE
    }


    /**
     * The direction the traversal in moving in.
     *
     * The word before underscore (_) is the direction from which
     * we reached the node and the word after underscore is the
     * direction we will be traversing to.
     * <br>
     * For example,
     * <pre>
     *                          (1)
     *                         /   \
     *                       (2)   (3)
     *                      /  \  /  \
     *                    (4) (5)(6) (7)
     * </pre>
     *
     * In traversing the above tree, when the traversalType is
     * TraversalType.INORDER, the actual order of traversal is
     * (1)-(2)-(4)*-(2)*-(5)*-(2)-(1)*-(3)-(6)*-(3)*-(7)*-(3)-(1),
     * where the asterisk (*) marks the DESTINATION_NODE.
     * <br>
     * When on node (2), on the path to (4)*, the direction is
     * UP_DOWN, since we came to node(2) from UP and, since it
     * has a left child, we move DOWN.
     * <br>
     * When on node (4)*, the direction is UP_UP, since we came to
     * node (4)* from UP and, since it is a leaf node, we move UP
     * after (4)*.
     * <br>
     * When on node (2)*, the direction is DOWN_DOWN, since we came
     * to node(2)* from DOWN (from node (4)*) and, since it has a
     * right child, we move DOWN.
     * <br>
     * When on node (2), on the path to (1)*, the direction is DOWN_UP,
     * since we came to node (2) from DOWN (from node(5)*) and, since
     * it is not the root node, we move UP.
     */
    public enum Direction {
        UP_DOWN,
        UP_UP,
        DOWN_DOWN,
        DOWN_UP
    }

}

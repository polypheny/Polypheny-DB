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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cql.exception.UnexpectedTypeException;
import org.polypheny.db.cql.utils.Tree.NodeType;
import org.polypheny.db.cql.utils.Tree.TraversalType;


public class TreeTest {

    private final Tree<Integer, Integer> tree;
    private final int[] actualTraversal = new int[]{
            1, 2, 4, 2, 5, 2, 1, 3, 6, 3, 7, 3, 1
    };
    private final int[] revActualTraversal = new int[]{
            1, 3, 7, 3, 6, 3, 1, 2, 5, 2, 4, 2, 1
    };
    private final int[] preorderTraversal = new int[]{
            1, 2, 4, 5, 3, 6, 7
    };
    private final int[] revPreorderTraversal = new int[]{
            1, 3, 7, 6, 2, 5, 4
    };
    private final int[] inorderTraversal = new int[]{
            4, 2, 5, 1, 6, 3, 7
    };
    private final int[] revInorderTraversal = new int[]{
            7, 3, 6, 1, 5, 2, 4
    };
    private final int[] postorderTraversal = new int[]{
            4, 5, 2, 6, 7, 3, 1
    };
    private final int[] revPostorderTraversal = new int[]{
            7, 6, 3, 5, 4, 2, 1
    };


    public TreeTest() {
        Tree<Integer, Integer> four = new Tree<>( 4 );
        Tree<Integer, Integer> five = new Tree<>( 5 );
        Tree<Integer, Integer> six = new Tree<>( 6 );
        Tree<Integer, Integer> seven = new Tree<>( 7 );
        Tree<Integer, Integer> two = new Tree<>( four, 2, five );
        Tree<Integer, Integer> three = new Tree<>( six, 3, seven );
        tree = new Tree<>( two, 1, three );
    }


    private static int getValue( Tree<Integer, Integer> treeNode ) {
        if ( treeNode.isLeaf() ) {
            return treeNode.getExternalNode();
        } else {
            return treeNode.getInternalNode();
        }
    }


    @Test
    public void testGetInternalNode() {
        Integer internalNode = tree.getInternalNode();
        assertEquals( 1, internalNode.intValue() );
    }


    @Test
    public void testGetInternalNodeThrowsUnexpectedTypeException() {
        assertThrows( UnexpectedTypeException.class, () -> tree.left.left.getInternalNode() );
    }


    @Test
    public void testGetExternalNode() {
        Integer externalNode = tree.left.left.getExternalNode();
        assertEquals( 4, externalNode.intValue() );
    }


    @Test
    public void testGetExternalNodeThrowsUnexpectedTypeException() {
        assertThrows( UnexpectedTypeException.class, () -> tree.left.getExternalNode() );
    }


    @Test
    public void testPreorderTraversal() {
        AtomicInteger actualTraversalIndex = new AtomicInteger();
        AtomicInteger preorderTraversalIndex = new AtomicInteger();
        tree.traverse( TraversalType.PREORDER, ( treeNode, nodeType, direction, frame ) -> {
            int actual = getValue( treeNode );
            int expected;
            if ( nodeType == NodeType.DESTINATION_NODE ) {
                expected = preorderTraversal[preorderTraversalIndex.get()];
                assertEquals( expected, actual );
                preorderTraversalIndex.getAndIncrement();
            }
            expected = actualTraversal[actualTraversalIndex.get()];
            assertEquals( expected, actual );
            actualTraversalIndex.getAndIncrement();

            return true;
        } );
    }


    @Test
    public void testInorderTraversal() {
        AtomicInteger actualTraversalIndex = new AtomicInteger();
        AtomicInteger inorderTraversalIndex = new AtomicInteger();
        tree.traverse( TraversalType.INORDER, ( treeNode, nodeType, direction, frame ) -> {
            int actual = getValue( treeNode );
            int expected;
            if ( nodeType == NodeType.DESTINATION_NODE ) {
                expected = inorderTraversal[inorderTraversalIndex.get()];
                assertEquals( expected, actual );
                inorderTraversalIndex.getAndIncrement();
            }
            expected = actualTraversal[actualTraversalIndex.get()];
            assertEquals( expected, actual );
            actualTraversalIndex.getAndIncrement();

            return true;
        } );
    }


    @Test
    public void testPostorderTraversal() {
        AtomicInteger actualTraversalIndex = new AtomicInteger();
        AtomicInteger postorderTraversalIndex = new AtomicInteger();
        tree.traverse( TraversalType.POSTORDER, ( treeNode, nodeType, direction, frame ) -> {
            int actual = getValue( treeNode );
            int expected;
            if ( nodeType == NodeType.DESTINATION_NODE ) {
                expected = postorderTraversal[postorderTraversalIndex.get()];
                assertEquals( expected, actual );
                postorderTraversalIndex.getAndIncrement();
            }
            expected = actualTraversal[actualTraversalIndex.get()];
            assertEquals( expected, actual );
            actualTraversalIndex.getAndIncrement();

            return true;
        } );
    }


    @Test
    public void testRevPreorderTraversal() {
        AtomicInteger revActualTraversalIndex = new AtomicInteger();
        AtomicInteger revPreorderTraversalIndex = new AtomicInteger();
        tree.traverse( TraversalType.REV_PREORDER, ( treeNode, nodeType, direction, frame ) -> {
            int actual = getValue( treeNode );
            int expected;
            if ( nodeType == NodeType.DESTINATION_NODE ) {
                expected = revPreorderTraversal[revPreorderTraversalIndex.get()];
                assertEquals( expected, actual );
                revPreorderTraversalIndex.getAndIncrement();
            }
            expected = revActualTraversal[revActualTraversalIndex.get()];
            assertEquals( expected, actual );
            revActualTraversalIndex.getAndIncrement();

            return true;
        } );
    }


    @Test
    public void testRevInorderTraversal() {
        AtomicInteger revActualTraversalIndex = new AtomicInteger();
        AtomicInteger revInorderTraversalIndex = new AtomicInteger();
        tree.traverse( TraversalType.REV_INORDER, ( treeNode, nodeType, direction, frame ) -> {
            int actual = getValue( treeNode );
            int expected;
            if ( nodeType == NodeType.DESTINATION_NODE ) {
                expected = revInorderTraversal[revInorderTraversalIndex.get()];
                assertEquals( expected, actual );
                revInorderTraversalIndex.getAndIncrement();
            }
            expected = revActualTraversal[revActualTraversalIndex.get()];
            assertEquals( expected, actual );
            revActualTraversalIndex.getAndIncrement();

            return true;
        } );
    }


    @Test
    public void testRevPostorderTraversal() {
        AtomicInteger revActualTraversalIndex = new AtomicInteger();
        AtomicInteger revPostorderTraversalIndex = new AtomicInteger();
        tree.traverse( TraversalType.REV_POSTORDER, ( treeNode, nodeType, direction, frame ) -> {
            int actual = getValue( treeNode );
            int expected;
            if ( nodeType == NodeType.DESTINATION_NODE ) {
                expected = revPostorderTraversal[revPostorderTraversalIndex.get()];
                assertEquals( expected, actual );
                revPostorderTraversalIndex.getAndIncrement();
            }
            expected = revActualTraversal[revActualTraversalIndex.get()];
            assertEquals( expected, actual );
            revActualTraversalIndex.getAndIncrement();

            return true;
        } );
    }

}

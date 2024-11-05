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

package org.polypheny.db.transaction;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class WaitForGraphTest {

    private WaitForGraph waitForGraph;
    private TransactionImpl transaction1;
    private TransactionImpl transaction2;
    private TransactionImpl transaction3;
    private TransactionImpl transaction4;


    @BeforeEach
    public void setup() {
        waitForGraph = new WaitForGraph();
        transaction1 = Mockito.mock( TransactionImpl.class );
        transaction2 = Mockito.mock( TransactionImpl.class );
        transaction3 = Mockito.mock( TransactionImpl.class );
        transaction4 = Mockito.mock( TransactionImpl.class );
    }


    @Test
    public void testAddSingleTransactionNoCycle() {
        Set<TransactionImpl> successors = new HashSet<>();
        successors.add( transaction2 );
        waitForGraph.add( transaction1, successors );

        assertFalse( waitForGraph.isMemberOfCycle( transaction1 ) );
    }


    @Test
    public void testAddAndDetectCycle() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        Set<TransactionImpl> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        waitForGraph.add( transaction1, successors1 );

        Set<TransactionImpl> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        waitForGraph.add( transaction2, successors2 );

        Set<TransactionImpl> successors3 = new HashSet<>();
        successors3.add( transaction1 );
        waitForGraph.add( transaction3, successors3 );

        assertTrue( waitForGraph.isMemberOfCycle( transaction1 ) );
        assertTrue( waitForGraph.isMemberOfCycle( transaction2 ) );
        assertFalse( waitForGraph.isMemberOfCycle( transaction3 ) );
    }


    @Test
    public void testAddOutsideOfCycleNotDetected() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        Set<TransactionImpl> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        waitForGraph.add( transaction1, successors1 );

        Set<TransactionImpl> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        waitForGraph.add( transaction2, successors2 );

        Set<TransactionImpl> successors3 = new HashSet<>();
        successors3.add( transaction1 );
        waitForGraph.add( transaction3, successors3 );

        // Add transaction not involved in cycle
        Set<TransactionImpl> successors4 = new HashSet<>();
        successors3.add( transaction1 );
        waitForGraph.add( transaction4, successors4 );

        assertFalse( waitForGraph.isMemberOfCycle( transaction4 ) );
    }


    @Test
    public void testAddTransactionsWithoutCycle() {
        // No cycle: T1 -> T2 -> T3
        Set<TransactionImpl> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        waitForGraph.add( transaction1, successors1 );

        Set<TransactionImpl> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        waitForGraph.add( transaction2, successors2 );

        assertFalse( waitForGraph.isMemberOfCycle( transaction1 ) );
        assertFalse( waitForGraph.isMemberOfCycle( transaction2 ) );
        assertFalse( waitForGraph.isMemberOfCycle( transaction3 ) );
    }


    @Test
    public void testRemoveTransactionBreaksCycle() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        Set<TransactionImpl> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        waitForGraph.add( transaction1, successors1 );

        Set<TransactionImpl> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        waitForGraph.add( transaction2, successors2 );

        Set<TransactionImpl> successors3 = new HashSet<>();
        successors3.add( transaction1 );
        waitForGraph.add( transaction3, successors3 );

        assertTrue( waitForGraph.isMemberOfCycle( transaction1 ) );

        waitForGraph.remove( transaction2 );

        assertFalse( waitForGraph.isMemberOfCycle( transaction1 ) );
        assertFalse( waitForGraph.isMemberOfCycle( transaction3 ) );
    }
}

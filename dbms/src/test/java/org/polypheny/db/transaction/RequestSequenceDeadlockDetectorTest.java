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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.polypheny.db.transaction.deadlocks.RequestSequenceDeadlockDetector;
import org.polypheny.db.transaction.locking.Lockable;

public class RequestSequenceDeadlockDetectorTest {

    private RequestSequenceDeadlockDetector deadlockDetector;
    private TransactionImpl transaction1;
    private TransactionImpl transaction2;
    private TransactionImpl transaction3;
    private TransactionImpl transaction4;
    private Lockable lockable;


    @BeforeEach
    public void setup() {
        deadlockDetector = new RequestSequenceDeadlockDetector();
        transaction1 = Mockito.mock( TransactionImpl.class );
        transaction2 = Mockito.mock( TransactionImpl.class );
        transaction3 = Mockito.mock( TransactionImpl.class );
        transaction4 = Mockito.mock( TransactionImpl.class );
        lockable = Mockito.mock( Lockable.class );
    }


    @Test
    public void testAddAndDetectSingleTransaction() {
        Set<Transaction> successors = new HashSet<>();
        successors.add( transaction2 );
        deadlockDetector.add( lockable, transaction1, successors );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );
    }


    @Test
    public void testAddAndAbortIfDeadlockCycle() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        Set<Transaction> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        deadlockDetector.add( lockable, transaction1, successors1 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        deadlockDetector.add( lockable, transaction2, successors2 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors3 = new HashSet<>();
        successors3.add( transaction1 );
        deadlockDetector.add( lockable, transaction3, successors3 );

        List<Transaction> conflictingTransactions = deadlockDetector.getConflictingTransactions();
        assertEquals( 3, conflictingTransactions.size() );
        assertTrue( conflictingTransactions.contains( transaction1 ) );
        assertTrue( conflictingTransactions.contains( transaction2 ) );
        assertTrue( conflictingTransactions.contains( transaction3 ) );
    }


    @Test
    public void testAddAndAbortIfDeadlockTransactionsWithoutCycle() {
        // No cycle: T1 -> T2 -> T3
        Set<Transaction> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        deadlockDetector.add( lockable, transaction1, successors1 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        deadlockDetector.add( lockable, transaction2, successors2 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );
    }


    @Test
    public void testRemoveTransactionBreaksCycle() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        Set<Transaction> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        deadlockDetector.add( lockable, transaction1, successors1 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        deadlockDetector.add( lockable, transaction2, successors2 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors3 = new HashSet<>();
        successors3.add( transaction1 );
        deadlockDetector.add( lockable, transaction3, successors3 );

        List<Transaction> conflictingTransactions = deadlockDetector.getConflictingTransactions();
        assertEquals( 3, conflictingTransactions.size() );
        assertTrue( conflictingTransactions.contains( transaction1 ) );
        assertTrue( conflictingTransactions.contains( transaction2 ) );
        assertTrue( conflictingTransactions.contains( transaction3 ) );

        deadlockDetector.remove( lockable, transaction2 );

        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );
    }


    @Test
    public void testUnrelatedTransactionNotDetected() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        Set<Transaction> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        deadlockDetector.add( lockable, transaction1, successors1 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        deadlockDetector.add( lockable, transaction2, successors2 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors3 = new HashSet<>();
        successors3.add( transaction1 );
        deadlockDetector.add( lockable, transaction3, successors3 );

        List<Transaction> conflictingTransactions = deadlockDetector.getConflictingTransactions();
        assertEquals( 3, conflictingTransactions.size() );
        assertTrue( conflictingTransactions.contains( transaction1 ) );
        assertTrue( conflictingTransactions.contains( transaction2 ) );
        assertTrue( conflictingTransactions.contains( transaction3 ) );

        Set<Transaction> successors4 = new HashSet<>();
        successors4.add( transaction1 );
        deadlockDetector.add( lockable, transaction4, successors4 );

        assertFalse( conflictingTransactions.contains( transaction4 ) );

        deadlockDetector.remove( lockable, transaction2 );

        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );
    }


    @Test
    public void testMultipleCyclesAndRemoval() {
        // Add first cycle: T1 -> T2 -> T1
        Set<Transaction> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        deadlockDetector.add( lockable, transaction1, successors1 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors2 = new HashSet<>();
        successors2.add( transaction1 );
        deadlockDetector.add( lockable, transaction2, successors2 );

        assertTrue( deadlockDetector.getConflictingTransactions().contains( transaction1 ) ||
                deadlockDetector.getConflictingTransactions().contains( transaction2 ) );

        // Add second cycle: T3 -> T4 -> T3
        Set<Transaction> successors3 = new HashSet<>();
        successors3.add( transaction4 );
        deadlockDetector.add( lockable, transaction3, successors3 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors4 = new HashSet<>();
        successors4.add( transaction3 );
        deadlockDetector.add( lockable, transaction4, successors4 );
        assertTrue( deadlockDetector.getConflictingTransactions().contains( transaction3 ) ||
                deadlockDetector.getConflictingTransactions().contains( transaction4 ) );

        // check for any transaction to be returned
        List<Transaction> conflictingTransactions = deadlockDetector.getConflictingTransactions();
        assertFalse( conflictingTransactions.isEmpty() );

        deadlockDetector.remove( lockable, transaction1 );
        deadlockDetector.remove( lockable, transaction2 );

        // check for second cycle
        conflictingTransactions = deadlockDetector.getConflictingTransactions();
        assertEquals( 2, conflictingTransactions.size() );
        assertTrue( conflictingTransactions.contains( transaction3 ) );
        assertTrue( conflictingTransactions.contains( transaction4 ) );
    }


}

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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.polypheny.db.transaction.deadlocks.RequestSequenceDeadlockDetector;
import org.polypheny.db.transaction.locking.Lockable;
import org.polypheny.db.transaction.locking.LockableImpl;

@Disabled
public class RequestSequenceDeadlockDetectorTest {

    private RequestSequenceDeadlockDetector deadlockDetector;
    private Transaction transaction1;
    private Transaction transaction2;
    private Transaction transaction3;
    private Transaction transaction4;
    private LockableImpl lockable1;
    private LockableImpl lockable2;
    private LockableImpl lockable3;

    Set<Transaction> emptySet;


    @BeforeEach
    public void setup() {
        deadlockDetector = new RequestSequenceDeadlockDetector();

        transaction1 = new MockTransaction( 1 );
        transaction2 = new MockTransaction( 2 );
        transaction3 = new MockTransaction( 3 );
        transaction4 = new MockTransaction( 4 );

        lockable1 = new LockableImpl( null );
        lockable2 = new LockableImpl( null );
        lockable3 = new LockableImpl( null );

        emptySet = new HashSet<>();
    }


    @Test
    public void testAddAndDetectSingleTransaction() {
        Set<Transaction> successors = new HashSet<>();

        deadlockDetector.add( lockable1, transaction1, successors );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );
    }


    @Test
    public void testAddAndAbortIfDeadlockCycle() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        deadlockDetector.add( lockable1, transaction1, emptySet );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        deadlockDetector.add( lockable2, transaction2, emptySet );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        deadlockDetector.add( lockable1, transaction3, emptySet );

        List<Transaction> conflictingTransactions = deadlockDetector.getConflictingTransactions();
        assertEquals( 1, conflictingTransactions.size() );
        assertTrue( conflictingTransactions.contains( transaction3 ) );
    }


    @Test
    public void testAddAndAbortIfDeadlockTransactionsWithoutCycle() {
        // No cycle: T1 -> T2 -> T3
        deadlockDetector.add( lockable1, transaction1, emptySet );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        deadlockDetector.add( lockable2, transaction2, emptySet );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );
    }


    @Test
    public void testRemoveTransactionBreaksCycle() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        deadlockDetector.add( lockable1, transaction1, emptySet );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        deadlockDetector.add( lockable2, transaction2, emptySet );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        deadlockDetector.add( lockable1, transaction3, emptySet );

        List<Transaction> conflictingTransactions = deadlockDetector.getConflictingTransactions();
        assertEquals( 1, conflictingTransactions.size() );
        assertTrue( conflictingTransactions.contains( transaction3 ) );

        deadlockDetector.remove( lockable2, transaction2 );

        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );
    }


    @Test
    public void testUnrelatedTransactionNotDetected() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        deadlockDetector.add( lockable1, transaction1, emptySet );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        deadlockDetector.add( lockable2, transaction2, emptySet );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        deadlockDetector.add( lockable1, transaction3, emptySet );

        List<Transaction> conflictingTransactions = deadlockDetector.getConflictingTransactions();
        assertEquals( 1, conflictingTransactions.size() );

        assertTrue( conflictingTransactions.contains( transaction3 ) );

        deadlockDetector.add( lockable3, transaction4, emptySet );

        assertFalse( conflictingTransactions.contains( transaction4 ) );

        deadlockDetector.remove( lockable2, transaction2 );

        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );
    }


    @Test
    public void testMultipleCyclesAndRemoval() {
        // Add first cycle: T1 -> T2 -> T1
        deadlockDetector.add( lockable1, transaction1, emptySet );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );


        deadlockDetector.add( lockable1, transaction2, emptySet );
        List<Transaction> criticalTransactions = deadlockDetector.getConflictingTransactions();
        assertTrue( criticalTransactions.contains( transaction1 ) || criticalTransactions.contains( transaction2 ) );

        // Add second cycle: T3 -> T4 -> T3
        deadlockDetector.add( lockable3, transaction3, emptySet );
        deadlockDetector.add( lockable3, transaction4, emptySet );

        // remove first cycle
        deadlockDetector.remove( lockable1, transaction1 );
        deadlockDetector.remove( lockable1, transaction2 );

        criticalTransactions = deadlockDetector.getConflictingTransactions();
        assertTrue( criticalTransactions.contains( transaction3 ) || criticalTransactions.contains( transaction4 ) );
    }
}

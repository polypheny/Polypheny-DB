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
import org.polypheny.db.transaction.deadlocks.GraphDeadlockDetector;

public class GraphDeadlockDetectorTest {

    private GraphDeadlockDetector deadlockDetector;
    private TransactionImpl transaction1;
    private TransactionImpl transaction2;
    private TransactionImpl transaction3;
    private TransactionImpl transaction4;


    @BeforeEach
    public void setup() {
        deadlockDetector = new GraphDeadlockDetector();
        transaction1 = Mockito.mock( TransactionImpl.class );
        transaction2 = Mockito.mock( TransactionImpl.class );
        transaction3 = Mockito.mock( TransactionImpl.class );
        transaction4 = Mockito.mock( TransactionImpl.class );
    }


    @Test
    public void testAddAndDetectSingleTransaction() {
        Set<Transaction> successors = new HashSet<>();
        successors.add( transaction2 );
        deadlockDetector.add( transaction1, successors );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );
    }


    @Test
    public void testAddAndAbortIfDeadlockCycle() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        Set<Transaction> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        deadlockDetector.add( transaction1, successors1 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        deadlockDetector.add( transaction2, successors2 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors3 = new HashSet<>();
        successors3.add( transaction1 );
        deadlockDetector.add( transaction3, successors3 );

        List<Transaction> conflictingTransactions = deadlockDetector.getConflictingTransactions();
        assertEquals( 1, conflictingTransactions.size() );
        assertTrue( conflictingTransactions.contains( transaction3 ) );
    }


    @Test
    public void testAddAndAbortIfDeadlockTransactionsWithoutCycle() {
        // No cycle: T1 -> T2 -> T3
        Set<Transaction> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        deadlockDetector.add( transaction1, successors1 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        deadlockDetector.add( transaction2, successors2 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );
    }


    @Test
    public void testRemoveTransactionBreaksCycle() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        Set<Transaction> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        deadlockDetector.add( transaction1, successors1 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        deadlockDetector.add( transaction2, successors2 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors3 = new HashSet<>();
        successors3.add( transaction1 );
        deadlockDetector.add( transaction3, successors3 );

        List<Transaction> conflictingTransactions = deadlockDetector.getConflictingTransactions();
        assertEquals( 1, conflictingTransactions.size() );
        assertTrue( conflictingTransactions.contains( transaction3 ) );

        deadlockDetector.remove( transaction2 );

        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );
    }

    @Test
    public void testUnrelatedTransactionNotDetected() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        Set<Transaction> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        deadlockDetector.add( transaction1, successors1 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        deadlockDetector.add( transaction2, successors2 );
        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );

        Set<Transaction> successors3 = new HashSet<>();
        successors3.add( transaction1 );
        deadlockDetector.add( transaction3, successors3 );

        List<Transaction> conflictingTransactions = deadlockDetector.getConflictingTransactions();
        assertEquals( 1, conflictingTransactions.size() );
        assertTrue( conflictingTransactions.contains( transaction3 ) );

        Set<Transaction> successors4 = new HashSet<>();
        successors4.add( transaction1 );
        deadlockDetector.add( transaction4, successors4 );

        assertFalse( conflictingTransactions.contains( transaction4 ) );

        deadlockDetector.remove( transaction2 );

        assertTrue( deadlockDetector.getConflictingTransactions().isEmpty() );
    }

}

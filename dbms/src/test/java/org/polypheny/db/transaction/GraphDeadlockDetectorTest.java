package org.polypheny.db.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.polypheny.db.transaction.deadlocks.GraphDeadlockDetector;
import org.polypheny.db.transaction.locking.Lockable;

public class GraphDeadlockDetectorTest {

    private GraphDeadlockDetector deadlockDetector;
    private Transaction transaction1;
    private Transaction transaction2;
    private Transaction transaction3;
    private Transaction transaction4;
    private Lockable lockable;


    @BeforeEach
    public void setup() {
        deadlockDetector = new GraphDeadlockDetector();

        transaction1 = new MockTransaction( 1 );
        transaction2 = new MockTransaction( 2 );
        transaction3 = new MockTransaction( 3 );
        transaction4 = new MockTransaction( 4 );
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

        Set<Transaction> successors4 = new HashSet<>();
        successors4.add( transaction3 );
        deadlockDetector.add( lockable, transaction4, successors4 );

        // remove first cycle
        deadlockDetector.remove( lockable, transaction1 );
        //deadlockDetector.remove( lockable, transaction2 );

        assertTrue( deadlockDetector.getConflictingTransactions().contains( transaction3 ) ||
                deadlockDetector.getConflictingTransactions().contains( transaction4 ) );
    }


}

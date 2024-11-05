package org.polypheny.db.transaction;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

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
    public void testAddAndAbortIfDeadlockSingleTransaction() {
        Set<Transaction> successors = new HashSet<>();
        successors.add( transaction2 );
        waitForGraph.addAndAbortIfDeadlock( transaction1, successors );

        verify( transaction1, never() ).abort();
        verify( transaction2, never() ).abort();
    }


    @Test
    public void testAddAndAbortIfDeadlockCycle() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        Set<Transaction> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        waitForGraph.addAndAbortIfDeadlock( transaction1, successors1 );

        Set<Transaction> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        waitForGraph.addAndAbortIfDeadlock( transaction2, successors2 );

        Set<Transaction> successors3 = new HashSet<>();
        successors3.add( transaction1 );
        waitForGraph.addAndAbortIfDeadlock( transaction3, successors3 );

        verify( transaction3, atLeastOnce() ).abort();
    }


    @Test
    public void testAddAndAbortIfDeadlockOutsideOfCycleNotDetected() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        Set<Transaction> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        waitForGraph.addAndAbortIfDeadlock( transaction1, successors1 );

        Set<Transaction> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        waitForGraph.addAndAbortIfDeadlock( transaction2, successors2 );

        Set<Transaction> successors3 = new HashSet<>();
        successors3.add( transaction1 );
        waitForGraph.addAndAbortIfDeadlock( transaction3, successors3 );

        Set<Transaction> successors4 = new HashSet<>();
        successors4.add( transaction1 );
        waitForGraph.addAndAbortIfDeadlock( transaction4, successors4 );

        verify( transaction4, never() ).abort();
    }


    @Test
    public void testAddAndAbortIfDeadlockTransactionsWithoutCycle() {
        // No cycle: T1 -> T2 -> T3
        Set<Transaction> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        waitForGraph.addAndAbortIfDeadlock( transaction1, successors1 );

        Set<Transaction> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        waitForGraph.addAndAbortIfDeadlock( transaction2, successors2 );

        verify( transaction1, never() ).abort();
        verify( transaction2, never() ).abort();
        verify( transaction3, never() ).abort();
    }


    @Test
    public void testRemoveTransactionBreaksCycle() {
        // Add a cycle: T1 -> T2 -> T3 -> T1
        Set<Transaction> successors1 = new HashSet<>();
        successors1.add( transaction2 );
        waitForGraph.addAndAbortIfDeadlock( transaction1, successors1 );

        Set<Transaction> successors2 = new HashSet<>();
        successors2.add( transaction3 );
        waitForGraph.addAndAbortIfDeadlock( transaction2, successors2 );

        Set<Transaction> successors3 = new HashSet<>();
        successors3.add( transaction1 );
        waitForGraph.addAndAbortIfDeadlock( transaction3, successors3 );

        verify( transaction3, atLeastOnce() ).abort();

        waitForGraph.remove( transaction2 );

        reset( transaction1 );
        reset( transaction2 );
        reset( transaction3 );

        waitForGraph.addAndAbortIfDeadlock( transaction3, successors1 );

        verify( transaction1, never() ).abort();
        verify( transaction2, never() ).abort();
        verify( transaction3, never() ).abort();
    }

}

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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.transaction.locking.LockableImpl;

public class LockableImplTest {

    private static TestHelper testHelper;

    private LockableImpl lockable;
    private Transaction transaction1;
    private Transaction transaction2;
    private Transaction transaction3;


    @BeforeAll
    public static void setUpClass() {
        testHelper = TestHelper.getInstance();
    }


    @BeforeEach
    public void setup() {
        transaction1 = new MockTransaction( 1 );
        transaction2 = new MockTransaction( 2 );
        transaction3 = new MockTransaction( 3 );
        lockable = new LockableImpl( null );
    }


    @Test
    public void isSharedByDefault() {
        assertEquals( LockType.SHARED, lockable.getLockType() );
        assertTrue( lockable.getCopyOfOwners().isEmpty() );
    }


    @Test
    public void isExclusiveAfterAcquired() {
        transaction1.acquireLockable( lockable, LockType.EXCLUSIVE );
        assertEquals( LockType.EXCLUSIVE, lockable.getLockType() );
        assertEquals( 1, lockable.getCopyOfOwners().size() );
    }


    @Test
    public void isSharedAfterAcquired() {
        transaction1.acquireLockable( lockable, LockType.SHARED );
        assertEquals( LockType.SHARED, lockable.getLockType() );
        assertEquals( 1, lockable.getCopyOfOwners().size() );
    }


    @Test
    public void simpleUpgrade() {
        transaction1.acquireLockable( lockable, LockType.SHARED );
        assertEquals( LockType.SHARED, lockable.getLockType() );
        transaction1.acquireLockable( lockable, LockType.EXCLUSIVE );
        assertEquals( LockType.EXCLUSIVE, lockable.getLockType() );
        assertEquals( 1, lockable.getCopyOfOwners().size() );
    }


    @Test
    public void simpleSharedAfterUpgrade() {
        transaction1.acquireLockable( lockable, LockType.SHARED );
        assertEquals( LockType.SHARED, lockable.getLockType() );
        transaction1.acquireLockable( lockable, LockType.EXCLUSIVE );
        assertEquals( LockType.EXCLUSIVE, lockable.getLockType() );
        assertEquals( 1, lockable.getCopyOfOwners().size() );
        transaction1.acquireLockable( lockable, LockType.SHARED );
        assertEquals( LockType.EXCLUSIVE, lockable.getLockType() );
        assertEquals( 1, lockable.getCopyOfOwners().size() );
    }


    @Test
    public void multipleSharedOwners() {
        transaction1.acquireLockable( lockable, LockType.SHARED );
        transaction2.acquireLockable( lockable, LockType.SHARED );
        assertEquals( LockType.SHARED, lockable.getLockType() );
        assertEquals( 2, lockable.getCopyOfOwners().size() );
    }


    @Test
    public void releaseSharedSingle() {
        transaction1.acquireLockable( lockable, LockType.SHARED );
        assertEquals( LockType.SHARED, lockable.getLockType() );
        assertEquals( 1, lockable.getCopyOfOwners().size() );
        transaction1.commit();
        assertEquals( LockType.SHARED, lockable.getLockType() );
        assertTrue( lockable.getCopyOfOwners().isEmpty() );
    }


    @Test
    public void releaseExclusiveSingle() {
        transaction1.acquireLockable( lockable, LockType.EXCLUSIVE );
        assertEquals( LockType.EXCLUSIVE, lockable.getLockType() );
        assertEquals( 1, lockable.getCopyOfOwners().size() );
        transaction1.commit();
        assertEquals( LockType.SHARED, lockable.getLockType() );
        assertTrue( lockable.getCopyOfOwners().isEmpty() );
    }


    @Test
    public void multipleTransactionsExclusive() throws ExecutionException, InterruptedException, TimeoutException {
        transaction1.acquireLockable( lockable, LockType.EXCLUSIVE );
        assertEquals( LockType.EXCLUSIVE, lockable.getLockType() );
        assertEquals( 1, lockable.getCopyOfOwners().size() );

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> future = executorService.submit( () -> {
            try {
                transaction2.acquireLockable( lockable, LockType.EXCLUSIVE );
                assertEquals( LockType.EXCLUSIVE, lockable.getLockType() );
                assertEquals( 1, lockable.getCopyOfOwners().size() );
                assertTrue( lockable.getCopyOfOwners().contains( transaction2 ) );
                transaction2.commit();
            } catch ( Exception e ) {
                fail( "Transaction 2 failed: " + e.getMessage() );
            }
        } );

        Thread.sleep( 2000 );

        transaction1.commit();

        future.get( 1, TimeUnit.MINUTES );

        assertEquals( LockType.SHARED, lockable.getLockType() );
        assertTrue( lockable.getCopyOfOwners().isEmpty() );

        executorService.shutdown();
    }


    @Test
    public void upgradeWhileShared() throws ExecutionException, InterruptedException, TimeoutException {
        transaction1.acquireLockable( lockable, LockType.SHARED );
        assertEquals( LockType.SHARED, lockable.getLockType() );
        assertEquals( 1, lockable.getCopyOfOwners().size() );

        transaction2.acquireLockable( lockable, LockType.SHARED );
        assertEquals( LockType.SHARED, lockable.getLockType() );
        assertEquals( 2, lockable.getCopyOfOwners().size() );

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> future = executorService.submit( () -> {
            try {
                transaction2.acquireLockable( lockable, LockType.EXCLUSIVE );
                assertEquals( LockType.EXCLUSIVE, lockable.getLockType() );
                assertEquals( 1, lockable.getCopyOfOwners().size() );
                assertTrue( lockable.getCopyOfOwners().contains( transaction2 ) );
                transaction2.commit();
            } catch ( Exception e ) {
                fail( "Transaction 2 failed: " + e.getMessage() );
            }
        } );

        Thread.sleep( 2000 );

        transaction1.commit();
        future.get( 1, TimeUnit.MINUTES );

        assertEquals( LockType.SHARED, lockable.getLockType() );
        assertTrue( lockable.getCopyOfOwners().isEmpty() );

        executorService.shutdown();
    }

}

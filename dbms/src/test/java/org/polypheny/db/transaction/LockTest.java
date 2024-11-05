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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.polypheny.db.transaction.Lock.LockType;

public class LockTest {

    private WaitForGraph waitForGraph;
    private TransactionImpl transaction1;
    private TransactionImpl transaction2;


    @BeforeEach
    public void setup() {
        waitForGraph = new WaitForGraph();
        transaction1 = Mockito.mock( TransactionImpl.class );
        transaction2 = Mockito.mock( TransactionImpl.class );
    }


    @Test
    void testSharedLockSameOwners() throws InterruptedException {
        Lock lock = new Lock();
        CountDownLatch latch = new CountDownLatch( 2 );
        CountDownLatch secondThreadStartLatch = new CountDownLatch( 1 );
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool( 2 );
        List<Boolean> results = new ArrayList<>();

        executor.execute( () -> {
            try {
                lock.acquire( transaction1, Lock.LockType.SHARED, waitForGraph );
                results.add( true );
            } catch ( InterruptedException e ) {
                results.add( false );
                Thread.currentThread().interrupt();
            } finally {
                secondThreadStartLatch.countDown();
                latch.countDown();
            }
        } );

        executor.execute( () -> {
            try {
                secondThreadStartLatch.await();
                lock.acquire( transaction1, Lock.LockType.SHARED, waitForGraph );
                results.add( true );
            } catch ( InterruptedException e ) {
                results.add( false );
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        } );

        latch.await();
        executor.shutdown();

        assertEquals( 2, results.size() );
        assertTrue( results.stream().allMatch( result -> result ) );
    }


    @Test
    void testSharedLockMultipleOwners() throws InterruptedException {
        Lock lock = new Lock();
        CountDownLatch latch = new CountDownLatch( 2 );
        CountDownLatch secondThreadStartLatch = new CountDownLatch( 1 );
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool( 2 );
        List<Boolean> results = new ArrayList<>();

        executor.execute( () -> {
            try {
                lock.acquire( transaction1, Lock.LockType.SHARED, waitForGraph );
                results.add( true );
            } catch ( InterruptedException e ) {
                results.add( false );
                Thread.currentThread().interrupt();
            } finally {
                secondThreadStartLatch.countDown();
                latch.countDown();
            }
        } );

        executor.execute( () -> {
            try {
                secondThreadStartLatch.await();
                lock.acquire( transaction2, Lock.LockType.SHARED, waitForGraph );
                results.add( true );
            } catch ( InterruptedException e ) {
                results.add( false );
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        } );

        latch.await();
        executor.shutdown();

        assertEquals( 2, results.size() );
        assertTrue( results.stream().allMatch( result -> result ) );
    }


    @Test
    void testExclusiveLockSingleAccess() throws InterruptedException {
        Lock lock = new Lock();
        CountDownLatch latch = new CountDownLatch( 2 );
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool( 2 );
        List<Boolean> results = new ArrayList<>();

        executor.execute( () -> {
            try {
                lock.acquire( transaction1, Lock.LockType.EXCLUSIVE, waitForGraph );
                results.add( true );
                Thread.sleep( 1000 ); // ensure contention
                lock.release( transaction1, waitForGraph );
            } catch ( InterruptedException e ) {
                results.add( false );
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        } );

        executor.execute( () -> {
            try {
                lock.acquire( transaction2, Lock.LockType.EXCLUSIVE, waitForGraph );
                results.add( true );
                lock.release( transaction2, waitForGraph );
            } catch ( InterruptedException e ) {
                results.add( false );
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        } );

        latch.await();
        executor.shutdown();

        assertEquals( 2, results.size() );
        assertTrue( results.stream().allMatch( result -> result ) );
    }


    @Test
    void testUpgradeSharedToExclusive() {
        Lock lock = new Lock();
        try {
            lock.acquire( transaction1, Lock.LockType.SHARED, waitForGraph );
            lock.upgradeToExclusive( transaction1, waitForGraph );
            assertSame( lock.getLockType(), LockType.EXCLUSIVE );
            lock.release( transaction1, waitForGraph );
        } catch ( InterruptedException e ) {
            fail();
        }
    }


    @Test
    void testSharedLockWaitOnExclusiveLock() throws InterruptedException {
        Lock lock = new Lock();
        CountDownLatch secondThreadStartLatch = new CountDownLatch( 1 );
        CountDownLatch latch = new CountDownLatch( 2 );
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool( 2 );
        List<String> results = new ArrayList<>();

        executor.execute( () -> {
            try {
                lock.acquire( transaction1, Lock.LockType.EXCLUSIVE, waitForGraph );
                results.add( "EXCLUSIVE_ACQUIRED" );
                secondThreadStartLatch.countDown();
                Thread.sleep( 1000 ); // ensure contention
                lock.release( transaction1, waitForGraph );
                results.add( "EXCLUSIVE_RELEASED" );
            } catch ( InterruptedException e ) {
                results.add( "EXCLUSIVE_FAILED" );
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        } );

        executor.execute( () -> {
            try {
                secondThreadStartLatch.await();
                lock.acquire( transaction2, Lock.LockType.SHARED, waitForGraph );
                results.add( "SHARED_ACQUIRED" );
                lock.release( transaction2, waitForGraph );
            } catch ( InterruptedException e ) {
                results.add( "SHARED_FAILED" );
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        } );

        latch.await();
        executor.shutdown();

        assertTrue( results.contains( "EXCLUSIVE_ACQUIRED" ) );
        assertTrue( results.contains( "EXCLUSIVE_RELEASED" ) );
        assertTrue( results.contains( "SHARED_ACQUIRED" ) );
    }


    @Test
    void testExclusiveLockWaitOnSharedLock() throws InterruptedException {
        Lock lock = new Lock();
        CountDownLatch secondThreadStartLatch = new CountDownLatch( 1 );
        CountDownLatch latch = new CountDownLatch( 2 );
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool( 2 );
        List<String> results = new ArrayList<>();

        executor.execute( () -> {
            try {

                lock.acquire( transaction1, Lock.LockType.SHARED, waitForGraph );
                results.add( "SHARED_ACQUIRED" );
                secondThreadStartLatch.countDown();
                lock.release( transaction1, waitForGraph );
            } catch ( InterruptedException e ) {
                results.add( "SHARED_FAILED" );
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        } );

        executor.execute( () -> {
            try {
                secondThreadStartLatch.await();
                lock.acquire( transaction2, Lock.LockType.EXCLUSIVE, waitForGraph );
                results.add( "EXCLUSIVE_ACQUIRED" );
                Thread.sleep( 1000 ); // ensure contention
                lock.release( transaction2, waitForGraph );
                results.add( "EXCLUSIVE_RELEASED" );
            } catch ( InterruptedException e ) {
                results.add( "EXCLUSIVE_FAILED" );
                Thread.currentThread().interrupt();
            } finally {
                latch.countDown();
            }
        } );

        latch.await();
        executor.shutdown();

        assertTrue( results.contains( "EXCLUSIVE_ACQUIRED" ) );
        assertTrue( results.contains( "EXCLUSIVE_RELEASED" ) );
        assertTrue( results.contains( "SHARED_ACQUIRED" ) );
    }

}

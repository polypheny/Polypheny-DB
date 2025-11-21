/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.transaction.locking;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.util.DeadlockException;

@Slf4j
public class LockManager {

    private static final LockManager INSTANCE = new LockManager();

    // This map contains active transactions and a lock ensuring only a single thread is using a transaction
    private final Map<Transaction, Lock> transactions = new ConcurrentHashMap<>();
    // The lock state of the database
    private final AtomicReference<Set<LockEntry>> entries = new AtomicReference<>( Set.of() );

    private final Map<Lockable, CompletableFuture<Boolean>> futures = new ConcurrentHashMap<>();
    private final Map<Transaction, Lockable> waitFor = new ConcurrentHashMap<>();


    public record LockEntry( Transaction transaction, Lockable lockable, LockType lockType ) {

    }


    private LockManager() {
    }


    public static LockManager getInstance() {
        return INSTANCE;
    }


    public void registerTransaction( Transaction transaction ) {
        transactions.put( transaction, new ReentrantLock() );
    }


    private static boolean canLockDirectly( Set<LockEntry> locks, LockEntry entry ) {
        if ( entry.lockType == LockType.SHARED && locks.stream().anyMatch( e -> e.transaction == entry.transaction && e.lockable == entry.lockable && e.lockType == LockType.EXCLUSIVE ) ) {
            // We already have an exclusive lock
            return true;
        }

        if ( entry.lockType == LockType.EXCLUSIVE && locks.stream().noneMatch( e -> e.transaction != entry.transaction && e.lockable == entry.lockable ) ) {
            // No one else holds a lock
            return true;
        } else if ( entry.lockType == LockType.SHARED && locks.stream().noneMatch( e -> e.lockable == entry.lockable && e.lockType == LockType.EXCLUSIVE ) ) { // No match for transaction, handled above
            // No one has an exclusive lock
            return true;
        }
        return false;
    }


    private static Set<LockEntry> tryAcquireLock( Set<LockEntry> locks, LockEntry entry ) {
        if ( canLockDirectly( locks, entry ) ) {
            Set<LockEntry> newLocks = new HashSet<>( locks );
            newLocks.add( entry );
            return Set.copyOf( newLocks );
        }
        return null;
    }


    private static Set<Transaction> findLockHolders( Transaction transaction, Lockable lockable, Set<LockEntry> locks ) {
        return locks.stream()
                .filter( e -> e.lockable == lockable && e.transaction != transaction )
                .map( e -> e.transaction )
                .collect( Collectors.toSet() );
    }


    private static boolean hasDeadlock( Set<LockEntry> locks, Map<Transaction, Lockable> waiting, Transaction transaction, Lockable lockable ) {
        // Transaction is waiting for lockable
        Set<Transaction> openTransactions = new HashSet<>( findLockHolders( transaction, lockable, locks ) );
        Set<Transaction> closedTransactions = new HashSet<>();
        while ( !openTransactions.isEmpty() ) {
            Transaction t = openTransactions.iterator().next();
            openTransactions.remove( t );
            closedTransactions.add( t );
            Lockable waitingFor = waiting.get( t );
            if ( waitingFor != null ) {
                for ( Transaction t2 : findLockHolders( t, waitingFor, locks ) ) {
                    if ( t2 == transaction ) {
                        return true; // Deadlock!
                    } else if ( !closedTransactions.contains( t2 ) ) {
                        openTransactions.add( t2 );
                    }
                }
            }
        }
        return false;
    }


    private boolean acquireLock( Transaction transaction, Lockable lockable, LockType lockType ) {
        Set<LockEntry> locks = entries.get();

        LockEntry entry = new LockEntry( transaction, lockable, lockType );

        if ( locks.contains( entry ) ) {
            // If we have the lock, return immediately
            return true;
        }

        Set<LockEntry> newLocks = tryAcquireLock( locks, entry );

        if ( newLocks != null ) {
            if ( entries.compareAndSet( locks, newLocks ) ) {
                Lockable l = waitFor.remove( transaction ); // No longer waiting
                if ( l != null && l != lockable ) {
                    throw new AssertionError( "Wrong lockable" );
                }
                return true;
            } else {
                return false; // Try again, someone else has acquired a lock
            }
        } else {
            waitFor.put( transaction, lockable ); // TODO: Two transactions could abort at the same time
            // If there is a Deadlock, this means that other Transactions are already waiting for us, so the relevant parts won't change
            if ( hasDeadlock( locks, Map.copyOf( waitFor ), transaction, lockable ) ) {
                throw new DeadlockException( "Deadlock detected" );
            }

            CompletableFuture<Boolean> future = futures.putIfAbsent( lockable, new CompletableFuture<>() );
            if ( future == null ) {
                // Retry, lock state could have changed already
                return false;
            }
            try {
                future.get();
            } catch ( InterruptedException | ExecutionException e ) {
                // ignore
            }
            return false;
        }
    }


    public void acquire( Transaction transaction, Lockable lockable, LockType lockType ) {
        Lock lock = transactions.get( transaction );
        if ( !lock.tryLock() ) {
            throw new GenericRuntimeException( "Multiple threads using same transaction" );
        }
        try {
            while ( true ) {
                if ( acquireLock( transaction, lockable, lockType ) ) {
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
    }


    public void releaseAllLocks( Transaction transaction ) {
        Lock lock = transactions.remove( transaction );
        if ( lock == null ) {
            return;
        }
        if ( !lock.tryLock() ) {
            throw new GenericRuntimeException( "Multiple threads using same transaction" );
        }
        try {
            while ( true ) {
                Set<LockEntry> locks = entries.get();
                Set<LockEntry> newEntries = locks.stream().filter( e -> e.transaction != transaction ).collect( Collectors.toUnmodifiableSet() );
                if ( entries.compareAndExchange( locks, newEntries ) == locks ) {
                    // Success
                    locks.stream().filter( e -> e.transaction == transaction ).forEach( e -> {
                        CompletableFuture<Boolean> future = futures.remove( e.lockable );
                        if ( future != null ) {
                            future.complete( true );
                        }
                    } );
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
    }

}

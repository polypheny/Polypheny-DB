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

package org.polypheny.db.transaction.locking;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.deadlocks.DeadlockHandler;
import org.polypheny.db.util.DeadlockException;

@Slf4j
public class LockableImpl implements Lockable {

    private final ReentrantLock concurrencyLock = new ReentrantLock( true );
    private final Condition concurrencyCondition = concurrencyLock.newCondition();
    private final HashMap<Transaction, Long> owners = new HashMap<>();
    private final Lockable parent;
    private LockState state = LockState.SHARED;


    public LockableImpl( Lockable Parent ) {
        this.parent = Parent;
    }


    public void acquire( @NotNull Transaction transaction, @NotNull LockType lockType ) throws DeadlockException {
        try {
            concurrencyLock.lock();
            if ( !owners.containsKey( transaction ) ) {
                switch ( lockType ) {
                    case SHARED -> acquireShared( transaction );
                    case EXCLUSIVE -> acquireExclusive( transaction );
                }
            } else {
                LockType heldLockType = getLockType();
                if ( heldLockType == lockType ) {
                    return;
                }
                if ( heldLockType == LockType.SHARED ) {
                    upgradeToExclusive( transaction );
                }
            }
        } catch ( InterruptedException e ) {
            concurrencyLock.unlock();
            transaction.releaseAllLocks();
            throw new DeadlockException( MessageFormat.format( "Transaction {0} encountered a deadlock while acquiring a lock of type {1} on entry {2}.", transaction.getId(), lockType, this ) );
        } catch ( DeadlockException e ) {
            concurrencyLock.unlock();
            transaction.releaseAllLocks();
            throw e;
        } catch ( Throwable t ) {
            concurrencyLock.unlock();
            log.error( "Unexpected exception while acquiring lock", t );
            transaction.releaseAllLocks();
            throw t;
        } finally {
            if ( concurrencyLock.isHeldByCurrentThread() ) {
                concurrencyLock.unlock();
            }
        }
    }


    private void upgradeToExclusive( Transaction transaction ) throws InterruptedException {
        if ( state == LockState.EXCLUSIVE ) {
            if ( owners.size() != 1 || !owners.containsKey( transaction ) ) {
                throw new AssertionError( "Exclusive lock not held exclusively" );
            }
            return;
        }
        while ( owners.size() != 1 ) {
            if ( owners.isEmpty() || !owners.containsKey( transaction ) ) {
                throw new AssertionError( "Expected the lock to have at least one owner and to be an owner of the lock" );
            }
            Set<Transaction> ownerSet = owners.keySet().stream().filter( t -> !t.equals( transaction ) ).collect( Collectors.toSet() );
            if ( DeadlockHandler.INSTANCE.addAndResolveDeadlock( this, transaction, ownerSet ) ) {
                throw new InterruptedException( "Deadlock detected" );
            }
            concurrencyCondition.await();
        }
        if ( !owners.containsKey( transaction ) ) {
            throw new AssertionError( "Expected to be the sole owner of the exclusive lock" );
        }
        state = LockState.EXCLUSIVE;
        printAcquiredInfo( "UEx", transaction );
    }


    public void release( @NotNull Transaction transaction ) {
        concurrencyLock.lock();
        if ( !owners.containsKey( transaction ) ) {
            concurrencyLock.unlock();
            return;
        }
        try {
            if ( state == LockState.EXCLUSIVE ) {
                if ( owners.size() != 1 ) {
                    throw new AssertionError( "Unlocking exclusive lock with multiple owners!" );
                }
                state = LockState.SHARED;
            }
            owners.remove( transaction );
            DeadlockHandler.INSTANCE.remove( this, transaction );
            concurrencyCondition.signalAll();
            printAcquiredInfo( "R", transaction );
        } finally {
            concurrencyLock.unlock();
            if ( !isRoot() ) {
                parent.release( transaction );
            }
        }
    }


    @Override
    public LockType getLockType() {
        return state == LockState.EXCLUSIVE ? LockType.EXCLUSIVE : LockType.SHARED;
    }


    public Map<Transaction, Long> getCopyOfOwners() {
        return Map.copyOf( owners );
    }


    @Override
    public boolean isRoot() {
        return parent == null;
    }


    private void acquireShared( Transaction transaction ) throws InterruptedException {
        if ( !isRoot() ) {
            parent.acquire( transaction, LockType.SHARED );
        }
        while ( state == LockState.EXCLUSIVE || hasWaitingTransactions() ) {
            if ( DeadlockHandler.INSTANCE.addAndResolveDeadlock( this, transaction, owners.keySet() ) ) {
                throw new InterruptedException( "Deadlock detected" );
            }
            concurrencyCondition.await();
        }
        if ( state != LockState.SHARED ) {
            throw new AssertionError( "Expected lock to be shared" );
        }
        owners.put( transaction, 1L );
        printAcquiredInfo( "ASh", transaction );
    }


    private void acquireExclusive( Transaction transaction ) throws InterruptedException {
        if ( !isRoot() ) {
            parent.acquire( transaction, LockType.SHARED );
        }
        while ( !owners.isEmpty() ) {
            if ( DeadlockHandler.INSTANCE.addAndResolveDeadlock( this, transaction, owners.keySet() ) ) {
                throw new InterruptedException( "Deadlock detected" );
            }
            concurrencyCondition.await();
        }
        if ( state != LockState.SHARED ) {
            throw new AssertionError( "Expected lock to be shared" );
        }
        state = LockState.EXCLUSIVE;
        owners.put( transaction, 1L );
        printAcquiredInfo( "AEx", transaction );
    }


    private boolean hasWaitingTransactions() {
        return concurrencyLock.hasWaiters( concurrencyCondition );
    }


    private void printAcquiredInfo( String message, Transaction transaction ) {
        log.debug( "{}, TX: {}, L: {}", message, transaction.getId(), this );
    }

}

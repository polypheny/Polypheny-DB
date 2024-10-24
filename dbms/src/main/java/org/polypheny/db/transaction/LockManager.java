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


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.transaction.xa.Xid;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Triple;

@Slf4j
public class LockManager {

    public static final LockManager INSTANCE = new LockManager();

    private boolean isExclusive = false;
    private final Set<Xid> owners = new HashSet<>();
    private final ConcurrentLinkedQueue<LockInformation> waiters = new ConcurrentLinkedQueue<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();


    private LockManager() {
    }


    public void lock( LockMode mode, @NonNull Transaction transaction ) throws DeadlockException {
        // Decide on which locking approach to focus
        synchronized ( this ) {
            if ( owners.isEmpty() ) {
                handleLockOrThrow( mode, transaction );
                return;
            } else if ( owners.contains( transaction.getXid() ) && (mode == LockMode.SHARED || isExclusive) ) {
                log.debug( "already locked {}", transaction.getXid() );
                // already have the required lock
                return;
            }
        }
        Thread thread = Thread.currentThread();

        synchronized ( waiters ) {
            if ( waiters.add( new LockInformation( thread, mode, transaction.getXid() ) ) ) {
                log.debug( "could not add" );
            }
        }

        // wait
        while ( true ) {
            lock.lock();
            try {
                //noinspection DataFlowIssue // else we have a general problem
                while ( waiters.peek().thread() != thread ) {
                    log.debug( "wait {} ", transaction.getXid() );
                    boolean successful = condition.await( RuntimeConfig.LOCKING_MAX_TIMEOUT_SECONDS.getInteger(), TimeUnit.SECONDS );

                    if ( !successful ) {
                        cleanupWaiters( thread );
                        lock.unlock();
                        log.warn( "open transactions isExclusive: {} in {}", isExclusive, owners );
                        log.warn( "waiters {}", waiters );
                        throw new DeadlockException( "Could not acquire lock, after max timeout was reached" );
                    }
                }
            } catch ( InterruptedException e ) {
                cleanupWaiters( thread );
                lock.unlock();
                throw new GenericRuntimeException( e );
            }

            lock.unlock();

            synchronized ( this ) {

                // try execute
                if ( handleSimpleLock( mode, transaction ) ) {
                    // remove successful
                    waiters.poll();
                    // signal
                    signalAll();

                    return;
                } else if ( owners.contains( transaction.getXid() ) && (mode == LockMode.EXCLUSIVE)
                        && owners.size() <= waiters.size()
                        // trx is owner and wants to upgrade, other transaction has the same -> deadlock
                        && waiters.stream().filter( w -> w.xid() != transaction.getXid() ).anyMatch( w -> owners.contains( w.xid() ) && w.mode() == LockMode.EXCLUSIVE ) ) {
                    cleanupWaiters( thread );
                    // we have to interrupt one transaction, all want to upgrade
                    throw new DeadlockException( "Write-write conflict with multiple transactions." );
                }

                if ( owners.isEmpty() ) {
                    cleanupWaiters( transaction.getXid() );
                    throw new GenericRuntimeException( "Could not acquire lock" );
                }

                // we wait until next signal
                if ( !owners.contains( transaction.getXid() ) && waiters.size() > 1 ) {
                    // not in owners list queue at the end -> current owner has shared or exclusive lock
                    waiters.add( waiters.poll() );
                    // signal
                    signalAll();
                }
            }

        }

    }


    private void cleanupWaiters( Thread thread ) {
        synchronized ( waiters ) {
            List<LockInformation> remove = waiters.stream().filter( w -> w.thread() == thread ).toList();
            if ( remove.isEmpty() ) {
                return;
            }
            assert remove.size() == 1;
            waiters.remove( remove.get( 0 ) );
        }
    }


    private void cleanupWaiters( PolyXid xid ) {
        synchronized ( waiters ) {
            List<LockInformation> remove = waiters.stream().filter( w -> w.xid() == xid ).toList();
            if ( remove.isEmpty() ) {
                return;
            }
            assert remove.size() == 1;
            assert remove.get( 0 ).thread() == Thread.currentThread();
            waiters.remove( remove.get( 0 ) );
        }
    }


    private void handleLockOrThrow( LockMode mode, @NotNull Transaction transaction ) {
        if ( !handleSimpleLock( mode, transaction ) ) {
            throw new GenericRuntimeException( "Could not acquire lock, as single transaction" );
        }
    }


    private synchronized boolean handleSimpleLock( @NonNull LockMode mode, Transaction transaction ) {
        if ( mode == LockMode.EXCLUSIVE ) {
            // get w
            if ( owners.isEmpty() || (owners.size() == 1 && owners.contains( transaction.getXid() )) ) {
                if ( isExclusive ) {
                    log.debug( "lock already exclusive" );
                    return true;
                }

                log.debug( "x lock {}", transaction.getXid() );
                isExclusive = true;
                owners.add( transaction.getXid() );
                return true;
            }

        } else {
            // get r
            if ( !isExclusive || owners.contains( transaction.getXid() ) ) {
                log.debug( "r lock {}", transaction.getXid() );
                owners.add( transaction.getXid() );
                return true;
            }

        }
        return false;
    }


    private void signalAll() {
        lock.lock();
        try {
            synchronized ( condition ) {
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }


    public synchronized void unlock( @NonNull Transaction transaction ) {
        if ( !owners.contains( transaction.getXid() ) ) {
            log.debug( "Transaction is no owner" );
            return;
        }

        if ( isExclusive ) {
            isExclusive = false;
        }
        log.debug( "release {}", transaction.getXid() );
        owners.remove( transaction.getXid() );

        cleanupWaiters( transaction.getXid() );

        // wake up waiters
        signalAll();
    }


    public void removeTransaction( @NonNull Transaction transaction ) {
        unlock( transaction );
    }

}

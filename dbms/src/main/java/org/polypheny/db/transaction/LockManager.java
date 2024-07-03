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


import com.google.common.base.Stopwatch;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
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

@Slf4j
public class LockManager implements Runnable {

    public static final LockManager INSTANCE = new LockManager();

    private boolean isExclusive = false;
    private final Set<Xid> owners = new HashSet<>();
    private final ConcurrentLinkedQueue<Thread> waiters = new ConcurrentLinkedQueue<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();


    private LockManager() {
    }


    public void lock( LockMode mode, @NonNull TransactionImpl transaction ) throws DeadlockException {
        // Decide on which locking  approach to focus
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

        waiters.add( thread );

        Stopwatch watch = Stopwatch.createStarted();
        // wait
        while ( true ) {
            lock.lock();
            try {
                while ( waiters.peek() != thread ) {
                    if ( watch.elapsed().getSeconds() > RuntimeConfig.LOCKING_MAX_TIMEOUT_SECONDS.getInteger() ) {
                        cleanup( thread );
                        throw new DeadlockException( new GenericRuntimeException( "Could not acquire lock, after max timeout was reached" ) );
                    }
                    log.debug( "wait {} ", transaction.getXid() );
                    condition.await();
                }
            } catch ( InterruptedException e ) {
                cleanup( thread );
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
                }

                if ( owners.isEmpty() ) {
                    waiters.remove( thread );
                    throw new GenericRuntimeException( "Could not acquire lock" );
                }
            }
            // we wait until next signal
        }

    }


    private void cleanup( Thread thread ) {
        waiters.remove( thread );
        lock.unlock();
    }


    private void handleLockOrThrow( LockMode mode, @NotNull TransactionImpl transaction ) {
        if ( !handleSimpleLock( mode, transaction ) ) {
            throw new GenericRuntimeException( "Could not acquire lock, as single transaction" );
        }
    }



    private synchronized boolean handleSimpleLock( @NonNull LockMode mode, TransactionImpl transaction ) {
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
        try{
            synchronized ( condition ) {
                condition.signalAll();
            }
        }finally {
            lock.unlock();
        }
    }


    public synchronized void unlock( @NonNull TransactionImpl transaction ) {
        if ( !owners.contains( transaction.getXid() ) ) {
            log.debug( "Transaction is no owner" );
            return;
        }

        if ( isExclusive ) {
            isExclusive = false;
        }
        log.debug( "release {}", transaction.getXid() );
        owners.remove( transaction.getXid() );

        // wake up waiters
        signalAll();
    }


    public void removeTransaction( @NonNull TransactionImpl transaction ) {
        unlock( transaction );
    }


    @Override
    public void run() {

    }

}

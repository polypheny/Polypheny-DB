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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Lock {

    public enum LockType {
        SHARED,
        EXCLUSIVE
    }


    private static final long EXCLUSIVE_OWNER = -1;
    private static final long NO_OWNER = 0;

    private final ReentrantLock concurrencyLock = new ReentrantLock( true );
    private final Condition concurrencyCondition = concurrencyLock.newCondition();

    private long ownership = 0;


    public void aquire( LockType lockType ) throws InterruptedException {
        switch ( lockType ) {
            case SHARED -> acquireShared();
            case EXCLUSIVE -> acquireExclusive();
        }
    }


    public void upgradeToExclusive() throws InterruptedException {
        concurrencyLock.lock();
        try {
            if ( ownership == EXCLUSIVE_OWNER ) {
                return;
            }
            while ( ownership != NO_OWNER ) {
                concurrencyCondition.await();
            }
            ownership = EXCLUSIVE_OWNER;
        } finally {
            concurrencyLock.unlock();
        }
    }


    public void release() {
        concurrencyLock.lock();
        try {
            if ( ownership == EXCLUSIVE_OWNER ) {
                ownership = NO_OWNER;
            }
            if ( ownership != NO_OWNER ) {
                ownership--;
            }
            concurrencyCondition.signalAll();
        } finally {
            concurrencyLock.unlock();
        }
    }

    public LockType getLockType() {
        if ( ownership == EXCLUSIVE_OWNER ) {
            return LockType.EXCLUSIVE;
        }
        return LockType.SHARED;
    }


    private void acquireShared() throws InterruptedException {
        concurrencyLock.lock();
        try {
            while ( ownership == EXCLUSIVE_OWNER || hasWaitingTransactions() ) {
                concurrencyCondition.await();
            }
            ownership++;
        } finally {
            concurrencyLock.unlock();
        }
    }


    private void acquireExclusive() throws InterruptedException {
        concurrencyLock.lock();
        try {
            while ( ownership != NO_OWNER ) {
                concurrencyCondition.await();
            }
            ownership = EXCLUSIVE_OWNER;
        } finally {
            concurrencyLock.unlock();
        }
    }


    private boolean hasWaitingTransactions() {
        return concurrencyLock.hasWaiters( concurrencyCondition );
    }


}

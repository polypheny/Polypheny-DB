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
import java.util.Set;
import com.google.common.base.Stopwatch;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.util.DeadlockException;
import javax.transaction.xa.Xid;

@Slf4j
public class LockManager {

    public static final LockManager INSTANCE = new LockManager();

    private boolean isExclusive = false;
    private final Set<Xid> owners = new HashSet<>();


    private LockManager() {
    }


    public void lock( LockMode mode, @NonNull TransactionImpl transaction ) throws DeadlockException {
        // Decide on which locking  approach to focus
        Stopwatch watch = Stopwatch.createStarted();
        while ( !handleSimpleLock(mode, transaction )){
            if( watch.elapsed().getSeconds() > 10 ){
                throw new DeadlockException( new GenericRuntimeException( "Could not get lock after retry" ) );
            }
            try {
                Thread.sleep( 5 );
            } catch ( InterruptedException e ) {
                // ignored
            }
        }
    }


    private synchronized boolean handleSimpleLock( @NonNull LockMode mode, TransactionImpl transaction ) {
        if ( mode == LockMode.EXCLUSIVE ) {
            // get w
            if ( owners.isEmpty() || (owners.size() == 1 && owners.contains( transaction.getXid() ))) {

                isExclusive = true;
                owners.add( transaction.getXid() );
                return true;
            }

        } else {
            // get r
            if ( !isExclusive || owners.contains( transaction.getXid()) ) {
                owners.add( transaction.getXid() );
                return true;
            }

        }
        return false;
    }



    public synchronized void unlock( @NonNull TransactionImpl transaction ) {

        if ( !owners.contains( transaction.getXid()) ) {
            log.debug( "Transaction is no owner" );
            return;
        }

        if ( isExclusive ) {
            isExclusive = false;
        }

        owners.remove( transaction.getXid() );

    }


    public void removeTransaction( @NonNull TransactionImpl transaction ) {
        unlock( transaction );
    }


}

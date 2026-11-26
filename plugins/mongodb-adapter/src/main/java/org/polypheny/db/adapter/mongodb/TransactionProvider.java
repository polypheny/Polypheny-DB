/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.adapter.mongodb;

import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.TransactionOptions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.transaction.PolyXid;

/**
 * This class handles the MongoDB transaction logic,
 * it starts new sessions, for transactions and exposes the functionality to
 * commit or rollback these transactions
 */
@Slf4j
public class TransactionProvider {

    @Setter
    private volatile MongoClient client;

    private final ConcurrentMap<PolyXid, ClientSession> sessions = new ConcurrentHashMap<>();


    public TransactionProvider( MongoClient client ) {
        this.client = client;
    }


    /**
     * Starts a new transaction for a provided PolyXid
     *
     * @param xid the PolyXid to which the transaction belongs
     * @return the corresponding session, which holds the information of the transaction
     */
    public ClientSession startTransaction( PolyXid xid, boolean withTrx ) {
        return sessions.compute( xid, ( ignored, session ) -> {
            if ( session != null ) {
                if ( withTrx && !session.hasActiveTransaction() ) {
                    session.startTransaction();
                }
                return session;
            }

            ClientSession created = client.startSession();
            try {
                if ( withTrx ) {
                    created.startTransaction( TransactionOptions.builder().build() );
                }
                return created;
            } catch ( RuntimeException failure ) {
                try {
                    created.close();
                } catch ( RuntimeException cleanupFailure ) {
                    failure.addSuppressed( cleanupFailure );
                }
                throw failure;
            }
        } );
    }


    /**
     * Tries to commit the transaction,
     * if the commit fails the transaction is rollbacked
     * if the transaction is already commit this is a no-op
     *
     * @param xid the corresponding PolyXid for the transaction
     */
    public void commit( PolyXid xid ) {
        ClientSession session = sessions.remove( xid );
        if ( session != null ) {
            try ( session ) {
                try {
                    if ( session.hasActiveTransaction() ) {
                        session.commitTransaction();
                    }
                } catch ( MongoClientException | MongoCommandException failure ) {
                    try {
                        if ( session.hasActiveTransaction() ) {
                            session.abortTransaction();
                        }
                    } catch ( RuntimeException cleanupFailure ) {
                        failure.addSuppressed( cleanupFailure );
                    }
                    throw failure;
                }
            }
        } else {
            log.info( "No-op commit" );
        }
    }


    /**
     * Tries to rollback a transaction,
     * if the transaction is already rollbacked
     * this is a no-op
     *
     * @param xid the corresponding PolyXid
     */
    public void rollback( PolyXid xid ) {
        ClientSession session = sessions.remove( xid );
        if ( session != null ) {
            try ( session ) {
                if ( session.hasActiveTransaction() ) {
                    session.abortTransaction();
                }
            } catch ( MongoClientException e ) {
                // empty on purpose
            }
        } else {
            log.info( "No-op rollback" );
        }
    }


    public ClientSession getSession( PolyXid xid ) {
        ClientSession session = sessions.get( xid );
        return session == null ? startTransaction( xid, true ) : session;
    }

}

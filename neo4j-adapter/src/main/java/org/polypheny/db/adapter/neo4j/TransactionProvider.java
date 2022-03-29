/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adapter.neo4j;

import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.util.Pair;

@Slf4j
public class TransactionProvider {

    private final Driver db;

    private final ConcurrentHashMap<PolyXid, Pair<Session, Transaction>> register = new ConcurrentHashMap<>();


    public TransactionProvider( Driver db ) {
        this.db = db;
    }


    synchronized public Transaction get( PolyXid xid ) {
        if ( register.containsKey( xid ) ) {
            return register.get( xid ).right;
        }
        return startSession( xid );
    }


    synchronized void commit( PolyXid xid ) {
        if ( register.containsKey( xid ) ) {
            Pair<Session, Transaction> pair = register.get( xid );
            pair.right.commit();
            pair.right.close();
            pair.left.close();
        }
    }


    synchronized void rollback( PolyXid xid ) {
        if ( register.containsKey( xid ) ) {
            Pair<Session, Transaction> pair = register.get( xid );
            pair.right.rollback();
            pair.right.close();
            pair.left.close();
        }
    }


    synchronized public Transaction startSession( PolyXid xid ) {
        Session session = db.session();
        Transaction trx = session.beginTransaction();
        ;
        register.put( xid, Pair.of( session, trx ) );
        return trx;
    }

}

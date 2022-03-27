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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.polypheny.db.transaction.PolyXid;

public class TransactionProvider {

    private final Driver db;

    private final Map<PolyXid, Transaction> register = new HashMap<>();
    private final Session session;


    public TransactionProvider( Driver db, Session session ) {
        this.db = db;
        this.session = session;
    }


    public Transaction get( PolyXid xid ) {
        if ( register.containsKey( xid ) ) {
            return register.get( xid );
        }
        commitAll();
        Transaction trx = session.beginTransaction();
        put( xid, trx );
        return trx;
    }


    void commit( PolyXid xid ) {
        register.get( xid ).commit();
        register.remove( xid );
    }


    void rollback( PolyXid xid ) {
        register.get( xid ).rollback();
        register.remove( xid );
    }


    private void commitAll() {
        for ( PolyXid polyXid : new ArrayList<>( register.keySet() ) ) {
            commit( polyXid );
        }
    }


    public void put( PolyXid xid, Transaction transaction ) {
        register.put( xid, transaction );
    }

}

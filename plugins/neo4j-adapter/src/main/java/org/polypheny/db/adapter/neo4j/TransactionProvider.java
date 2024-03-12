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

package org.polypheny.db.adapter.neo4j;

import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.util.Pair;


@Slf4j
public class TransactionProvider {

    private final Driver db;

    private final ConcurrentHashMap<PolyXid, Pair<Session, Transaction>> register = new ConcurrentHashMap<>();

    private Session ddlSession;
    private Transaction ddlTransaction;


    /**
     * Handler class, which abstract the transaction logic of the neo4j driver.
     */
    public TransactionProvider( Driver db ) {
        this.db = db;
    }


    /**
     * Returns the associated transaction for the given {@link PolyXid}.
     *
     * @param xid the target transaction from Polypheny
     * @return the driver transaction from the Neo4j driver
     */
    synchronized public Transaction get( PolyXid xid ) {
        if ( register.containsKey( xid ) ) {
            return register.get( xid ).right;
        }
        return startSession( xid );
    }


    /**
     * If the requested transaction exists it gets committed.
     *
     * @param xid the identifier for the transaction
     */
    synchronized void commit( PolyXid xid ) {
        if ( register.containsKey( xid ) ) {
            Pair<Session, Transaction> pair = register.get( xid );
            if ( pair.right.isOpen() ) {
                pair.right.commit();
            }

            pair.right.close();
            pair.left.close();
        }
    }


    /**
     * If the requested transaction exists it gets rolled back.
     *
     * @param xid the identifier for the transaction
     */
    synchronized void rollback( PolyXid xid ) {
        if ( register.containsKey( xid ) ) {
            Pair<Session, Transaction> pair = register.get( xid );
            if ( pair.right.isOpen() ) {
                pair.right.rollback();
                pair.right.close();
            }
            pair.left.close();
        }
    }


    /**
     * Starts a new Neo4j driver session and returns a transaction for a given {@link PolyXid}.
     *
     * @param xid the identifier form Polypheny.
     * @return a Neo4j driver specific transaction
     */
    synchronized public Transaction startSession( PolyXid xid ) {
        Session session = db.session();
        Transaction trx = session.beginTransaction();
        register.put( xid, Pair.of( session, trx ) );
        return trx;
    }


    public Transaction getDdlTransaction() {
        if ( ddlSession == null || ddlTransaction == null ) {
            ddlSession = db.session();
            ddlTransaction = ddlSession.beginTransaction();
        }
        return ddlTransaction;
    }


    public void commitDdlTransaction() {
        commitAll();
    }


    public void rollbackDdlTransaction() {
        if ( !ddlTransaction.isOpen() ) {
            throw new GenericRuntimeException( "There is no ongoing DDL transaction!" );
        }
        ddlTransaction.rollback();
        ddlTransaction.close();
        ddlSession.close();
        ddlTransaction = null;
        ddlSession = null;
    }


    public void commitAll() {
        if ( ddlSession != null && ddlTransaction != null ) {
            ddlTransaction.commit();
            ddlTransaction.close();
            ddlSession.close();
            ddlTransaction = null;
            ddlSession = null;
        }

        register.keySet().forEach( this::commit );
    }

}

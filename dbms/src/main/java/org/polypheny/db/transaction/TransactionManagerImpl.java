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


import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.transaction.PUID.ConnectionId;
import org.polypheny.db.transaction.PUID.NodeId;
import org.polypheny.db.transaction.PUID.Type;
import org.polypheny.db.transaction.PUID.UserId;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;


@Slf4j
public class TransactionManagerImpl implements TransactionManager {

    private final ConcurrentHashMap<PolyXid, Transaction> transactions = new ConcurrentHashMap<>();

    private final AtomicLong totalTransactions = new AtomicLong( 0 );


    private TransactionManagerImpl() {
        InformationManager im = InformationManager.getInstance();
        InformationPage page = new InformationPage( "Transactions" );
        page.fullWidth();
        im.addPage( page );
        InformationGroup runningTransactionsGroup = new InformationGroup( page, "Running Transactions" );
        im.addGroup( runningTransactionsGroup );
        InformationTable runningTransactionsTable = new InformationTable(
                runningTransactionsGroup,
                Arrays.asList( "ID", "XID Hash", "Statements", "Analyze", "Involved Adapters", "Origin" ) );
        im.registerInformation( runningTransactionsTable );
        page.setRefreshFunction( () -> {
            runningTransactionsTable.reset();
            transactions.forEach( ( k, v ) -> runningTransactionsTable.addRow(
                    v.getId(),
                    k.toString().hashCode(),
                    v.getNumberOfStatements(),
                    v.isAnalyze(),
                    v.getInvolvedAdapters().stream().map( Adapter::getUniqueName ).collect( Collectors.joining( ", " ) ),
                    v.getOrigin() ) );
        } );
    }


    private static final class InstanceHolder {

        private static final TransactionManager INSTANCE = new TransactionManagerImpl();

    }


    public static TransactionManager getInstance() {
        return InstanceHolder.INSTANCE;
    }


    private Transaction startTransaction( LogicalUser user, LogicalNamespace defaultNamespace, boolean analyze, String origin, MultimediaFlavor flavor ) {
        final NodeId nodeId = (NodeId) PUID.randomPUID( Type.NODE ); // TODO: get real node id -- configuration.get("nodeid")
        final UserId userId = (UserId) PUID.randomPUID( Type.USER ); // TODO: use real user id
        final ConnectionId connectionId = (ConnectionId) PUID.randomPUID( Type.CONNECTION ); // TODO
        PolyXid xid = generateNewTransactionId( nodeId, userId, connectionId );
        transactions.put( xid, new TransactionImpl( xid, this, user, defaultNamespace, analyze, origin, flavor ) );
        totalTransactions.incrementAndGet();
        return transactions.get( xid );
    }


    @Override
    public Transaction startTransaction( long userId, long defaultNamespaceId, boolean analyze, String origin ) {
        return startTransaction(
                Catalog.snapshot().getUser( userId ).orElse( null ),
                Catalog.snapshot().getNamespace( defaultNamespaceId ).orElse( null ),
                analyze,
                origin,
                MultimediaFlavor.DEFAULT );
    }


    @Override
    public Transaction startTransaction( long userId, long defaultNamespaceId, boolean analyze, String origin, MultimediaFlavor flavor ) {
        return startTransaction( Catalog.snapshot().getUser( userId ).orElseThrow(), Catalog.snapshot().getNamespace( defaultNamespaceId ).orElseThrow(), analyze, origin, flavor );
    }


    @Override
    public Transaction startTransaction( long userId, boolean analyze, String origin ) {
        return startTransaction(
                Catalog.snapshot().getUser( userId ).orElseThrow(),
                Catalog.snapshot().getNamespace( Catalog.defaultNamespaceId ).orElseThrow(),
                analyze,
                origin,
                MultimediaFlavor.DEFAULT );
    }


    @Override
    public void removeTransaction( PolyXid xid ) {
        if ( !transactions.containsKey( xid ) ) {
            log.warn( "Unknown transaction id: {}", xid );
        } else {
            transactions.remove( xid );
        }
    }


    @Override
    public boolean isActive( PolyXid xid ) {
        return transactions.containsKey( xid );
    }


    private static PolyXid generateNewTransactionId( final NodeId nodeId, final UserId userId, final ConnectionId connectionId ) {
        return Utils.generateGlobalTransactionIdentifier( nodeId, userId, connectionId, PUID.randomPUID( PUID.Type.TRANSACTION ) );
    }


    @Override
    public long getNumberOfActiveTransactions() {
        return transactions.size();
    }


    @Override
    public long getNumberOfTotalTransactions() {
        return totalTransactions.get();
    }

}

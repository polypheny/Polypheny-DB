/*
 * Copyright 2019-2020 The Polypheny Project
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
import java.util.stream.Collectors;
import org.polypheny.db.PUID;
import org.polypheny.db.PUID.ConnectionId;
import org.polypheny.db.PUID.NodeId;
import org.polypheny.db.PUID.Type;
import org.polypheny.db.PUID.UserId;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.catalog.CatalogManagerImpl;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.util.background.BackgroundTask.TaskPriority;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;
import org.polypheny.db.util.background.BackgroundTaskManager;


public class TransactionManagerImpl implements TransactionManager {

    private ConcurrentHashMap<PolyXid, Transaction> transactions = new ConcurrentHashMap<>();


    public TransactionManagerImpl() {
        InformationManager im = InformationManager.getInstance();
        InformationPage page = new InformationPage( "Transactions", "Transactions" );
        page.fullWidth();
        im.addPage( page );
        InformationGroup runningTransactionsGroup = new InformationGroup( page, "Running Transactions" );
        im.addGroup( runningTransactionsGroup );
        InformationTable runningTransactionsTable = new InformationTable(
                runningTransactionsGroup,
                Arrays.asList( "ID", "Analyze", "Involved Stores" ) );
        im.registerInformation( runningTransactionsTable );
        BackgroundTaskManager.INSTANCE.registerTask(
                () -> {
                    runningTransactionsTable.reset();
                    transactions.forEach( ( k, v ) -> runningTransactionsTable.addRow( k.getGlobalTransactionId(), v.isAnalyze(), v.getInvolvedStores().stream().map( Store::getUniqueName ).collect( Collectors.joining( ", " ) ) ) );
                },
                "Update transaction overview",
                TaskPriority.LOW,
                TaskSchedulingType.EVERY_FIVE_SECONDS );
    }


    @Override
    public Transaction startTransaction( CatalogUser user, CatalogSchema defaultSchema, CatalogDatabase database, boolean analyze ) {
        final NodeId nodeId = (NodeId) PUID.randomPUID( Type.NODE ); // TODO: get real node id -- configuration.get("nodeid")
        final UserId userId = (UserId) PUID.randomPUID( Type.USER ); // TODO: use real user id
        final ConnectionId connectionId = (ConnectionId) PUID.randomPUID( Type.CONNECTION ); // TODO
        PolyXid xid = generateNewTransactionId( nodeId, userId, connectionId );
        transactions.put( xid, new TransactionImpl( xid, this, user, defaultSchema, database, analyze ) );
        return transactions.get( xid );
    }


    @Override
    public Transaction startTransaction( String user, String database, boolean analyze ) throws GenericCatalogException, UnknownUserException, UnknownDatabaseException, UnknownSchemaException {
        CatalogUser catalogUser = CatalogManagerImpl.getInstance().getUser( user );

        // TODO MV: This is not nice and should be replaced
        // Because of the current implementation of the catalog requiring a transaction id for schema requests we first  need to create a "dummy" transaction for accessing the catalog
        // to get the actual information required for starting the actual transaction.
        Transaction transaction = startTransaction( catalogUser, null, null, false );
        CatalogDatabase catalogDatabase = transaction.getCatalog().getDatabase( database );
        CatalogSchema catalogSchema = transaction.getCatalog().getSchema( catalogDatabase.id, catalogDatabase.defaultSchemaName );
        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            throw new RuntimeException( e );
        }

        return startTransaction( catalogUser, catalogSchema, catalogDatabase, analyze );
    }


    @Override
    public void removeTransaction( PolyXid xid ) throws TransactionException {
        if ( !transactions.containsKey( xid ) ) {
            throw new TransactionException( "Unknown transaction id: " + xid );
        }
        transactions.remove( xid );
    }


    private static PolyXid generateNewTransactionId( final NodeId nodeId, final UserId userId, final ConnectionId connectionId ) {
        return Utils.generateGlobalTransactionIdentifier( nodeId, userId, connectionId, PUID.randomPUID( PUID.Type.TRANSACTION ) );
    }


}

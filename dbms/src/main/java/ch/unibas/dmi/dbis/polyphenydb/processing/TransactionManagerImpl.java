/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.processing;


import ch.unibas.dmi.dbis.polyphenydb.PUID;
import ch.unibas.dmi.dbis.polyphenydb.PUID.ConnectionId;
import ch.unibas.dmi.dbis.polyphenydb.PUID.NodeId;
import ch.unibas.dmi.dbis.polyphenydb.PUID.Type;
import ch.unibas.dmi.dbis.polyphenydb.PUID.UserId;
import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.TransactionException;
import ch.unibas.dmi.dbis.polyphenydb.TransactionManager;
import ch.unibas.dmi.dbis.polyphenydb.Utils;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManagerImpl;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransactionManagerImpl implements TransactionManager {

    private static final Logger LOG = LoggerFactory.getLogger( TransactionManagerImpl.class );

    private ConcurrentHashMap<PolyXid, Transaction> transactions = new ConcurrentHashMap<>();


    @Override
    public Transaction startTransaction( CatalogUser user, CatalogSchema defaultSchema, CatalogDatabase database ) {
        final NodeId nodeId = (NodeId) PUID.randomPUID( Type.NODE ); // TODO: get real node id -- configuration.get("nodeid")
        final UserId userId = (UserId) PUID.randomPUID( Type.USER ); // TODO: use real user id
        final ConnectionId connectionId = (ConnectionId) PUID.randomPUID( Type.CONNECTION ); // TODO
        PolyXid xid = generateNewTransactionId( nodeId, userId, connectionId );
        transactions.put( xid, new TransactionImpl( xid, this, user, defaultSchema, database ) );
        return transactions.get( xid );
    }


    @Override
    public Transaction startTransaction( String user, String database ) throws GenericCatalogException, UnknownUserException, UnknownDatabaseException, UnknownSchemaException {
        CatalogUser catalogUser = CatalogManagerImpl.getInstance().getUser( user );

        Transaction transaction = startTransaction( catalogUser, null, null );
        CatalogDatabase catalogDatabase = transaction.getCatalog().getDatabase( database );
        CatalogSchema catalogSchema = transaction.getCatalog().getSchema( catalogDatabase.id, catalogDatabase.defaultSchemaName );

        return startTransaction( catalogUser, catalogSchema, catalogDatabase );
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

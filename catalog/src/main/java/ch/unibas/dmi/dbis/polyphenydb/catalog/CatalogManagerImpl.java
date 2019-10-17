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

package ch.unibas.dmi.dbis.polyphenydb.catalog;


import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogStore;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogTransactionException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import lombok.val;


/**
 *
 */
@Slf4j
public class CatalogManagerImpl extends CatalogManager {

    private static final boolean CREATE_SCHEMA = true;

    private static final CatalogManagerImpl INSTANCE = new CatalogManagerImpl();

    private final ConcurrentHashMap<PolyXid, CatalogImpl> catalogs = new ConcurrentHashMap<>();


    public static CatalogManagerImpl getInstance() {
        return INSTANCE;
    }


    private CatalogManagerImpl() {
        if ( CREATE_SCHEMA ) {
            LocalTransactionHandler transactionHandler = null;
            try {
                transactionHandler = LocalTransactionHandler.getTransactionHandler();
                Statements.dropSchema( transactionHandler );
                Statements.createSchema( transactionHandler );
                transactionHandler.commit();
            } catch ( CatalogConnectionException | CatalogTransactionException e ) {
                log.error( "Exception while creating catalog schema", e );
                try {
                    if ( transactionHandler != null ) {
                        transactionHandler.rollback();
                    }
                } catch ( CatalogTransactionException e1 ) {
                    log.error( "Exception while rollback", e );
                }
            }
        }
    }


    /**
     * Returns the user with the specified name.
     *
     * @param username The username
     * @return The user
     * @throws UnknownUserException If there is no user with this name.
     */
    @Override
    public CatalogUser getUser( String username ) throws UnknownUserException, GenericCatalogException {
        try {
            val transactionHandler = LocalTransactionHandler.getTransactionHandler();
            return Statements.getUser( transactionHandler, username );
        } catch ( CatalogConnectionException | GenericCatalogException e ) {
            throw new GenericCatalogException( e );
        }
    }

    /**
     * Get list of all stores
     *
     * @return List of stores
     */
    public List<CatalogStore> getStores() throws GenericCatalogException {
        try {
            val transactionHandler = LocalTransactionHandler.getTransactionHandler();
            return Statements.getStores( transactionHandler );
        } catch ( CatalogConnectionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public Catalog getCatalog( PolyXid xid ) {
        if ( !catalogs.containsKey( xid ) ) {
            catalogs.put( xid, new CatalogImpl( xid ) );
        }
        return catalogs.get( xid );
    }


    void removeCatalog( PolyXid xid ) {
        if ( !catalogs.containsKey( xid ) ) {
            log.error( "There is no catalog instance associated with this transaction id." );
        } else {
            catalogs.remove( xid );
        }
    }


}

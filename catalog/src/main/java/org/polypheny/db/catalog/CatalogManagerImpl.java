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

package ch.unibas.dmi.dbis.polyphenydb.catalog;


import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogTransactionException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
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

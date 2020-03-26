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

package org.polypheny.db.catalog;


import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.transaction.PolyXid;


/**
 *
 */
@Slf4j
public class CatalogManagerImpl extends CatalogManager {

    private static final boolean CREATE_SCHEMA = true;

    //private static CatalogManagerImpl INSTANCE = new CatalogManagerImpl();

    private final ConcurrentHashMap<PolyXid, CatalogImpl> catalogs = new ConcurrentHashMap<>();

    private static CatalogImpl catalog = null;


    public static CatalogManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new CatalogManagerImpl();
        }
        return INSTANCE;
    }

    /*
    public static CatalogManager getInstance() {
        return INSTANCE;
    }*/


    private CatalogManagerImpl() {
        catalog = new CatalogImpl();
    }


    /**
     * Returns the user with the specified name.
     *
     * @param username The username
     * @return The user
     * @throws UnknownUserException If there is no user with this name.
     */
    @Override
    public CatalogUser getUser( String username ) throws UnknownUserException {
        return catalog.getUser( username );
    }


    @Override
    public Catalog getCatalog( PolyXid xid ) {
        return catalog;
    }


    @Override
    public Catalog getCatalog() {
        return catalog;
    }


    void removeCatalog( PolyXid xid ) {
        log.error( "removing store" );
    }
}

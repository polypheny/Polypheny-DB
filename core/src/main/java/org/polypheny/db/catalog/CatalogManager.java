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


import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.information.Information;
import org.polypheny.db.transaction.PolyXid;


public abstract class CatalogManager {

    public static CatalogManager INSTANCE = null;

    public static CatalogManager setAndGetInstance( CatalogManager catalogManager ) {
        if( INSTANCE != null ){
            throw new RuntimeException( "Setting the CatalogManager, when already set is not permitted." );
        }
        INSTANCE = catalogManager;
        return INSTANCE;
    }

    public static CatalogManager getInstance() {
        if( INSTANCE == null ){
            throw new RuntimeException( "CatalogManager was not set correctly on Polypheny-DB start-up" );
        }
        return INSTANCE;
    }


    public abstract Catalog getCatalog( PolyXid xid );

    public abstract Catalog getCatalog();

}

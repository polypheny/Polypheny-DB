/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.ddl;

import org.apache.calcite.linq4j.function.Deterministic;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.iface.QueryInterfaceManager.QueryInterfaceType;

@Deterministic
public class DefaultInserter {


    /**
     * Fills the catalog database with default data, skips if data is already inserted
     */
    public static void restoreData( DdlManager ddlManager ) {
        final Catalog catalog = Catalog.getInstance();
        restoreUsers( catalog );

        //////////////
        // init schema

        catalog.addNamespace( "public", NamespaceType.getDefault(), false );

        //////////////
        // init adapters

        restoreAdapters( ddlManager, catalog );

        catalog.commit();

    }


    public static void restoreInterfaces() {
        restoreAvatica();
        restoreInterfacesIfNecessary();
    }


    private static void restoreAdapters( DdlManager ddlManager, Catalog catalog ) {
        if ( catalog.getAdapters().size() != 0 ) {
            catalog.commit();
            return;
        }

        catalog.updateSnapshot();

        // Deploy default store
        ddlManager.addAdapter( "hsqldb", Catalog.defaultStore.getAdapterName(), AdapterType.STORE, Catalog.defaultStore.getDefaultSettings() );
        // Deploy default CSV view
        ddlManager.addAdapter( "hr", Catalog.defaultSource.getAdapterName(), AdapterType.SOURCE, Catalog.defaultSource.getDefaultSettings() );
    }


    private static void restoreUsers( Catalog catalog ) {
        //////////////
        // init users
        long systemId = catalog.addUser( "system", "" );

        catalog.addUser( "pa", "" );

        Catalog.defaultUserId = systemId;
    }


    public static void restoreInterfacesIfNecessary() {
        ////////////////////////
        // init query interfaces
        if ( Catalog.getInstance().getInterfaces().size() != 0 ) {
            return;
        }
        QueryInterfaceManager.getREGISTER().values().forEach( i -> Catalog.getInstance().addQueryInterface( i.interfaceName, i.clazz.getName(), i.defaultSettings ) );
        Catalog.getInstance().commit();

    }


    public static void restoreAvatica() {
        if ( Catalog.snapshot().getQueryInterface( "avatica" ) != null ) {
            return;
        }
        QueryInterfaceType avatica = QueryInterfaceManager.getREGISTER().get( "AvaticaInterface" );
        Catalog.getInstance().addQueryInterface( "avatica", avatica.clazz.getName(), avatica.defaultSettings );
    }

}

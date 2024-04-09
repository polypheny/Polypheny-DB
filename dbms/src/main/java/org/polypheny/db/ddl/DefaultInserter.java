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

package org.polypheny.db.ddl;

import java.util.Map;
import org.apache.calcite.linq4j.function.Deterministic;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.iface.QueryInterfaceManager.QueryInterfaceTemplate;
import org.polypheny.db.util.RunMode;

@Deterministic
public class DefaultInserter {


    public static final String DEFAULT_NAMESPACE = "public";


    /**
     * Fills the catalog database with default data, skips if data is already inserted
     */
    public static void resetData( DdlManager ddlManager, RunMode mode ) {
        final Catalog catalog = Catalog.getInstance();
        restoreUsers( catalog );

        //////////////
        // init schema

        if ( catalog.getSnapshot().getNamespace( DEFAULT_NAMESPACE ).isEmpty() ) {
            catalog.createNamespace( "public", DataModel.getDefault(), false );
        }

        //////////////
        // init adapters

        restoreAdapters( ddlManager, catalog, mode );

        catalog.commit();

    }


    private static void restoreAdapters( DdlManager ddlManager, Catalog catalog, RunMode mode ) {
        if ( !catalog.getAdapters().isEmpty() ) {
            catalog.commit();
            return;
        }

        catalog.updateSnapshot();

        // Deploy default store (HSQLDB)
        Map<String, String> defaultStore = Catalog.snapshot().getAdapterTemplate( Catalog.defaultStore.getAdapterName(), AdapterType.STORE ).orElseThrow().getDefaultSettings();
        ddlManager.createStore( "hsqldb", Catalog.defaultStore.getAdapterName(), AdapterType.STORE, defaultStore, DeployMode.EMBEDDED );

        if ( mode == RunMode.TEST ) {
            return; // source adapters create schema structure, which we do not want for testing
        }

        // Deploy default source (CSV with HR data)
        Map<String, String> defaultSource = Catalog.snapshot().getAdapterTemplate( Catalog.defaultSource.getAdapterName(), AdapterType.SOURCE ).orElseThrow().getDefaultSettings();
        ddlManager.createSource( "hr", Catalog.defaultSource.getAdapterName(), Catalog.defaultNamespaceId, AdapterType.SOURCE, defaultSource, DeployMode.REMOTE );


    }


    private static void restoreUsers( Catalog catalog ) {
        //////////////
        // init users
        long systemId = catalog.createUser( "system", "" );

        catalog.createUser( "pa", "" );

        Catalog.defaultUserId = systemId;
    }


    public static void restoreInterfacesIfNecessary() {
        ////////////////////////
        // init query interfaces
        if ( !Catalog.getInstance().getInterfaces().isEmpty() ) {
            return;
        }
        Catalog.getInstance().getInterfaceTemplates().values().forEach( i -> Catalog.getInstance().createQueryInterface( i.interfaceName, i.clazz.getName(), i.defaultSettings ) );
        Catalog.getInstance().commit();

    }


    public static void restoreAvatica() {
        if ( Catalog.snapshot().getQueryInterface( "avatica" ).isPresent() ) {
            return;
        }
        QueryInterfaceTemplate avatica = Catalog.snapshot().getInterfaceTemplate( "AvaticaInterface" ).orElseThrow();
        Catalog.getInstance().createQueryInterface( "avatica", avatica.clazz.getName(), avatica.defaultSettings );
    }

}

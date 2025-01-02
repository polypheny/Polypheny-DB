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
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

@Deterministic
public class DefaultInserter {


    public static final String DEFAULT_NAMESPACE = "public";


    /**
     * Fills the catalog database with default data, skips if data is already inserted
     */
    public static void resetData( Transaction transaction, DdlManager ddlManager, RunMode mode ) {
        final Catalog catalog = Catalog.getInstance();
        restoreUsers( catalog );

        //////////////
        // init schema

        if ( catalog.getSnapshot().getNamespace( DEFAULT_NAMESPACE ).isEmpty() ) {
            catalog.createNamespace( "public", DataModel.getDefault(), false, false );
        }

        //////////////
        // init adapters

        restoreAdapters( transaction, ddlManager, catalog, mode );

        transaction.commit();
    }


    private static void restoreAdapters( Transaction transaction, DdlManager ddlManager, Catalog catalog, RunMode mode ) {
        if ( !catalog.getAdapters().isEmpty() ) {
            catalog.commit();
            return;
        }

        catalog.updateSnapshot();

        // Deploy default store (HSQLDB)
        AdapterTemplate storeTemplate = Catalog.snapshot().getAdapterTemplate( Catalog.defaultStore.getAdapterName(), AdapterType.STORE ).orElseThrow();
        ddlManager.createStore( "hsqldb", Catalog.defaultStore.getAdapterName(), AdapterType.STORE, storeTemplate.getDefaultSettings(), storeTemplate.getDefaultMode() );

        if ( mode == RunMode.TEST ) {
            return; // source adapters create schema structure, which we do not want for testing
        }

        // Deploy default source (CSV with HR data)
        AdapterTemplate sourceTemplate = Catalog.snapshot().getAdapterTemplate( Catalog.defaultSource.getAdapterName(), AdapterType.SOURCE ).orElseThrow();
        ddlManager.createSource( transaction, "hr", Catalog.defaultSource.getAdapterName(), Catalog.defaultNamespaceId, AdapterType.SOURCE, sourceTemplate.getDefaultSettings(), sourceTemplate.getDefaultMode() );
    }


    private static void restoreUsers( Catalog catalog ) {
        if ( catalog.getUsers().values().stream().anyMatch( u -> u.getName().equals( "system" ) ) ) {
            catalog.commit();
            return;
        }

        //////////////
        // init users
        long systemId = catalog.createUser( "system", "" );

        catalog.createUser( "pa", "" );

        Catalog.defaultUserId = systemId;
    }


    public static void restoreInterfacesIfNecessary( Catalog catalog ) {
        ////////////////////////
        // init query interfaces
        if ( !catalog.getInterfaces().isEmpty() ) {
            return;
        }
        catalog.getInterfaceTemplates().values().forEach( i -> Catalog.getInstance().createQueryInterface( i.interfaceType().toLowerCase(), i.interfaceType(), i.getDefaultSettings() ) );
        // TODO: This is ugly, both because it is racy, and depends on a string (which might be changed)
        if ( catalog.getInterfaceTemplates().values().stream().anyMatch( t -> t.interfaceType().equals( "Prism Interface (Unix transport)" ) ) ) {
            catalog.createQueryInterface(
                    "prism interface (unix transport @ .polypheny)",
                    "Prism Interface (Unix transport)",
                    Map.of( "path", PolyphenyHomeDirManager.getInstance().registerNewGlobalFile( "polypheny-prism.sock" ).getAbsolutePath() )
            );
        }
        catalog.commit();

    }

}

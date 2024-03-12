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

package org.polypheny.db.hsqldb.stores;

import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.sql.language.SqlDialectRegistry;

@SuppressWarnings("unused")
public class HsqldbPlugin extends PolyPlugin {


    public static final String ADAPTER_NAME = "HSQLDB";
    private long id;


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public HsqldbPlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void afterCatalogInit() {
        SqlDialectRegistry.registerDialect( "HSQLDB", HsqldbSqlDialect.DEFAULT );
        this.id = AdapterManager.addAdapterTemplate( HsqldbStore.class, ADAPTER_NAME, HsqldbStore::new );
    }


    @Override
    public void stop() {
        SqlDialectRegistry.unregisterDialect( "HSQLDB" );
        AdapterManager.removeAdapterTemplate( this.id );
    }

}

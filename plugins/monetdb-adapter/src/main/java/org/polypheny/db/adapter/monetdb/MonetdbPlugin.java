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

package org.polypheny.db.adapter.monetdb;

import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.monetdb.sources.MonetdbSource;
import org.polypheny.db.adapter.monetdb.stores.MonetdbStore;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.sql.language.SqlDialectRegistry;

@SuppressWarnings("unused")
public class MonetdbPlugin extends PolyPlugin {


    public static final String ADAPTER_NAME = "MonetDB";
    private long storeTemplateId;
    private long sourceTemplateId;


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to
     * be successfully loaded by manager.
     */
    public MonetdbPlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void afterCatalogInit() {
        SqlDialectRegistry.registerDialect( "MonetDB", MonetdbSqlDialect.DEFAULT );
        this.storeTemplateId = AdapterManager.addAdapterTemplate( MonetdbStore.class, ADAPTER_NAME, MonetdbStore::new );
        this.sourceTemplateId = AdapterManager.addAdapterTemplate( MonetdbSource.class, ADAPTER_NAME, MonetdbSource::new );
    }


    @Override
    public void stop() {
        SqlDialectRegistry.unregisterDialect( "MonetDB" );
        AdapterManager.removeAdapterTemplate( storeTemplateId );
        AdapterManager.removeAdapterTemplate( sourceTemplateId );
    }

}

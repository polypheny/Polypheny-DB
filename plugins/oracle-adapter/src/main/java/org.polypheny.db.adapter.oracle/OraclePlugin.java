/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.adapter.oracle;


import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.oracle.source.OracleSource;
import org.polypheny.db.adapter.oracle.store.OracleStore;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.sql.language.SqlDialectRegistry;

public class OraclePlugin extends PolyPlugin {


    public static final String ADAPTER_NAME = "Oracle";
    private long sourceId;
    private long storeId;


    public OraclePlugin( PluginContext context ) { super( context ); }


    @Override
    public void afterCatalogInit() {
        // SqlDialectRegistry.registerDialect( "Oracle", OracleSqlDialect.DEFAULT ); // TODO: Dialect might not be necessary.
        this.sourceId = AdapterManager.addAdapterTemplate( OracleSource.class, ADAPTER_NAME, OracleSource::new );
        this.storeId = AdapterManager.addAdapterTemplate( OracleStore.class, ADAPTER_NAME, OracleStore::new );
    }


    @Override
    public void stop() {
        SqlDialectRegistry.unregisterDialect( "Oracle" ); // TODO: if dialect is not necessary, unregistering dialect is redundant.
        AdapterManager.removeAdapterTemplate( this.sourceId );
        AdapterManager.removeAdapterTemplate( this.storeId );
    }
}

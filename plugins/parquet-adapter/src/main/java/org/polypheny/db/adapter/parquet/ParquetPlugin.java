/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet;

import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;

/**
 * Plugin entry point for the Parquet adapter.
 */
public class ParquetPlugin extends PolyPlugin {

    private long id;

    /**
     * Constructor
     * Create Plugin Instance
     */
    public ParquetPlugin( PluginContext context ) {
        super( context );
    }

    /**
     * Registers the adapter template once the catalog is ready.
     */
    @Override
    public void afterCatalogInit() {
        this.id = AdapterManager.addAdapterTemplate( ParquetSource.class, "Parquet", ParquetSource::new );
    }

    /**
     * Removes the adapter template on shutdown.
     */
    @Override
    public void stop() {
        AdapterManager.removeAdapterTemplate( id );
    }

}

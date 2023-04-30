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

package org.polypheny.db.hsqldb.stores;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;

public class HsqldbPlugin extends PolyPlugin {


    public static final String ADAPTER_NAME = "HSQLDB";


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public HsqldbPlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void start() {
        Map<String, String> settings = ImmutableMap.copyOf( Map.of(
                "type", "Memory",
                "mode", "embedded",
                "tableType", "Memory",
                "maxConnections", "25",
                "trxControlMode", "mvcc",
                "trxIsolationLevel", "read_committed"
        ) );

        AdapterManager.addAdapterTemplate( HsqldbStore.class, ADAPTER_NAME, settings );
    }


    @Override
    public void stop() {
        AdapterManager.removeAdapterTemplate( HsqldbStore.class, ADAPTER_NAME );
    }

}

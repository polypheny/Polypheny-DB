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

package org.polypheny.db.adapter.postgres;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.adapter.postgres.source.PostgresqlSource;
import org.polypheny.db.adapter.postgres.store.PostgresqlStore;
import org.polypheny.db.catalog.Adapter;

public class PostgresqlPlugin extends Plugin {


    public static final String ADAPTER_NAME = "POSTGRESQL";


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to
     * be successfully loaded by manager.
     *
     * @param wrapper
     */
    public PostgresqlPlugin( PluginWrapper wrapper ) {
        super( wrapper );
    }


    @Override
    public void start() {
        Map<String, String> settings = ImmutableMap.of(
                "mode", "docker",
                "password", "polypheny",
                "instanceId", "0",
                "port", "3306",
                "maxConnections", "25"
        );

        Adapter.addAdapter( PostgresqlStore.class, ADAPTER_NAME, settings );
        Adapter.addAdapter( PostgresqlSource.class, ADAPTER_NAME, settings );
    }


    @Override
    public void stop() {
        Adapter.removeAdapter( PostgresqlStore.class, ADAPTER_NAME );
        Adapter.removeAdapter( PostgresqlSource.class, ADAPTER_NAME );
    }

}

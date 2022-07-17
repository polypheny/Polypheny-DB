/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adaptimizer.rndschema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adaptimizer.exceptions.RandomSchemaException;

public class SchemaUtil {
    public static final String HSQLDB_ADAPTER_NAME = "adapt_hsqldb_adapter";
    public static final String POSTGRE_ADAPTER_NAME = "adapt_postgre_adapter";

    private static AdapterManager adapterManager;
    private static List<DataStore> dataStores;

    public static String addRndSchemaTestDataStores() {
        adapterManager = AdapterManager.getInstance();
        dataStores = List.of( addHsqldbAdapter() );
        // dataStores.add( addPostgreSQLAdapter() );
        return "Success.";
    }

    public static String generateSchema( HashMap<String, String> parameters ) {
        if ( dataStores == null ) {
            throw new RandomSchemaException( "No data-stores provided...", new NullPointerException() );
        }
        SchemaTemplate template = SchemaTemplate.builder()
                .random( new Random( Long.parseLong( parameters.get( "seed" ) ) ) )
                .referenceProbability( Float.parseFloat( parameters.get( "refP" ) ) )
                .meanTables( 7 )
                .sigmaTables( 1.5 )
                .meanAttributes( 5 )
                .sigmaAttributes( 1.5 )
                .meanReferences( 7 )
                .sigmaReferences( 1.5 )
                .build();
        new RandomSchemaGenerator( template, dataStores ).generate();
        return "Success";
    }

    public static DataStore addHsqldbAdapter() {
        Map<String, String> settings = new HashMap<>();
        settings.put( "type", "Memory" );
        settings.put( "tableType", "Memory" );
        settings.put( "path", "maxConnections" );
        settings.put( "maxConnections", "25" );
        settings.put( "trxControlMode", "mvcc" );
        settings.put( "trxIsolationLevel", "read_committed" );
        settings.put( "mode", "embedded" );
        return ( DataStore ) adapterManager.addAdapter(
                "org.polypheny.db.adapter.jdbc.stores.HsqldbStore", HSQLDB_ADAPTER_NAME, settings
        );
    }

    public static DataStore addPostgreSQLAdapter() {
        Map<String, String> settings = new HashMap<>();
        settings.put( "database", "postgres" );
        settings.put( "mode", "docker" );
        settings.put( "host", "localhost" );
        settings.put( "maxConnections", "25" );
        settings.put( "password", "polypheny" );
        settings.put( "username", "postgres" );
        settings.put( "port", "5432" );
        settings.put( "instanceId", "0");
        settings.put( "transactionIsolation", "SERIALIZABLE" );
        return ( DataStore ) adapterManager.addAdapter(
                "org.polypheny.db.adapter.jdbc.stores.PostgresqlStore", POSTGRE_ADAPTER_NAME, settings
        );
    }

}

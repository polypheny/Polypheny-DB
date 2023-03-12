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

package org.polypheny.db.polyfier.schemas;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Adapter;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.polyfier.core.PolyfierException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@Slf4j
public class SchemaUtil {
    private static final String MODULE_ADAPTER_PREFIX = "ADAPT_";
    private static final String MODULE_CONFIG_PREFIX = "CONFIG_ADAPT_";

    private static final AdapterManager adapterManager;
    private static List<DataStore> dataStores;

    static {
        adapterManager = AdapterManager.getInstance();
    }

    public static String addRndSchemaTestDataStores() {
        dataStores = List.of( addDataStore( Adapter.fromString( "HSQLDB", CatalogAdapter.AdapterType.STORE ) ) );
        return "Success.";
    }

    public static String generateSchema( HashMap<String, String> parameters ) {
        if (dataStores == null) {
            throw new PolyfierException("No data-stores provided...", new NullPointerException());
        }
        SchemaTemplate template = SchemaTemplate.builder()
                .random(new Random(Long.parseLong(parameters.get("seed"))))
                .referenceProbability(Float.parseFloat(parameters.get("refP")))
                .meanTables(7)
                .sigmaTables(1.5)
                .meanAttributes(5)
                .sigmaAttributes(1.5)
                .meanReferences(7)
                .sigmaReferences(1.5)
                .build();
        new RandomSchemaGenerator(template, dataStores).generate();
        return "Success";
    }

    public static DataStore addDataStore( @NonNull Adapter adapter ) {
        Map<String, String> settings = adapter.getDefaultSettings();
        Map<String, String> isThisNecessaryQuestionMark = new HashMap<>( settings );
        if ( adapter.getAdapterName().equals("MONETDB") ) {
            // Some port issue when going for 50k
            isThisNecessaryQuestionMark.put("port", "50001");
        }

        try {
            return (DataStore) adapterManager.addAdapter(
                    adapter.getAdapterName(), MODULE_ADAPTER_PREFIX + adapter.getAdapterName(), adapter.getAdapterType(), isThisNecessaryQuestionMark
            );

        } catch ( RuntimeException runtimeException ) {
            throw new RuntimeException( "RuntimeException adding Adapter " + MODULE_ADAPTER_PREFIX + adapter.getAdapterName() + "..", runtimeException );
        }
    }

    public static DataStore addConfigDatastore( Adapter adapter ) {
        Map<String, String> settings = adapter.getDefaultSettings();
        return (DataStore) adapterManager.addAdapter(
                adapter.getAdapterName(), MODULE_CONFIG_PREFIX + adapter.getAdapterName(), adapter.getAdapterType(), settings
        );
    }

}
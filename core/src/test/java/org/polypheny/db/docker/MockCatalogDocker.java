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

package org.polypheny.db.docker;


import java.util.HashMap;
import java.util.Map;
import org.polypheny.db.catalog.MockCatalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;

/**
 * This is a bare-bone catalog which allows to mock register adapters
 * which then can be retrieved while testing adapters which use Docker
 */
public class MockCatalogDocker extends MockCatalog {

    int i = 0;
    HashMap<Integer, CatalogAdapter> adapters = new HashMap<>();


    @Override
    public int addAdapter( String uniqueName, String clazz, AdapterType type, Map<String, String> settings ) {
        i++;
        adapters.put( i, new CatalogAdapter( i, uniqueName, clazz, type, settings ) );
        return i;
    }


    @Override
    public boolean checkIfExistsAdapter( int adapterId ) {
        return adapters.containsKey( adapterId );
    }


    @Override
    public CatalogAdapter getAdapter( int adapterId ) {
        return adapters.get( adapterId );
    }

}

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

package org.polypheny.db.adapter.java;

import java.util.Map;
import lombok.Getter;
import lombok.Value;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;

@Value
public class AdapterTemplate {

    @Getter
    public Map<String, String> defaultSettings;

    public Class<?> clazz;
    public String adapterName;
    public AdapterType adapterType;


    public AdapterTemplate( Class<?> clazz, String adapterName, Map<String, String> defaultSettings ) {
        this.adapterName = adapterName;
        this.clazz = clazz;
        this.defaultSettings = defaultSettings;
        this.adapterType = getAdapterType( clazz );
    }


    public static AdapterType getAdapterType( Class<?> clazz ) {
        return DataStore.class.isAssignableFrom( clazz ) ? AdapterType.STORE : AdapterType.SOURCE;
    }


    public static AdapterTemplate fromString( String adapterName, AdapterType adapterType ) {
        return AdapterManager.getAdapterType( adapterName.toUpperCase().split( "-DB" )[0] + "_" + adapterType );// todo dl fix on UI layer
    }


}

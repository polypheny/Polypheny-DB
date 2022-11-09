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

package org.polypheny.db.catalog;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;

public class Adapter {

    public static Map<String, Adapter> REGISTER;


    static {
        REGISTER = new ConcurrentHashMap<>();
        System.out.println( "init" );
    }


    @Getter
    private final Map<String, String> defaultSettings;

    @Getter
    private final Class<?> clazz;
    @Getter
    private final String adapterName;
    @Getter
    private final AdapterType adapterType;


    public Adapter( Class<?> clazz, String adapterName, Map<String, String> defaultSettings ) {
        this.adapterName = adapterName;
        this.clazz = clazz;
        this.defaultSettings = defaultSettings;
        this.adapterType = getAdapterType( clazz );
    }


    private static AdapterType getAdapterType( Class<?> clazz ) {
        return DataStore.class.isAssignableFrom( clazz ) ? AdapterType.STORE : AdapterType.SOURCE;
    }


    public static Adapter fromString( String adapterName, AdapterType adapterType ) {
        return REGISTER.get( adapterName.toUpperCase() + "_" + adapterType );
    }


    public static void addAdapter( Class<?> clazz, String adapterName, Map<String, String> defaultSettings ) {
        REGISTER.put( getKey( clazz, adapterName ), new Adapter( clazz, adapterName.toUpperCase(), defaultSettings ) );
    }


    private static String getKey( Class<?> clazz, String adapterName ) {
        return adapterName.toUpperCase() + "_" + getAdapterType( clazz );
    }


    public static List<Adapter> getAdapters( AdapterType adapterType ) {
        return REGISTER.values().stream().filter( a -> a.adapterType == adapterType ).collect( Collectors.toList() );
    }

}

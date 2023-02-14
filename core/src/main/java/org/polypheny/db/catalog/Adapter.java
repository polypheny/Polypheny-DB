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

package org.polypheny.db.catalog;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;

public class Adapter {

    private static final Map<String, Adapter> REGISTER = new ConcurrentHashMap<>();


    @Getter
    private final Map<String, String> defaultSettings;

    @Getter
    private final Class<?> clazz;
    @Getter
    private final String adapterName;
    @Getter
    private final AdapterType adapterType;
    @Getter
    private final Consumer<Map<String, String>> preEvaluation;


    public Adapter( Class<?> clazz, String adapterName, Map<String, String> defaultSettings, @Nullable Consumer<Map<String, String>> preEvaluation ) {
        this.adapterName = adapterName;
        this.clazz = clazz;
        this.defaultSettings = defaultSettings;
        this.adapterType = getAdapterType( clazz );
        this.preEvaluation = preEvaluation;
    }


    public Adapter( Class<?> clazz, String adapterName, Map<String, String> defaultSetting ) {
        this( clazz, adapterName, defaultSetting, null );
    }


    private static AdapterType getAdapterType( Class<?> clazz ) {
        return DataStore.class.isAssignableFrom( clazz ) ? AdapterType.STORE : AdapterType.SOURCE;
    }


    public static Adapter fromString( String adapterName, AdapterType adapterType ) {
        return REGISTER.get( adapterName.toUpperCase().split( "-DB" )[0] + "_" + adapterType );// todo dl fix on UI layer
    }


    public static void addAdapter( Class<?> clazz, String adapterName, Map<String, String> defaultSettings ) {
        REGISTER.put( getKey( clazz, adapterName ), new Adapter( clazz, adapterName.toUpperCase(), defaultSettings ) );
    }


    public static void removeAdapter( Class<?> clazz, String adapterName ) {
        if ( Catalog.getInstance().getAdapters().stream().anyMatch( a -> a.getAdapterTypeName().equals( adapterName ) ) ) {
            throw new RuntimeException( "Adapter is still deployed!" );
        }
        REGISTER.remove( getKey( clazz, adapterName ) );
    }


    private static String getKey( Class<?> clazz, String adapterName ) {
        return adapterName.toUpperCase() + "_" + getAdapterType( clazz );
    }


    public static List<Adapter> getAdapters( AdapterType adapterType ) {
        return REGISTER.values().stream().filter( a -> a.adapterType == adapterType ).collect( Collectors.toList() );
    }

}

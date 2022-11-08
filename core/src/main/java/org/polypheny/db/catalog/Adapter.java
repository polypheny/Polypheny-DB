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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import lombok.Getter;

public class Adapter {

    private static final Map<String, Adapter> register = new HashMap<>();
    @Getter
    private final Map<String, String> defaultSettings;

    @Getter
    private final Class<?> clazz;
    @Getter
    private final String adapterName;


    public Adapter( Class<?> clazz, String adapterName, Map<String, String> defaultSettings ) {
        this.adapterName = adapterName;
        this.clazz = clazz;
        this.defaultSettings = defaultSettings;
    }


    public static Adapter fromString( String adapterName ) {
        return register.get( adapterName.toUpperCase( Locale.ROOT ) );
    }


    public static void addAdapter( Class<?> clazz, String adapterName, Map<String, String> defaultSettings ) {
        register.put( adapterName.toUpperCase(), new Adapter( clazz, adapterName.toUpperCase(), defaultSettings ) );
    }

}

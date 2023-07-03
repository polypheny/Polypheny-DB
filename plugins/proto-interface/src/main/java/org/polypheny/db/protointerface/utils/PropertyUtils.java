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

package org.polypheny.db.protointerface.utils;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class PropertyUtils {

    public static final String USERNAME_KEY = "user";
    public static final String PASSWORD_KEY = "password";
    public static final String AUTOCOMMIT_KEY = "autocommit";
    public static final String FETCH_SIZE_KEY = "fetchSize";

    private static final Map<String, String> DEFAULT_VALUES =
            ImmutableMap.<String, String>builder()
                    .put( AUTOCOMMIT_KEY, "true")
                    .put( FETCH_SIZE_KEY, "100")
                    .build();


    public static String getDefaultOf( String propertyKey ) throws IllegalArgumentException{
        String defaultValue = DEFAULT_VALUES.get( propertyKey );
        if ( defaultValue == null ) {
            throw new IllegalArgumentException( "Unknown key" );
        }
        return defaultValue;
    }

}

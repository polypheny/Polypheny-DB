/*
 * Copyright 2019-2021 The Polypheny Project
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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import lombok.Getter;

public enum ADAPTER {
    MONGODB( "org.polypheny.db.adapter.mongodb.MongoStore" ),
    HSQLDB( "org.polypheny.db.adapter.jdbc.stores.HsqldbStore" ),
    CSV( "org.polypheny.db.adapter.csv.CsvSource" );

    @Getter
    private final String path;
    @Getter
    private final Class<?> clazz;


    ADAPTER( String path ) {
        this.path = path;
        try {
            this.clazz = Class.forName( path );
        } catch ( ClassNotFoundException e ) {
            throw new RuntimeException( "The supplied store name was not recognized." );
        }
    }


    public static EnumSet<ADAPTER> store = EnumSet.of( MONGODB, HSQLDB );


    public static ADAPTER fromString( String defaultStore ) {
        return ADAPTER.valueOf( defaultStore.toUpperCase( Locale.ROOT ) );
    }


    public Map<String, String> getDefaultSettings() {
        Map<String, String> settings = new HashMap<>();
        switch ( this ) {

            case MONGODB:
                settings.put( "persistent", "true" );
                settings.put( "port", "27017" );
                settings.put( "type", "mongo" );
                settings.put( "instanceId", "0" );
                settings.put( "mode", "docker" );
                break;
            case HSQLDB:
                settings.put( "type", "Memory" );
                settings.put( "mode", "embedded" );
                settings.put( "tableType", "Memory" );
                settings.put( "maxConnections", "25" );
                settings.put( "trxControlMode", "mvcc" );
                settings.put( "trxIsolationLevel", "read_committed" );
                break;
            case CSV:
                settings.put( "mode", "embedded" );
                settings.put( "directory", "classpath://hr" );
                settings.put( "maxStringLength", "255" );
                break;
        }

        return settings;
    }

}

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

public enum Adapter {

    MONGODB( "org.polypheny.db.adapter.mongodb.MongoStore" ),
    HSQLDB( "org.polypheny.db.adapter.jdbc.stores.HsqldbStore" ),
    CSV( "org.polypheny.db.adapter.csv.CsvSource" ),
    CASSANDRA( "org.polypheny.db.adapter.cassandra.CassandraStore" ),
    MONETDB( "org.polypheny.db.adapter.jdbc.stores.MonetdbStore" ),
    COTTONTAIL( "org.polypheny.db.adapter.cottontail.CottontailStore" ),
    POSTGRESQL( "org.polypheny.db.adapter.jdbc.stores.PostgresqlStore" ),
    FILE( "org.polypheny.db.adapter.file.FileStore" );

    @Getter
    private final String path;
    @Getter
    private final Class<?> clazz;


    Adapter( String path ) {
        this.path = path;
        try {
            this.clazz = Class.forName( path );
        } catch ( ClassNotFoundException e ) {
            throw new RuntimeException( "The supplied store name was not recognized: " + path );
        }
    }


    public static Adapter fromString( String storeName ) {
        return Adapter.valueOf( storeName.toUpperCase( Locale.ROOT ) );
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
                settings.put( "trxLifetimeLimit", "1209600" );
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
            case CASSANDRA:
                settings.put( "mode", "docker" );
                settings.put( "instanceId", "0" );
                settings.put( "port", "9042" );
                break;
            case MONETDB:
                settings.put( "mode", "docker" );
                settings.put( "instanceId", "0" );
                settings.put( "password", "polypheny" );
                settings.put( "maxConnections", "25" );
                settings.put( "port", "5000" );
                break;
            case COTTONTAIL:
                settings.put( "mode", "embedded" );
                settings.put( "database", "cottontail" );
                settings.put( "port", "1865" );
                settings.put( "engine", "MAPDB" );
                settings.put( "host", "localhost" );
                break;
            case POSTGRESQL:
                settings.put( "mode", "docker" );
                settings.put( "password", "polypheny" );
                settings.put( "instanceId", "0" );
                settings.put( "port", "3306" );
                settings.put( "maxConnections", "25" );
                break;
            case FILE:
                settings.put( "mode", "embedded" );
                break;
        }

        return settings;
    }

}

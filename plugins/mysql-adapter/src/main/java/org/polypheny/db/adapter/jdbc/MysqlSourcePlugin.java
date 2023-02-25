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

package org.polypheny.db.adapter.jdbc;


import com.google.common.collect.ImmutableMap;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.Adapter.AdapterSettingList;
import org.polypheny.db.adapter.Adapter.AdapterSettingString;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.jdbc.sources.AbstractJdbcSource;
import org.polypheny.db.catalog.Adapter;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.sql.language.dialect.MysqlSqlDialect;

public class MysqlSourcePlugin extends Plugin {


    public static final String ADAPTER_NAME = "MYSQL";


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public MysqlSourcePlugin( PluginWrapper wrapper ) {
        super( wrapper );
    }


    @Override
    public void start() {
        Map<String, String> settings = ImmutableMap.of();

        Adapter.addAdapter( MysqlSource.class, ADAPTER_NAME, settings );
    }


    @Override
    public void stop() {
        Adapter.removeAdapter( MysqlSource.class, ADAPTER_NAME );
    }


    @Slf4j
    @AdapterProperties(
            name = "MySQL",
            description = "Data source adapter for the relational database systems MariaDB and MySQL.",
            usedModes = DeployMode.REMOTE)
    @AdapterSettingString(name = "host", defaultValue = "localhost", position = 1,
            description = "Hostname or IP address of the remote MariaDB / MySQL instance.")
    @AdapterSettingInteger(name = "port", defaultValue = 3306, position = 2,
            description = "JDBC port number on the remote MariaDB / MySQL instance.")
    @AdapterSettingString(name = "database", defaultValue = "polypheny", position = 3,
            description = "Name of the database to connect to.")
    @AdapterSettingString(name = "username", defaultValue = "polypheny", position = 4,
            description = "Username to be used for authenticating at the remote instance.")
    @AdapterSettingString(name = "password", defaultValue = "polypheny", position = 5,
            description = "Password to be used for authenticating at the remote instance.")
    @AdapterSettingInteger(name = "maxConnections", defaultValue = 25,
            description = "Maximum number of concurrent JDBC connections.")
    @AdapterSettingList(name = "transactionIsolation", options = { "SERIALIZABLE", "READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ" }, defaultValue = "SERIALIZABLE",
            description = "Which level of transaction isolation should be used.")
    @AdapterSettingString(name = "tables", defaultValue = "foo,bar",
            description = "List of tables which should be imported. The names must to be separated by a comma.")
    public static class MysqlSource extends AbstractJdbcSource {

        public MysqlSource( int storeId, String uniqueName, final Map<String, String> settings ) {
            super( storeId, uniqueName, settings, "org.mariadb.jdbc.Driver", MysqlSqlDialect.DEFAULT, false );
        }


        @Override
        public PhysicalTable createTableSchema( PhysicalTable boilerplate ) {
            return currentJdbcSchema.createJdbcTable( catalogTable, columnPlacementsOnStore, partitionPlacement );
        }


        @Override
        public Namespace getCurrentSchema() {
            return currentJdbcSchema;
        }


        @Override
        public void shutdown() {
            try {
                removeInformationPage();
                connectionFactory.close();
            } catch ( SQLException e ) {
                log.warn( "Exception while shutting down {}", getUniqueName(), e );
            }
        }


        @Override
        protected void reloadSettings( List<String> updatedSettings ) {
            // TODO: Implement disconnect and reconnect to PostgreSQL instance.
        }


        @Override
        protected String getConnectionUrl( final String dbHostname, final int dbPort, final String dbName ) {
            return String.format( "jdbc:mysql://%s:%d/%s", dbHostname, dbPort, dbName );
        }


        @Override
        protected boolean requiresSchema() {
            return false;
        }

    }

}
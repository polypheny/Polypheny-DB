/*
 * Copyright 2019-2024 The Polypheny Project
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


import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.jdbc.sources.AbstractJdbcSource;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.language.dialect.MysqlSqlDialect;

@SuppressWarnings("unused")
public class MysqlSourcePlugin extends PolyPlugin {


    public static final String ADAPTER_NAME = "MySQL";
    private long id;


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public MysqlSourcePlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void afterCatalogInit() {
        this.id = AdapterManager.addAdapterTemplate( MysqlSource.class, ADAPTER_NAME, MysqlSource::new );
    }


    @Override
    public void stop() {
        AdapterManager.removeAdapterTemplate( this.id );
    }


    @Slf4j
    @AdapterProperties(
            name = "MySQL",
            description = "Data source adapter for the relational database systems MariaDB and MySQL.",
            usedModes = DeployMode.REMOTE,
            defaultMode = DeployMode.REMOTE)
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

        public MysqlSource( final long storeId, final String uniqueName, final Map<String, String> settings ) {
            super( storeId, uniqueName, settings, "org.mariadb.jdbc.Driver", MysqlSqlDialect.DEFAULT, false );
        }


        @Override
        public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
            PhysicalTable table = adapterCatalog.createTable(
                    logical.table.getNamespaceName(),
                    logical.table.name,
                    logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c.name ) ),
                    logical.table,
                    logical.columns.stream().collect( Collectors.toMap( t -> t.id, t -> t ) ),
                    logical.pkIds, allocation );

            JdbcTable physical = currentJdbcSchema.createJdbcTable( table );

            adapterCatalog.replacePhysical( physical );

            return List.of( physical );
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
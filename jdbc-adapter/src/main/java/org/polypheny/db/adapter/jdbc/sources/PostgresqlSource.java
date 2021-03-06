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

package org.polypheny.db.adapter.jdbc.sources;


import com.google.common.collect.ImmutableList;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.dialect.PostgresqlSqlDialect;


@Slf4j
public class PostgresqlSource extends AbstractJdbcSource {

    public static final String ADAPTER_NAME = "PostgreSQL";

    public static final String DESCRIPTION = "Relational database system optimized for transactional workload that provides an advanced set of features. PostgreSQL is fully ACID compliant and ensures that all requirements are met.";

    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingString( "host", false, true, false, "localhost" )
                    .setDescription( "Hostname or IP address of the remote PostgreSQL instance." ),
            new AdapterSettingInteger( "port", false, true, false, 5432 )
                    .setDescription( "JDBC port number on the remote PostgreSQL instance." ),
            new AdapterSettingString( "database", false, true, false, "postgres" )
                    .setDescription( "Name of the database to connect to." ),
            new AdapterSettingString( "username", false, true, false, "postgres" )
                    .setDescription( "Username to be used for authenticating at the remote instance" ),
            new AdapterSettingString( "password", false, true, false, "polypheny" )
                    .setDescription( "Password to be used for authenticating at the remote instance" ),
            new AdapterSettingInteger( "maxConnections", false, true, false, 25 )
                    .setDescription( "Maximum number of concurrent JDBC connections." ),
            new AdapterSettingList( "transactionIsolation", false, true, false, ImmutableList.of( "SERIALIZABLE", "READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ" ) )
                    .setDescription( "Which level of transaction isolation should be used." ),
            new AdapterSettingString( "tables", false, true, false, "public.foo,public.bar" )
                    .setDescription( "List of tables which should be imported. The names must to be separated by a comma." )
    );


    public PostgresqlSource( int storeId, String uniqueName, final Map<String, String> settings ) {
        super(
                storeId,
                uniqueName,
                settings,
                "org.postgresql.Driver",
                PostgresqlSqlDialect.DEFAULT,
                false );
    }


    @Override
    public Table createTableSchema( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        return currentJdbcSchema.createJdbcTable( catalogTable, columnPlacementsOnStore );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentJdbcSchema;
    }


    @Override
    public String getAdapterName() {
        return ADAPTER_NAME;
    }


    @Override
    public List<AdapterSetting> getAvailableSettings() {
        return AVAILABLE_SETTINGS;
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
        return String.format( "jdbc:postgresql://%s:%d/%s", dbHostname, dbPort, dbName );
    }


    @Override
    protected boolean requiresSchema() {
        return true;
    }

}

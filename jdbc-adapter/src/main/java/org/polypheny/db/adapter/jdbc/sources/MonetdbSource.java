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


import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.Adapter.AdapterSettingString;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RemoteDeployable;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.TransactionalConnectionFactory;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.SqlDialect;
import org.polypheny.db.sql.dialect.MonetdbSqlDialect;


@Slf4j
@AdapterSettingString(name = "hose", defaultValue = "localhost", appliesTo = DeployMode.DEFAULT,
        description = "Hostname or IP address of the remote MonetDB instance.")
@AdapterSettingInteger(name = "port", defaultValue = 50000, appliesTo = DeployMode.DEFAULT,
        description = "JDBC port number on the remote MonetDB instance.")
@AdapterSettingString(name = "database", defaultValue = "polypheny", appliesTo = DeployMode.DEFAULT,
        description = "JDBC port number on the remote MonetDB instance.")
@AdapterSettingString(name = "username", defaultValue = "polypheny", appliesTo = DeployMode.DEFAULT,
        description = "Name of the database to connect to.")
@AdapterSettingString(name = "password", defaultValue = "polypheny", appliesTo = DeployMode.DEFAULT,
        description = "Username to be used for authenticating at the remote instance")
@AdapterSettingInteger(name = "maxConnections", defaultValue = 25, appliesTo = DeployMode.DEFAULT,
        description = "Password to be used for authenticating at the remote instance")
@AdapterSettingString(name = "table", defaultValue = "public.foo,public.bar", appliesTo = DeployMode.DEFAULT,
        description = "Maximum number of concurrent JDBC connections.")
public class MonetdbSource extends AbstractJdbcSource implements RemoteDeployable {

    public static final String ADAPTER_NAME = "MonetDB";

    public static final String DESCRIPTION = "MonetDB is an open-source column-oriented database management system. It is based on an optimistic concurrency control.";


    public MonetdbSource( int storeId, String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, "nl.cwi.monetdb.jdbc.MonetDriver", MonetdbSqlDialect.DEFAULT, false );
    }


    @Override
    protected ConnectionFactory createConnectionFactory( final Map<String, String> settings, SqlDialect dialect, String driverClass ) {
        BasicDataSource dataSource = new BasicDataSource();
        final String connectionUrl;
        if ( settings.get( "host" ).equals( "running-embedded" ) ) { // For integration testing
            connectionUrl = "jdbc:monetdb:embedded::memory:";
        } else {
            dataSource.setDriverClassName( driverClass );
            connectionUrl = getConnectionUrl( settings.get( "host" ), Integer.parseInt( settings.get( "port" ) ), settings.get( "database" ) );
            dataSource.setUsername( settings.get( "username" ) );
            dataSource.setPassword( settings.get( "password" ) );
        }
        dataSource.setUrl( connectionUrl );
        if ( log.isDebugEnabled() ) {
            log.debug( "JDBC Connection URL: {}", connectionUrl );
        }
        dataSource.setDefaultAutoCommit( false );
        return new TransactionalConnectionFactory( dataSource, Integer.parseInt( settings.get( "maxConnections" ) ), dialect );
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
        return String.format( "jdbc:monetdb://%s:%d/%s", dbHostname, dbPort, dbName );
    }


    @Override
    protected boolean requiresSchema() {
        return true;
    }

}

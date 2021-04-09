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
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.Adapter.AdapterSettingList;
import org.polypheny.db.adapter.Adapter.AdapterSettingString;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RemoteDeployable;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.dialect.MysqlSqlDialect;


@Slf4j
@AdapterSettingString(name = "host", defaultValue = "localhost", appliesTo = DeployMode.DEFAULT,
        description = "Hostname or IP address of the remote MariaDB / MySQL instance.")
@AdapterSettingInteger(name = "port", defaultValue = 3306, appliesTo = DeployMode.DEFAULT,
        description = "JDBC port number on the remote MariaDB / MySQL instance.")
@AdapterSettingString(name = "database", defaultValue = "polypheny", appliesTo = DeployMode.DEFAULT,
        description = "Name of the database to connect to.")
@AdapterSettingString(name = "username", defaultValue = "polypheny", appliesTo = DeployMode.DEFAULT,
        description = "Username to be used for authenticating at the remote instance")
@AdapterSettingString(name = "password", defaultValue = "polypheny", appliesTo = DeployMode.DEFAULT,
        description = "Password to be used for authenticating at the remote instance")
@AdapterSettingInteger(name = "maxConnections", defaultValue = 25, appliesTo = DeployMode.DEFAULT,
        description = "Maximum number of concurrent JDBC connections.")
@AdapterSettingList(name = "transactionIsolation", options = { "SERIALIZABLE", "READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ" }, appliesTo = DeployMode.DEFAULT,
        description = "Which level of transaction isolation should be used.")
@AdapterSettingString(name = "tables", defaultValue = "foo,bar", appliesTo = DeployMode.DEFAULT,
        description = "List of tables which should be imported. The names must to be separated by a comma.")
public class MysqlSource extends AbstractJdbcSource implements RemoteDeployable {

    public static final String ADAPTER_NAME = "MySQL";

    public static final String DESCRIPTION = "Data source adapter for the relational database systems MariaDB and MySQL.";


    public MysqlSource( int storeId, String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, "org.mariadb.jdbc.Driver", MysqlSqlDialect.DEFAULT, false );
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
        return String.format( "jdbc:mysql://%s:%d/%s", dbHostname, dbPort, dbName );
    }


    @Override
    protected boolean requiresSchema() {
        return false;
    }

}

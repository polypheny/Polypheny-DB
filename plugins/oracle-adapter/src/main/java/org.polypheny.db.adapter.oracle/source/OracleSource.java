/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.adapter.oracle.source;


import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.jdbc.sources.AbstractJdbcSource;
import org.polypheny.db.adapter.oracle.OracleSqlDialect;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.language.SqlDialect;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
// TODO: Find out oracle credentials to log in and adjust URL if necessary.

@Slf4j
@AdapterProperties(
        name = "Oracle",
        description = "Data source explicit for relational oracle database systems.",
        usedModes = DeployMode.REMOTE,
        defaultMode = DeployMode.REMOTE)
@AdapterSettingString(name = "host", defaultValue = "localhost", position = 1, //TODO: Find out correct hostname, IP.
        description = "Hostname or IP address of the remote oracle instance.")
@AdapterSettingInteger(name = "port", defaultValue = 1521, position = 2, //TODO: Find out correct port number.
        description = "Port number of the remote oracle instance.")
@AdapterSettingString(name = "database", defaultValue = "polypheny", position = 3,
        description = "Name of the database to connect with.")
@AdapterSettingString(name = "username", defaultValue = "polypheny", position = 4,
        description = "Username used for authentication at the remote instance.")
@AdapterSettingString(name = "password", defaultValue = "polypheny", position = 5,
        description = "Password used for authentication at the remote instance.")
@AdapterSettingInteger(name = "maxConnections", defaultValue = 25,
        description = "Maximum number of concurrent connections.")
@AdapterSettingList(name = "transactionIsolation", options = { "SERIALIZABLE", "READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ" }, defaultValue = "SERIALIZABLE",
        description = "Which level of transaction isolation should be used.")
@AdapterSettingString(name = "tables", defaultValue = "foo,bar",
        description = "List of tables which should be imported. The names must be separated by a comma.")
public class OracleSource extends AbstractJdbcSource {

    public OracleSource( final long storeId, final String uniqueName, final Map<String, String> settings, final DeployMode mode ) {
        super(
                storeId,
                uniqueName,
                settings,
                mode,
                "oracle.jdbc.OracleDriver",
                OracleSqlDialect.DEFAULT,
                false );
    }


    @Override
    protected String getConnectionUrl( final String dbHostname, final int dbPort, final String dbName ) {
        return String.format( "jdbc:oracle:thin:@//%s:%d/%s", dbHostname, dbPort, dbName );
    }


    @Override
    protected boolean requiresSchema() {
        return false;
    }


    @Override
    public void shutdown() {
        try {
            removeInformationPage();
            connectionFactory.close();
        } catch ( SQLException e ) {
            log.warn( "Exception while closing oracle connection {}", getUniqueName(), e );
        }
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        //TODO: Implement disconnect and reconnect to Oracle instance.
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

        adapterCatalog.replacePhysical( currentJdbcSchema.createJdbcTable( table ) );
        return List.of( table );
    }
}

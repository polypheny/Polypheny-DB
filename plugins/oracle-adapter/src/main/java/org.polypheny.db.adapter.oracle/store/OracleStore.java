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

package org.polypheny.db.adapter.oracle.store;


import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.jdbc.sources.AbstractJdbcSource;
import org.polypheny.db.adapter.oracle.OracleSqlDialect;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.docker.DockerContainer;
import org.polypheny.db.prepare.Context;
import java.util.List;
import java.util.Map;

@Slf4j
@AdapterProperties(
        name = "Oracle",
        description = "Data source explicit for relational oracle database systems.",
        usedModes = { DeployMode.REMOTE, DeployMode.DOCKER },
        defaultMode = DeployMode.DOCKER)
@AdapterSettingString(name = "host", defaultValue = "localhost", position = 1,
        description = "Hostname or IP address of the remote PostgreSQL instance.", appliesTo = DeploySetting.REMOTE)
@AdapterSettingInteger(name = "port", defaultValue = 1521, position = 2,
        description = "JDBC port number on the remote PostgreSQL instance.", appliesTo = DeploySetting.REMOTE)
@AdapterSettingString(name = "database", defaultValue = "polypheny", position = 3,
        description = "Name of the database to connect to.", appliesTo = DeploySetting.REMOTE)
@AdapterSettingString(name = "username", defaultValue = "polypheny", position = 4,
        description = "Username to be used for authenticating at the remote instance.", appliesTo = DeploySetting.REMOTE)
@AdapterSettingString(name = "password", defaultValue = "polypheny", position = 5,
        description = "Password to be used for authenticating at the remote instance.", appliesTo = DeploySetting.REMOTE)
@AdapterSettingInteger(name = "maxConnections", defaultValue = 25, position = 6,
        description = "Maximum number of concurrent JDBC connections.")



public class OracleStore extends AbstractJdbcSource {
    private String host;
    private int port;
    private String database;
    private String username;
    private DockerContainer container;


    public OracleStore( final long storeId, final String uniqueName, final Map<String, String> settings, final DeployMode mode ) {
        super( storeId,
                uniqueName,
                settings,
                mode,
                "oracle.jdbc.OracleDriver",
                OracleSqlDialect.DEFAULT,
                false );
    }


    @Override
    protected String getConnectionUrl( String dbHostname, int dbPort, String dbName ) {
        return "";
    }


    @Override
    protected boolean requiresSchema() {
        return false;
    }


    @Override
    public void shutdown() {

    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {

    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        return List.of();
    }

}

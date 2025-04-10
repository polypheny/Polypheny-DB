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
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandler;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.adapter.jdbc.sources.AbstractJdbcSource;
import org.polypheny.db.adapter.oracle.OracleSqlDialect;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@AdapterProperties(
        name = "Oracle",
        description = "Data source explicit for relational oracle database systems.",
        usedModes = DeployMode.REMOTE,
        defaultMode = DeployMode.REMOTE)
@AdapterSettingString(name = "host", defaultValue = "localhost", position = 1,
        description = "Hostname or IP address of the remote oracle instance.")
@AdapterSettingInteger(name = "port", defaultValue = 1521, position = 2,
        description = "Port number of the remote oracle instance.")
@AdapterSettingString(name = "database", defaultValue = "XE", position = 3,
        description = "Name of the database to connect to.")
@AdapterSettingString(name = "username", defaultValue = "system", position = 4,
        description = "Username used for authentication at the remote instance.")
@AdapterSettingString(name = "password", defaultValue = "roman123", position = 5,
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
        return String.format( "jdbc:oracle:thin:@%s:%d/%s", dbHostname, dbPort, dbName );
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


    @Override
    public Map<String, List<ExportedColumn>> getExportedColumns() {
        Map<String, List<ExportedColumn>> map = new HashMap<>();

        PolyXid xid = PolyXid.generateLocalTransactionIdentifier( PUID.EMPTY_PUID, PUID.EMPTY_PUID);
        try {
            ConnectionHandler connectionHandler = connectionFactory.getOrCreateConnectionHandler(xid);
            java.sql.Statement statement = connectionHandler.getStatement();
            Connection connection = statement.getConnection();
            DatabaseMetaData dbmd = connection.getMetaData();

            // Für Oracle: Nimm den User (z. B. SYSTEM) als Schema
            String schema = "SYSTEM";  // liefert z. B. SYSTEM
            String tableName = "TEST"; // <- oder hole den Namen dynamisch aus settings

            List<String> primaryKeyColumns = new ArrayList<>();
            try ( ResultSet pk = dbmd.getPrimaryKeys(null, schema, tableName)) {
                while (pk.next()) {
                    primaryKeyColumns.add(pk.getString("COLUMN_NAME").toUpperCase());
                }
            }

            try (ResultSet columns = dbmd.getColumns(null, schema, tableName, "%")) {
                List<ExportedColumn> exportedColumns = new ArrayList<>();

                while (columns.next()) {
                    PolyType type = PolyType.getNameForJdbcType(columns.getInt("DATA_TYPE"));
                    Integer length = null;
                    Integer scale = null;

                    switch (type) {
                        case DECIMAL:
                            length = columns.getInt("COLUMN_SIZE");
                            scale = columns.getInt("DECIMAL_DIGITS");
                            break;
                        case CHAR:
                        case VARCHAR:
                            type = PolyType.VARCHAR;
                            length = columns.getInt("COLUMN_SIZE");
                            break;
                        case VARBINARY:
                        case BINARY:
                            type = PolyType.VARBINARY;
                            length = columns.getInt("COLUMN_SIZE");
                            break;
                        case TIME:
                        case TIMESTAMP:
                            length = columns.getInt("DECIMAL_DIGITS");
                            break;
                        default:
                            // andere Typen ohne Length/Scale
                            break;
                    }

                    exportedColumns.add(new ExportedColumn(
                            columns.getString("COLUMN_NAME").toUpperCase(),
                            type,
                            null, // keine collection
                            length,
                            scale,
                            null,
                            null,
                            "YES".equalsIgnoreCase(columns.getString("IS_NULLABLE")),
                            schema,
                            tableName,
                            columns.getString("COLUMN_NAME").toUpperCase(),
                            columns.getInt("ORDINAL_POSITION"),
                            primaryKeyColumns.contains(columns.getString("COLUMN_NAME").toUpperCase())
                    ));
                }

                map.put(tableName, exportedColumns);
            }
        } catch ( SQLException | ConnectionHandlerException e) {
            throw new GenericRuntimeException("Exception while collecting Oracle schema info", e);
        }

        return map;
    }

}

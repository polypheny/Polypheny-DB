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

package org.polypheny.db.adapter.postgres.source;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.jdbc.sources.AbstractJdbcSource;
import org.polypheny.db.adapter.postgres.PostgresqlSqlDialect;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schemaDiscovery.MetadataProvider;
import org.polypheny.db.schemaDiscovery.Node;


@Slf4j
@AdapterProperties(
        name = "PostgreSQL",
        description = "Relational database system optimized for transactional workload that provides an advanced set of features. PostgreSQL is fully ACID compliant and ensures that all requirements are met.",
        usedModes = DeployMode.REMOTE,
        defaultMode = DeployMode.REMOTE)
@AdapterSettingString(name = "host", defaultValue = "localhost", position = 1,
        description = "Hostname or IP address of the remote PostgreSQL instance.")
@AdapterSettingInteger(name = "port", defaultValue = 5432, position = 2,
        description = "JDBC port number on the remote PostgreSQL instance.")
@AdapterSettingString(name = "database", defaultValue = "postgres", position = 3,
        description = "Name of the database to connect to.")
@AdapterSettingString(name = "username", defaultValue = "postgres", position = 4,
        description = "Username to be used for authenticating at the remote instance.")
@AdapterSettingString(name = "password", defaultValue = "password", position = 5,
        description = "Password to be used for authenticating at the remote instance.")
@AdapterSettingInteger(name = "maxConnections", defaultValue = 25,
        description = "Maximum number of concurrent JDBC connections.")
@AdapterSettingList(name = "transactionIsolation", options = { "SERIALIZABLE", "READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ" }, defaultValue = "SERIALIZABLE",
        description = "Which level of transaction isolation should be used.")
@AdapterSettingString(name = "tables", defaultValue = "foo,bar",
        description = "List of tables which should be imported. The names must to be separated by a comma.")
public class PostgresqlSource extends AbstractJdbcSource {



    public Node fetchMetadataTree() {
        Node root = new Node("relational", getUniqueName());

        Map<String, List<ExportedColumn>> exported = getExportedColumns();

        Map<String, Map<String, List<ExportedColumn>>> grouped = new HashMap<>();
        for (Map.Entry<String, List<ExportedColumn>> entry : exported.entrySet()) {
            for (ExportedColumn col : entry.getValue()) {
                grouped
                        .computeIfAbsent(col.physicalSchemaName, k -> new HashMap<>())
                        .computeIfAbsent(col.physicalTableName, k -> new ArrayList<>())
                        .add(col);
            }
        }

        for (Map.Entry<String, Map<String, List<ExportedColumn>>> schemaEntry : grouped.entrySet()) {
            Node schemaNode = new Node("schema", schemaEntry.getKey());

            for (Map.Entry<String, List<ExportedColumn>> tableEntry : schemaEntry.getValue().entrySet()) {
                Node tableNode = new Node("table", tableEntry.getKey());

                for (ExportedColumn col : tableEntry.getValue()) {
                    Node colNode = new Node("column", col.getName());
                    colNode.addProperty("type", col.type.getName());
                    colNode.addProperty("nullable", col.nullable);
                    colNode.addProperty("primaryKey", col.primary);

                    if (col.length != null) {
                        colNode.addProperty("length", col.length);
                    }
                    if (col.scale != null) {
                        colNode.addProperty("scale", col.scale);
                    }

                    tableNode.addChild(colNode);
                }

                schemaNode.addChild(tableNode);
            }

            root.addChild(schemaNode);
        }

        return root;
    }


    private void printTree(Node node, int depth) {
        System.out.println("  ".repeat(depth) + node.getType() + ": " + node.getName());
        for (Map.Entry<String, Object> entry : node.getProperties().entrySet()) {
            System.out.println("  ".repeat(depth + 1) + "- " + entry.getKey() + ": " + entry.getValue());
        }
        for (Node child : node.getChildren()) {
            printTree(child, depth + 1);
        }
    }


    public PostgresqlSource( final long storeId, final String uniqueName, final Map<String, String> settings, final DeployMode mode ) {
        super(
                storeId,
                uniqueName,
                settings,
                mode,
                "org.postgresql.Driver",
                PostgresqlSqlDialect.DEFAULT,
                false );
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
        Node node = fetchMetadataTree();
        printTree( node, 0 );
        return List.of( table );
    }


}

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


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.java.SchemaFilter;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandler;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.adapter.jdbc.sources.AbstractJdbcSource;
import org.polypheny.db.adapter.postgres.PostgresqlSqlDialect;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schemaDiscovery.AbstractNode;
import org.polypheny.db.schemaDiscovery.AttributeNode;
import org.polypheny.db.schemaDiscovery.MetadataProvider;
import org.polypheny.db.schemaDiscovery.Node;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.PUID.Type;
import org.polypheny.db.transaction.PolyXid;


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
public class PostgresqlSource extends AbstractJdbcSource implements MetadataProvider {

    public AbstractNode metadataRoot;
    private Map<String, List<Map<String, Object>>> previewByTable = new LinkedHashMap<>();


    @Override
    public void setRoot( AbstractNode root ) {
        this.metadataRoot = root;
    }


    @Override
    public AbstractNode fetchMetadataTree() {

        String dbName = settings.get( "database" );
        Node root = new Node( "relational", dbName );

        SchemaFilter filter = SchemaFilter.forAdapter( adapterName );

        PolyXid xid = PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.RANDOM ), PUID.randomPUID( Type.RANDOM ) );

        java.sql.Statement stmt = null;
        Connection conn = null;


        try {
            ConnectionHandler handler = connectionFactory.getOrCreateConnectionHandler( xid );
            stmt = handler.getStatement();
            conn = stmt.getConnection();
            DatabaseMetaData meta = conn.getMetaData();

            try ( ResultSet schemas = requiresSchema()
                    ? meta.getSchemas( dbName, "%" )
                    : meta.getCatalogs() ) {
                while ( schemas.next() ) {

                    String schemaName = requiresSchema()
                            ? schemas.getString( "TABLE_SCHEM" )
                            : schemas.getString( "TABLE_CAT" );

                    if ( filter.ignoredSchemas.contains( schemaName.toLowerCase() ) ) {
                        continue;
                    }

                    AbstractNode schemaNode = new Node( "schema", schemaName );

                    try ( ResultSet tables = meta.getTables(
                            dbName,
                            requiresSchema() ? schemaName : null,
                            "%",
                            new String[]{ "TABLE" }
                    ) ) {
                        while ( tables.next() ) {

                            String tableName = tables.getString( "TABLE_NAME" );

                            String fqName = (requiresSchema() ? "\"" + schemaName + "\"." : "") + "\"" + tableName + "\"";
                            Connection finalConn = conn;
                            previewByTable.computeIfAbsent(
                                    schemaName + "." + tableName,
                                    k -> {
                                        try {
                                            return fetchPreview( finalConn, fqName, 10 );
                                        } catch ( Exception e ) {
                                            log.warn( "Preview failed for {}", fqName, e );
                                            return List.of();
                                        }
                                    } );

                            AbstractNode tableNode = new Node( "table", tableName );

                            Set<String> pkCols = new HashSet<>();
                            try ( ResultSet pk = meta.getPrimaryKeys(
                                    dbName,
                                    requiresSchema() ? schemaName : null,
                                    tableName ) ) {
                                while ( pk.next() ) {
                                    pkCols.add( pk.getString( "COLUMN_NAME" ) );
                                }
                            }

                            try ( ResultSet cols = meta.getColumns(
                                    dbName,
                                    requiresSchema() ? schemaName : null,
                                    tableName,
                                    "%" ) ) {
                                while ( cols.next() ) {

                                    String colName = cols.getString( "COLUMN_NAME" );
                                    String typeName = cols.getString( "TYPE_NAME" );
                                    boolean nullable = cols.getInt( "NULLABLE" ) == DatabaseMetaData.columnNullable;
                                    boolean primary = pkCols.contains( colName );

                                    AbstractNode colNode = new AttributeNode( "column", colName );
                                    colNode.addProperty( "type", typeName );
                                    colNode.addProperty( "nullable", nullable );
                                    colNode.addProperty( "primaryKey", primary );

                                    Integer len = (Integer) cols.getObject( "COLUMN_SIZE" );
                                    Integer scale = (Integer) cols.getObject( "DECIMAL_DIGITS" );
                                    if ( len != null ) {
                                        colNode.addProperty( "length", len );
                                    }
                                    if ( scale != null ) {
                                        colNode.addProperty( "scale", scale );
                                    }

                                    tableNode.addChild( colNode );
                                }
                            }

                            schemaNode.addChild( tableNode );
                        }
                    }

                    root.addChild( schemaNode );
                }
            }

        } catch ( SQLException | ConnectionHandlerException ex ) {
            throw new GenericRuntimeException( "Error while fetching metadata tree", ex );
        } finally {
            try {
                stmt.close();
                conn.close();
            } catch ( SQLException e ) {
                throw new RuntimeException( e );
            }
        }

        this.metadataRoot = root;
        log.error( "Neue Preview ist geladen als: " + previewByTable.toString() );
        return this.metadataRoot;
    }


    @Override
    public List<Map<String, Object>> fetchPreview( Connection conn, String fqName, int limit ) {
        List<Map<String, Object>> rows = new ArrayList<>();
        try ( Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + fqName + " LIMIT " + limit ) ) {

            ResultSetMetaData meta = rs.getMetaData();
            while ( rs.next() ) {
                Map<String, Object> row = new LinkedHashMap<>();
                for ( int i = 1; i <= meta.getColumnCount(); i++ ) {
                    row.put( meta.getColumnName( i ), rs.getObject( i ) );
                }
                rows.add( row );
            }
        } catch ( SQLException e ) {
            log.warn( "Preview failed for {}", fqName, e );
            return List.of();
        }
        return rows;
    }


    public Object getPreview() {
        return this.previewByTable;
    }


    @Override
    public void markSelectedAttributes( List<String> selectedPaths ) {

        List<List<String>> attributePaths = new ArrayList<>();

        for ( String path : selectedPaths ) {
            String cleanPath = path.replaceFirst( " ?:.*$", "" ).trim();

            List<String> segments = Arrays.asList( cleanPath.split( "\\." ) );
            if ( !segments.isEmpty() && segments.get( 0 ).equals( metadataRoot.getName() ) ) {
                segments = segments.subList( 1, segments.size() );
            }

            attributePaths.add( segments );
        }

        for ( List<String> pathSegments : attributePaths ) {
            AbstractNode current = metadataRoot;

            for ( int i = 0; i < pathSegments.size(); i++ ) {
                String segment = pathSegments.get( i );

                if ( i == pathSegments.size() - 1 ) {
                    Optional<AbstractNode> attrNodeOpt = current.getChildren().stream()
                            .filter( c -> c instanceof AttributeNode && segment.equals( c.getName() ) )
                            .findFirst();

                    if ( attrNodeOpt.isPresent() ) {
                        ((AttributeNode) attrNodeOpt.get()).setSelected( true );
                        log.info( "✅ Attribut gesetzt: " + String.join( ".", pathSegments ) );
                    } else {
                        log.warn( "❌ Attribut nicht gefunden: " + String.join( ".", pathSegments ) );
                    }

                } else {
                    Optional<AbstractNode> childOpt = current.getChildren().stream()
                            .filter( c -> segment.equals( c.getName() ) )
                            .findFirst();

                    if ( childOpt.isPresent() ) {
                        current = childOpt.get();
                    } else {
                        log.warn( "❌ Segment nicht gefunden: " + segment + " im Pfad " + String.join( ".", pathSegments ) );
                        break;
                    }
                }
            }
        }

    }


    @Override
    public void printTree( AbstractNode node, int depth ) {
        if ( node == null ) {
            node = this.metadataRoot;
        }
        System.out.println( "  ".repeat( depth ) + node.getType() + ": " + node.getName() );
        for ( Map.Entry<String, Object> entry : node.getProperties().entrySet() ) {
            System.out.println( "  ".repeat( depth + 1 ) + "- " + entry.getKey() + ": " + entry.getValue() );
        }
        for ( AbstractNode child : node.getChildren() ) {
            printTree( child, depth + 1 );
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
        this.metadataRoot = null;
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
        log.error( "Postgres Adapter ID ist: " + this.adapterId );
        return List.of( table );
    }

}

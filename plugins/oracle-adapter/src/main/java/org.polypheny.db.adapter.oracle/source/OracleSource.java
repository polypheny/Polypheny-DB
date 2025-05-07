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
import org.polypheny.db.schemaDiscovery.AbstractNode;
import org.polypheny.db.schemaDiscovery.AttributeNode;
import org.polypheny.db.schemaDiscovery.MetadataProvider;
import org.polypheny.db.schemaDiscovery.Node;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
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
public class OracleSource extends AbstractJdbcSource implements MetadataProvider {

    public AbstractNode metadataRoot;

    private static final Pattern ORACLE_INTERNAL =
            Pattern.compile( "^(AQ\\$|AQS\\$|SYS_|WRI\\$|MDSYS_|XDB_|CTXSYS_|OLAP\\$|LOG\\$|DBMS_|ORDDATA|ORDSYS)",
                    Pattern.CASE_INSENSITIVE );


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
    protected boolean requiresSchema() { return true; }


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
                // logical.table.getNamespaceName(),
                "SYSTEM",
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

        PolyXid xid = PolyXid.generateLocalTransactionIdentifier( PUID.EMPTY_PUID, PUID.EMPTY_PUID );
        try {
            ConnectionHandler connectionHandler = connectionFactory.getOrCreateConnectionHandler( xid );
            java.sql.Statement statement = connectionHandler.getStatement();
            Connection connection = statement.getConnection();
            DatabaseMetaData dbmd = connection.getMetaData();

            String[] tables = settings.get( "tables" ).split( "," );
            for ( String str : tables ) {
                String[] names = str.split( "\\." );

                if ( names.length == 0 || names.length > 2 || (requiresSchema() && names.length == 1) ) {
                    throw new GenericRuntimeException( "Invalid table name: " + tables );
                }
                String schema;
                String tableName;

                if ( requiresSchema() ) {
                    schema = names[0].toUpperCase();
                    tableName = names[1].toUpperCase();
                } else {
                    schema = null;
                    tableName = names[0].toUpperCase();
                }

                List<String> primaryKeyColumns = new ArrayList<>();
                try ( ResultSet pk = dbmd.getPrimaryKeys( null, schema, tableName ) ) {
                    while ( pk.next() ) {
                        primaryKeyColumns.add( pk.getString( "COLUMN_NAME" ).toUpperCase() );
                    }
                }
                try ( ResultSet columns = dbmd.getColumns( null, schema, tableName, "%" ) ) {
                    List<ExportedColumn> exportedColumns = new ArrayList<>();
                    while ( columns.next() ) {
                        PolyType type = PolyType.getNameForJdbcType( columns.getInt( "DATA_TYPE" ) );
                        Integer length = null;
                        Integer scale = null;

                        switch ( type ) {
                            case DECIMAL:
                                length = columns.getInt( "COLUMN_SIZE" );
                                scale = columns.getInt( "DECIMAL_DIGITS" );
                                break;
                            case CHAR:
                            case VARCHAR:
                                type = PolyType.VARCHAR;
                                length = columns.getInt( "COLUMN_SIZE" );
                                break;
                            case VARBINARY:
                            case BINARY:
                                type = PolyType.VARBINARY;
                                length = columns.getInt( "COLUMN_SIZE" );
                                break;
                            case TIME:
                            case TIMESTAMP:
                                length = columns.getInt( "DECIMAL_DIGITS" );
                                break;
                            default:
                                break;
                        }

                        exportedColumns.add( new ExportedColumn(
                                columns.getString( "COLUMN_NAME" ).toUpperCase(),
                                type,
                                null,
                                length,
                                scale,
                                null,
                                null,
                                "YES".equalsIgnoreCase( columns.getString( "IS_NULLABLE" ) ),
                                schema,
                                tableName,
                                columns.getString( "COLUMN_NAME" ).toUpperCase(),
                                columns.getInt( "ORDINAL_POSITION" ),
                                primaryKeyColumns.contains( columns.getString( "COLUMN_NAME" ).toUpperCase() )
                        ) );
                    }

                    map.put( tableName, exportedColumns );
                }
            }
        } catch ( SQLException | ConnectionHandlerException e ) {
            throw new GenericRuntimeException( "Exception while collecting Oracle schema info", e );
        }

        return map;
    }


    @Override
    public AbstractNode fetchMetadataTree() {
        Node root = new Node( "relational", settings.get( "database" ) );

        PolyXid xid = PolyXid.generateLocalTransactionIdentifier( PUID.EMPTY_PUID, PUID.EMPTY_PUID );

        try {
            ConnectionHandler h = connectionFactory.getOrCreateConnectionHandler( xid );
            DatabaseMetaData m = h.getStatement().getConnection().getMetaData();

            String currentUser = m.getUserName();                       // aktueller Owner (= Schema)

            try ( ResultSet schemas = m.getSchemas() ) {
                while ( schemas.next() ) {
                    String schemaName = schemas.getString( "TABLE_SCHEM" );
                    Node schemaNode = new Node( "schema", schemaName );

                    try ( ResultSet tables =
                            m.getTables( null, schemaName, "%", new String[]{ "TABLE" } ) ) {

                        while ( tables.next() ) {
                            String owner = tables.getString( "TABLE_SCHEM" ); // gleich schemaName
                            String tableName = tables.getString( "TABLE_NAME" );

                            /* (a) nur Objekte des eingeloggten Users                                       */
                            if ( !owner.equalsIgnoreCase( currentUser ) ) {
                                continue;
                            }
                            /* (b) interne Oracle‑Tabellen ausblenden                                        */
                            if ( tableName.contains( "$" ) || ORACLE_INTERNAL.matcher( tableName ).find() ) {
                                continue;
                            }

                            Node tableNode = new Node( "table", tableName );

                            /* Primärschlüsselspalten zwischenspeichern ---------------------------------- */
                            Set<String> pkCols = new HashSet<>();
                            try ( ResultSet pk = m.getPrimaryKeys( null, schemaName, tableName ) ) {
                                while ( pk.next() ) {
                                    pkCols.add( pk.getString( "COLUMN_NAME" ) );
                                }
                            }

                            /* Spalten lesen ------------------------------------------------------------- */
                            try ( ResultSet cols =
                                    m.getColumns( null, schemaName, tableName, "%" ) ) {

                                while ( cols.next() ) {
                                    String colName = cols.getString( "COLUMN_NAME" );
                                    String typeName = cols.getString( "TYPE_NAME" );
                                    boolean nullable =
                                            cols.getInt( "NULLABLE" ) == DatabaseMetaData.columnNullable;
                                    boolean primary = pkCols.contains( colName );

                                    AbstractNode colNode = new AttributeNode( "column", colName );
                                    colNode.addProperty( "type", typeName );
                                    colNode.addProperty( "nullable", nullable );
                                    colNode.addProperty( "primaryKey", primary );

                                    Integer len = (Integer) cols.getInt( "COLUMN_SIZE" );
                                    Integer scale = (Integer) cols.getInt( "DECIMAL_DIGITS" );
                                    if ( len != null ) {
                                        colNode.addProperty( "length", len );
                                    }
                                    if ( scale != null ) {
                                        colNode.addProperty( "scale", scale );
                                    }

                                    tableNode.addChild( colNode );
                                }
                            }
                            /* nur Tabellen mit mindestens einer sichtbaren Spalte übernehmen */
                            if ( !tableNode.getChildren().isEmpty() ) {
                                schemaNode.addChild( tableNode );
                            }
                        }
                    }
                    /* Schema nur anhängen, wenn mindestens eine Tabelle behalten wurde */
                    if ( !schemaNode.getChildren().isEmpty() ) {
                        root.addChild( schemaNode );
                    }
                }
            }
        } catch ( SQLException | ConnectionHandlerException e ) {
            throw new GenericRuntimeException( "Error while fetching Oracle metadata", e );
        }

        this.metadataRoot = root;
        return root;
    }


    @Override
    public Object fetchPreview( int limit ) {
        Map<String, List<Map<String, Object>>> preview = new LinkedHashMap<>();

        PolyXid xid = PolyXid.generateLocalTransactionIdentifier( PUID.EMPTY_PUID, PUID.EMPTY_PUID );
        try {
            ConnectionHandler ch = connectionFactory.getOrCreateConnectionHandler( xid );
            java.sql.Connection conn = ch.getStatement().getConnection();

            String[] tables = settings.get( "tables" ).split( "," );
            for ( String str : tables ) {
                String[] parts = str.split( "\\." );
                String schema = parts.length == 2 ? parts[0] : null;
                String table = parts.length == 2 ? parts[1] : parts[0];

                schema = schema.toUpperCase();
                table = table.toUpperCase();

                String fqName = (schema != null ? schema + "." : "") + table;
                List<Map<String, Object>> rows = new ArrayList<>();

                try ( var stmt = conn.createStatement();
                        var rs = stmt.executeQuery( "SELECT * FROM " + fqName + " FETCH FIRST " + limit + " ROWS ONLY" ) ) {

                    var meta = rs.getMetaData();
                    while ( rs.next() ) {
                        Map<String, Object> row = new HashMap<>();
                        for ( int i = 1; i <= meta.getColumnCount(); i++ ) {
                            row.put( meta.getColumnName( i ), rs.getObject( i ) );
                        }
                        rows.add( row );
                    }
                }

                preview.put( fqName, rows );
            }
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error fetching preview data", e );
        }

        return preview;
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


    @Override
    public void setRoot( AbstractNode root ) {
        this.metadataRoot = root;
    }

}

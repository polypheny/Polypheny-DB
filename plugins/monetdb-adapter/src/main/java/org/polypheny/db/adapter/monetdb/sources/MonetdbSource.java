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

package org.polypheny.db.adapter.monetdb.sources;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.jdbc.JdbcTable;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandler;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.adapter.jdbc.connection.TransactionalConnectionFactory;
import org.polypheny.db.adapter.jdbc.sources.AbstractJdbcSource;
import org.polypheny.db.adapter.monetdb.MonetdbSqlDialect;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;


@Slf4j
@AdapterProperties(
        name = "MonetDB",
        description = "MonetDB is an execute-source column-oriented database management system. It is based on an optimistic concurrency control.",
        usedModes = DeployMode.REMOTE,
        defaultMode = DeployMode.REMOTE)
@AdapterSettingString(name = "host", defaultValue = "localhost", description = "Hostname or IP address of the remote MonetDB instance.", position = 1)
@AdapterSettingInteger(name = "port", defaultValue = 50000, description = "JDBC port number on the remote MonetDB instance.", position = 2)
@AdapterSettingString(name = "database", defaultValue = "demo", description = "JDBC port number on the remote MonetDB instance.", position = 3)
@AdapterSettingString(name = "username", defaultValue = "monetdb", description = "Name of the database to connect to.", position = 4)
@AdapterSettingString(name = "password", defaultValue = "monetdb", description = "Username to be used for authenticating at the remote instance.", position = 5)
@AdapterSettingInteger(name = "maxConnections", defaultValue = 25, description = "Password to be used for authenticating at the remote instance.")
@AdapterSettingString(name = "tables", defaultValue = "sys.testtable", description = "Maximum number of concurrent JDBC connections.")
public class MonetdbSource extends AbstractJdbcSource {

    public MonetdbSource( final long storeId, final String uniqueName, final Map<String, String> settings, final DeployMode mode ) {
        super( storeId, uniqueName, settings, mode, "nl.cwi.monetdb.jdbc.MonetDriver", MonetdbSqlDialect.DEFAULT, false );
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


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        PhysicalTable table = adapterCatalog.createTable(
                logical.table.getNamespaceName(),
                logical.table.name,
                logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c.name ) ),
                logical.table,
                logical.columns.stream().collect( Collectors.toMap( t -> t.id, t -> t ) ),
                logical.pkIds,
                allocation );

        JdbcTable physical = currentJdbcSchema.createJdbcTable( table );

        adapterCatalog.replacePhysical( physical );

        return List.of( physical );
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
                    throw new GenericRuntimeException( "Invalid table name: " + str );
                }
                String tableName;
                String schemaPattern;
                if ( requiresSchema() ) {
                    schemaPattern = names[0];
                    tableName = names[1];
                } else {
                    schemaPattern = null;
                    tableName = names[0];
                }
                List<String> primaryKeyColumns = new ArrayList<>();
                try ( ResultSet row = dbmd.getPrimaryKeys( null, schemaPattern, tableName ) ) {
                    while ( row.next() ) {
                        primaryKeyColumns.add( row.getString( "COLUMN_NAME" ) );
                    }
                }
                try ( ResultSet row = dbmd.getColumns( null, schemaPattern, tableName, "%" ) ) {
                    List<ExportedColumn> list = new ArrayList<>();
                    while ( row.next() ) {
                        PolyType type = PolyType.getNameForJdbcType( row.getInt( "DATA_TYPE" ) );
                        Integer length = null;
                        Integer scale = null;
                        Integer dimension = null;
                        Integer cardinality = null;
                        switch ( type ) {
                            case BOOLEAN:
                            case TINYINT:
                            case SMALLINT:
                            case INTEGER:
                            case BIGINT:
                            case FLOAT:
                            case REAL:
                            case DOUBLE:
                            case DATE:
                                break;
                            case DECIMAL:
                                length = row.getInt( "COLUMN_SIZE" );
                                scale = row.getInt( "DECIMAL_DIGITS" );
                                break;
                            case TIME:
                                length = row.getInt( "DECIMAL_DIGITS" );
                                if ( length > 3 ) {
                                    throw new GenericRuntimeException( "Unsupported precision for data type time: " + length );
                                }
                                break;
                            case TIMESTAMP:
                                length = row.getInt( "DECIMAL_DIGITS" );
                                if ( length > 3 ) {
                                    throw new GenericRuntimeException( "Unsupported precision for data type timestamp: " + length );
                                }
                                break;
                            case CHAR:
                            case VARCHAR:
                                type = PolyType.VARCHAR;
                                length = row.getInt( "COLUMN_SIZE" );
                                break;
                            case BINARY:
                            case VARBINARY:
                                type = PolyType.VARBINARY;
                                length = row.getInt( "COLUMN_SIZE" );
                                break;
                            default:
                                throw new GenericRuntimeException( "Unsupported data type: " + type.getName() );
                        }
                        list.add( new ExportedColumn(
                                row.getString( "COLUMN_NAME" ).toLowerCase(),
                                type,
                                null,
                                length,
                                scale,
                                dimension,
                                cardinality,
                                row.getString( "IS_NULLABLE" ).equalsIgnoreCase( "YES" ),
                                requiresSchema() ? row.getString( "TABLE_SCHEM" ) : row.getString( "TABLE_CAT" ),
                                row.getString( "TABLE_NAME" ),
                                row.getString( "COLUMN_NAME" ),
                                row.getInt( "ORDINAL_POSITION" ),
                                primaryKeyColumns.contains( row.getString( "COLUMN_NAME" ) )
                        ) );
                    }
                    map.put( tableName, list );
                }
            }
        } catch ( SQLException | ConnectionHandlerException e ) {
            throw new GenericRuntimeException( "Exception while collecting schema information!" + e );
        }
        return map;
    }

}

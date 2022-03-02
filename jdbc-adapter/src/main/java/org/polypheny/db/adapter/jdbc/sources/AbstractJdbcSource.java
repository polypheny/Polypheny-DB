/*
 * Copyright 2019-2022 The Polypheny Project
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


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.jdbc.JdbcSchema;
import org.polypheny.db.adapter.jdbc.JdbcUtils;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandler;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.adapter.jdbc.connection.TransactionalConnectionFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.sql.sql.SqlDialect;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;


@Slf4j
public abstract class AbstractJdbcSource extends DataSource {

    protected SqlDialect dialect;
    protected JdbcSchema currentJdbcSchema;

    protected ConnectionFactory connectionFactory;


    public AbstractJdbcSource(
            int storeId,
            String uniqueName,
            Map<String, String> settings,
            String diverClass,
            SqlDialect dialect,
            boolean readOnly ) {
        super( storeId, uniqueName, settings, readOnly );
        this.connectionFactory = createConnectionFactory( settings, dialect, diverClass );
        this.dialect = dialect;
        // Register the JDBC Pool Size as information in the information manager and enable it
        registerInformationPage();
    }


    protected void registerInformationPage() {
        JdbcUtils.addInformationPoolSize( informationPage, informationGroups, informationElements, connectionFactory, getUniqueName() );
        addInformationPhysicalNames();
        enableInformationPage();
    }


    protected ConnectionFactory createConnectionFactory( final Map<String, String> settings, SqlDialect dialect, String driverClass ) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName( driverClass );

        final String connectionUrl = getConnectionUrl( settings.get( "host" ), Integer.parseInt( settings.get( "port" ) ), settings.get( "database" ) );
        dataSource.setUrl( connectionUrl );
        if ( log.isDebugEnabled() ) {
            log.debug( "JDBC Connection URL: {}", connectionUrl );
        }
        dataSource.setUsername( settings.get( "username" ) );
        dataSource.setPassword( settings.get( "password" ) );
        dataSource.setDefaultAutoCommit( false );
        switch ( settings.get( "transactionIsolation" ) ) {
            case "SERIALIZABLE":
                dataSource.setDefaultTransactionIsolation( Connection.TRANSACTION_SERIALIZABLE );
                break;
            case "READ_UNCOMMITTED":
                dataSource.setDefaultTransactionIsolation( Connection.TRANSACTION_READ_UNCOMMITTED );
                break;
            case "READ_COMMITTED":
                dataSource.setDefaultTransactionIsolation( Connection.TRANSACTION_READ_COMMITTED );
                break;
            case "REPEATABLE_READ":
                dataSource.setDefaultTransactionIsolation( Connection.TRANSACTION_REPEATABLE_READ );
                break;
        }
        return new TransactionalConnectionFactory( dataSource, Integer.parseInt( settings.get( "maxConnections" ) ), dialect );
    }


    protected abstract String getConnectionUrl( final String dbHostname, final int dbPort, final String dbName );


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        currentJdbcSchema = JdbcSchema.create( rootSchema, name, connectionFactory, dialect, this );
    }


    @Override
    public void truncate( Context context, CatalogTable catalogTable ) {
        // We get the physical schema / table name by checking existing column placements of the same logical table placed on this store.
        // This works because there is only one physical table for each logical table on JDBC stores. The reason for choosing this
        // approach rather than using the default physical schema / table names is that this approach allows truncating linked tables.
        String physicalTableName = Catalog.getInstance().getPartitionPlacementsByTableOnAdapter( getAdapterId(), catalogTable.id ).get( 0 ).physicalTableName;
        String physicalSchemaName = Catalog.getInstance().getPartitionPlacementsByTableOnAdapter( getAdapterId(), catalogTable.id ).get( 0 ).physicalSchemaName;
        StringBuilder builder = new StringBuilder();
        builder.append( "TRUNCATE TABLE " )
                .append( dialect.quoteIdentifier( physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physicalTableName ) );
        executeUpdate( builder, context );
    }


    protected void executeUpdate( StringBuilder builder, Context context ) {
        try {
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            connectionFactory.getOrCreateConnectionHandler( context.getStatement().getTransaction().getXid() ).executeUpdate( builder.toString() );
        } catch ( SQLException | ConnectionHandlerException e ) {
            throw new RuntimeException( e );
        }
    }


    @SneakyThrows
    @Override
    public boolean prepare( PolyXid xid ) {
        if ( connectionFactory.hasConnectionHandler( xid ) ) {
            return connectionFactory.getConnectionHandler( xid ).prepare();
        } else {
            log.warn( "There is no connection to prepare (Unique name: {}, XID: {})! Returning true.", getUniqueName(), xid );
            return true;
        }
    }


    @SneakyThrows
    @Override
    public void commit( PolyXid xid ) {
        if ( connectionFactory.hasConnectionHandler( xid ) ) {
            connectionFactory.getConnectionHandler( xid ).commit();
        } else {
            log.warn( "There is no connection to commit (Unique name: {}, XID: {})!", getUniqueName(), xid );
        }
    }


    @SneakyThrows
    @Override
    public void rollback( PolyXid xid ) {
        if ( connectionFactory.hasConnectionHandler( xid ) ) {
            connectionFactory.getConnectionHandler( xid ).rollback();
        } else {
            log.warn( "There is no connection to rollback (Unique name: {}, XID: {})!", getUniqueName(), xid );
        }
    }


    protected abstract boolean requiresSchema();


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
                    throw new RuntimeException( "Invalid table name: " + str );
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
                try ( ResultSet row = dbmd.getPrimaryKeys( settings.get( "database" ), schemaPattern, tableName ) ) {
                    while ( row.next() ) {
                        primaryKeyColumns.add( row.getString( "COLUMN_NAME" ) );
                    }
                }
                try ( ResultSet row = dbmd.getColumns( settings.get( "database" ), schemaPattern, tableName, "%" ) ) {
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
                            case TIME_WITH_LOCAL_TIME_ZONE:
                                type = PolyType.TIME;
                                length = row.getInt( "DECIMAL_DIGITS" );
                                if ( length > 3 ) {
                                    throw new RuntimeException( "Unsupported precision for data type time: " + length );
                                }
                                break;
                            case TIMESTAMP:
                            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                                type = PolyType.TIMESTAMP;
                                length = row.getInt( "DECIMAL_DIGITS" );
                                if ( length > 3 ) {
                                    throw new RuntimeException( "Unsupported precision for data type timestamp: " + length );
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
                                throw new RuntimeException( "Unsupported data type: " + type.getName() );
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
            throw new RuntimeException( "Exception while collecting schema information!" + e );
        }
        return map;
    }

}

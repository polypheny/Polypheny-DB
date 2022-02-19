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

package org.polypheny.db.adapter.jdbc.stores;


import com.google.common.collect.ImmutableList;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.Adapter.AdapterSettingString;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandler;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.adapter.jdbc.connection.TransactionalConnectionFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.docker.DockerManager.ContainerBuilder;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.sql.dialect.MonetdbSqlDialect;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.PUID.Type;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;


@Slf4j
@AdapterProperties(
        name = "MonetDB",
        description = "MonetDB is an open-source column-oriented database management system. It is based on an optimistic concurrency control.",
        usedModes = { DeployMode.REMOTE, DeployMode.DOCKER })
@AdapterSettingString(name = "host", defaultValue = "localhost", description = "Hostname or IP address of the remote MonetDB instance.", position = 1, appliesTo = DeploySetting.REMOTE)
@AdapterSettingInteger(name = "port", defaultValue = 50000, description = "JDBC port number on the remote MonetDB instance.", position = 2)
@AdapterSettingString(name = "database", defaultValue = "polypheny", description = "JDBC port number on the remote MonetDB instance.", position = 3, appliesTo = DeploySetting.REMOTE)
@AdapterSettingString(name = "username", defaultValue = "polypheny", description = "Name of the database to connect to.", position = 4, appliesTo = DeploySetting.REMOTE)
@AdapterSettingString(name = "password", defaultValue = "polypheny", description = "Username to be used for authenticating at the remote instance.")
@AdapterSettingInteger(name = "maxConnections", defaultValue = 25, description = "Password to be used for authenticating at the remote instance.")
public class MonetdbStore extends AbstractJdbcStore {

    private String host;
    private String database;
    private String username;


    public MonetdbStore( int storeId, String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, MonetdbSqlDialect.DEFAULT, true );
    }


    @Override
    protected ConnectionFactory deployDocker( int dockerInstanceId ) {
        DockerManager.Container container = new ContainerBuilder( getAdapterId(), "topaztechnology/monetdb:11.37.11", getUniqueName(), dockerInstanceId )
                .withMappedPort( 50000, Integer.parseInt( settings.get( "port" ) ) )
                .withEnvironmentVariables( Arrays.asList( "MONETDB_PASSWORD=" + settings.get( "password" ), "MONET_DATABASE=monetdb" ) )
                .withReadyTest( this::testConnection, 15000 )
                .build();

        host = container.getHost();
        database = "monetdb";
        username = "monetdb";

        DockerManager.getInstance().initialize( container ).start();

        ConnectionFactory connectionFactory = createConnectionFactory();
        createDefaultSchema( connectionFactory );
        return connectionFactory;
    }


    @Override
    protected ConnectionFactory deployRemote() {
        host = settings.get( "host" );
        database = settings.get( "database" );
        username = settings.get( "username" );
        if ( !testConnection() ) {
            throw new RuntimeException( "Unable to connect" );
        }
        ConnectionFactory connectionFactory = createConnectionFactory();
        createDefaultSchema( connectionFactory );
        return connectionFactory;
    }


    private void createDefaultSchema( ConnectionFactory connectionFactory ) {
        // Create schema public if it does not exist
        PolyXid randomXid = PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.NODE ), PUID.randomPUID( Type.TRANSACTION ) );
        try {
            ConnectionHandler handler = connectionFactory.getOrCreateConnectionHandler( randomXid );
            handler.execute( "CREATE SCHEMA IF NOT EXISTS \"public\";" );
            handler.commit();
        } catch ( SQLException | ConnectionHandlerException e ) {
            throw new RuntimeException( "Exception while creating default schema on monetdb store!", e );
        }
    }


    private ConnectionFactory createConnectionFactory() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName( "nl.cwi.monetdb.jdbc.MonetDriver" );

        final String connectionUrl = getConnectionUrl( host, Integer.parseInt( settings.get( "port" ) ), database );
        dataSource.setUrl( connectionUrl );
        dataSource.setUsername( username );
        dataSource.setPassword( settings.get( "password" ) );
        dataSource.setDefaultAutoCommit( false );
        return new TransactionalConnectionFactory( dataSource, Integer.parseInt( settings.get( "maxConnections" ) ), dialect );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn, PolyType oldType ) {
        if ( !this.dialect.supportsNestedArrays() && catalogColumn.collectionsType != null ) {
            return;
        }
        // MonetDB does not support updating the column type directly. We need to do a work-around
        CatalogTable catalogTable = Catalog.getInstance().getTable( catalogColumn.tableId );
        String tmpColName = columnPlacement.physicalColumnName + "tmp";
        StringBuilder builder;

        for ( CatalogPartitionPlacement partitionPlacement : catalog.getPartitionPlacementsByTableOnAdapter( columnPlacement.adapterId, columnPlacement.tableId ) ) {

            // (1) Create a temporary column `alter table tabX add column colXtemp NEW_TYPE;`
            builder = new StringBuilder();
            builder.append( "ALTER TABLE " )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalSchemaName ) )
                    .append( "." )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalTableName ) );
            builder.append( " ADD COLUMN " )
                    .append( dialect.quoteIdentifier( tmpColName ) )
                    .append( " " )
                    .append( getTypeString( catalogColumn.type ) );
            executeUpdate( builder, context );

            // (2) Set data in temporary column to original data `update tabX set colXtemp=colX;`
            builder = new StringBuilder();
            builder.append( "UPDATE " )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalSchemaName ) )
                    .append( "." )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalTableName ) );
            builder.append( " SET " )
                    .append( dialect.quoteIdentifier( tmpColName ) )
                    .append( "=" )
                    .append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) );
            executeUpdate( builder, context );

            // (3) Remove the original column `alter table tabX drop column colX;`
            builder = new StringBuilder();
            builder.append( "ALTER TABLE " )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalSchemaName ) )
                    .append( "." )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalTableName ) );
            builder.append( " DROP COLUMN " )
                    .append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) );
            executeUpdate( builder, context );

            // (4) Re-create the original column with the new type `alter table tabX add column colX NEW_TYPE;
            builder = new StringBuilder();
            builder.append( "ALTER TABLE " )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalSchemaName ) )
                    .append( "." )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalTableName ) );
            builder.append( " ADD COLUMN " )
                    .append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) )
                    .append( " " )
                    .append( getTypeString( catalogColumn.type ) );
            executeUpdate( builder, context );

            // (5) Move data from temporary column to new column `update tabX set colX=colXtemp`;
            builder = new StringBuilder();
            builder.append( "UPDATE " )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalSchemaName ) )
                    .append( "." )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalTableName ) );
            builder.append( " SET " )
                    .append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) )
                    .append( "=" )
                    .append( dialect.quoteIdentifier( tmpColName ) );
            executeUpdate( builder, context );

            // (6) Drop the temporary column `alter table tabX drop column colXtemp;`
            builder = new StringBuilder();
            builder.append( "ALTER TABLE " )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalSchemaName ) )
                    .append( "." )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalTableName ) );
            builder.append( " DROP COLUMN " )
                    .append( dialect.quoteIdentifier( tmpColName ) );
            executeUpdate( builder, context );
        }
        Catalog.getInstance().updateColumnPlacementPhysicalPosition( getAdapterId(), catalogColumn.id );

    }


    @Override
    public Table createTableSchema( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
        return currentJdbcSchema.createJdbcTable( catalogTable, columnPlacementsOnStore, partitionPlacement );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentJdbcSchema;
    }


    @Override
    public void addIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds ) {
        throw new RuntimeException( "MonetDB adapter does not support adding indexes" );
    }


    @Override
    public void dropIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds ) {
        throw new RuntimeException( "MonetDB adapter does not support dropping indexes" );
    }


    @Override
    public List<AvailableIndexMethod> getAvailableIndexMethods() {
        // According to the MonetDB documentation, MonetDB takes create index statements only as an advice and often freely
        // neglects them. Indexes are created and removed automatically. We therefore decided to not support manually creating
        // indexes on MonetDB.
        return ImmutableList.of();
    }


    @Override
    public AvailableIndexMethod getDefaultIndexMethod() {
        throw new RuntimeException( "MonetDB adapter does not support adding indexes" );
    }


    @Override
    public List<FunctionalIndexInfo> getFunctionalIndexes( CatalogTable catalogTable ) {
        return ImmutableList.of(); // TODO
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // TODO: Implement disconnect and reconnect to MonetDB instance.
    }


    @Override
    protected String getTypeString( PolyType type ) {
        if ( type.getFamily() == PolyTypeFamily.MULTIMEDIA ) {
            return "BLOB";
        }
        switch ( type ) {
            case BOOLEAN:
                return "BOOLEAN";
            case VARBINARY:
                throw new RuntimeException( "Unsupported datatype: " + type.name() );
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case REAL:
                return "REAL";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                return "DECIMAL";
            case VARCHAR:
            case JSON:
                return "VARCHAR";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case ARRAY:
                return "TEXT";
        }
        throw new RuntimeException( "Unknown type: " + type.name() );
    }


    @Override
    protected String getDefaultPhysicalSchemaName() {
        return "public";
    }


    private static String getConnectionUrl( final String dbHostname, final int dbPort, final String dbName ) {
        return String.format( "jdbc:monetdb://%s:%d/%s", dbHostname, dbPort, dbName );
    }


    private boolean testConnection() {
        ConnectionFactory connectionFactory = null;
        ConnectionHandler handler = null;
        try {
            connectionFactory = createConnectionFactory();

            PolyXid randomXid = PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.NODE ), PUID.randomPUID( Type.TRANSACTION ) );
            handler = connectionFactory.getOrCreateConnectionHandler( randomXid );
            ResultSet resultSet = handler.executeQuery( "SELECT 1" );

            if ( resultSet.isBeforeFirst() ) {
                handler.commit();
                connectionFactory.close();
                return true;
            }
        } catch ( Exception e ) {
            // ignore
        }
        if ( handler != null ) {
            try {
                handler.commit();
            } catch ( ConnectionHandlerException e ) {
                // ignore
            }
        }
        if ( connectionFactory != null ) {
            try {
                connectionFactory.close();
            } catch ( SQLException e ) {
                // ignore
            }
        }

        return false;
    }

}

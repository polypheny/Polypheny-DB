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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
import org.polypheny.db.sql.sql.dialect.PostgresqlSqlDialect;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.PUID.Type;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;


@Slf4j
@AdapterProperties(
        name = "PostgreSQL",
        description = "Relational database system optimized for transactional workload that provides an advanced set of features. PostgreSQL is fully ACID compliant and ensures that all requirements are met.",
        usedModes = { DeployMode.REMOTE, DeployMode.DOCKER })
@AdapterSettingString(name = "host", defaultValue = "localhost", position = 1,
        description = "Hostname or IP address of the remote PostgreSQL instance.", appliesTo = DeploySetting.REMOTE)
@AdapterSettingInteger(name = "port", defaultValue = 5432, position = 2,
        description = "JDBC port number on the remote PostgreSQL instance.")
@AdapterSettingString(name = "database", defaultValue = "polypheny", position = 3,
        description = "Name of the database to connect to.", appliesTo = DeploySetting.REMOTE)
@AdapterSettingString(name = "username", defaultValue = "polypheny", position = 4,
        description = "Username to be used for authenticating at the remote instance.", appliesTo = DeploySetting.REMOTE)
@AdapterSettingString(name = "password", defaultValue = "polypheny", position = 5,
        description = "Password to be used for authenticating at the remote instance.")
@AdapterSettingInteger(name = "maxConnections", defaultValue = 25,
        description = "Maximum number of concurrent JDBC connections.")
public class PostgresqlStore extends AbstractJdbcStore {

    private String host;
    private String database;
    private String username;


    public PostgresqlStore( int storeId, String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, PostgresqlSqlDialect.DEFAULT, true );
    }


    @Override
    public ConnectionFactory deployDocker( int instanceId ) {
        DockerManager.Container container = new ContainerBuilder( getAdapterId(), "postgres:13.2", getUniqueName(), instanceId )
                .withMappedPort( 5432, Integer.parseInt( settings.get( "port" ) ) )
                .withEnvironmentVariable( "POSTGRES_PASSWORD=" + settings.get( "password" ) )
                .withReadyTest( this::testConnection, 15000 )
                .build();

        host = container.getHost();
        database = "postgres";
        username = "postgres";

        DockerManager.getInstance().initialize( container ).start();

        return createConnectionFactory();
    }


    @Override
    protected ConnectionFactory deployRemote() {
        host = settings.get( "host" );
        database = settings.get( "database" );
        username = settings.get( "username" );
        if ( !testConnection() ) {
            throw new RuntimeException( "Unable to connect" );
        }
        return createConnectionFactory();
    }


    private ConnectionFactory createConnectionFactory() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName( "org.postgresql.Driver" );

        final String connectionUrl = getConnectionUrl( host, Integer.parseInt( settings.get( "port" ) ), database );
        dataSource.setUrl( connectionUrl );
        dataSource.setUsername( username );
        dataSource.setPassword( settings.get( "password" ) );
        dataSource.setDefaultAutoCommit( false );
        dataSource.setDefaultTransactionIsolation( Connection.TRANSACTION_READ_UNCOMMITTED );
        return new TransactionalConnectionFactory( dataSource, Integer.parseInt( settings.get( "maxConnections" ) ), dialect );
    }


    @Override
    public void createUdfs() {
        PolyXid xid = PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.CONNECTION ), PUID.randomPUID( Type.CONNECTION ) );
        try {
            ConnectionHandler ch = connectionFactory.getOrCreateConnectionHandler( xid );
            ch.executeUpdate( "DROP FUNCTION IF EXISTS ROUND(float,int)" );
            ch.executeUpdate( "DROP FUNCTION IF EXISTS MIN(boolean)" );
            ch.executeUpdate( "DROP FUNCTION IF EXISTS MAX(boolean)" );
            ch.executeUpdate( "DROP FUNCTION IF EXISTS DOW_SUNDAY(timestamp)" );
            ch.executeUpdate( "CREATE FUNCTION ROUND(float,int) RETURNS NUMERIC AS $$ SELECT ROUND($1::numeric,$2); $$ language SQL IMMUTABLE;" );
            ch.executeUpdate( "CREATE FUNCTION MIN(boolean) RETURNS INT AS $$ SELECT MIN($1::int); $$ language SQL IMMUTABLE;" );
            ch.executeUpdate( "CREATE FUNCTION MAX(boolean) RETURNS INT AS $$ SELECT MAX($1::int); $$ language SQL IMMUTABLE;" );
            ch.executeUpdate( "CREATE FUNCTION DOW_SUNDAY(timestamp) RETURNS INT AS $$ SELECT EXTRACT(DOW FROM $1::timestamp) + 1; $$ language SQL IMMUTABLE;" );
            ch.commit();
        } catch ( ConnectionHandlerException | SQLException e ) {
            log.error( "Error while creating udfs on Postgres", e );
        }
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn, PolyType oldType ) {
        StringBuilder builder = new StringBuilder();
        List<CatalogPartitionPlacement> partitionPlacements = catalog.getPartitionPlacementsByTableOnAdapter( getAdapterId(), catalogColumn.tableId );

        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
            builder.append( "ALTER TABLE " )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalSchemaName ) )
                    .append( "." )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalTableName ) );
            builder.append( " ALTER COLUMN " ).append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) );
            builder.append( " TYPE " ).append( getTypeString( catalogColumn.type ) );
            if ( catalogColumn.collectionsType != null ) {
                builder.append( " " ).append( catalogColumn.collectionsType.toString() );
            }
            if ( catalogColumn.length != null ) {
                builder.append( "(" );
                builder.append( catalogColumn.length );
                if ( catalogColumn.scale != null ) {
                    builder.append( "," ).append( catalogColumn.scale );
                }
                builder.append( ")" );
            }
            builder.append( " USING " )
                    .append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) )
                    .append( "::" )
                    .append( getTypeString( catalogColumn.type ) );
            if ( catalogColumn.collectionsType != null ) {
                builder.append( " " ).append( catalogColumn.collectionsType.toString() );
            }
            executeUpdate( builder, context );
        }

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
        List<CatalogPartitionPlacement> partitionPlacements = new ArrayList<>();
        partitionIds.forEach( id -> partitionPlacements.add( catalog.getPartitionPlacement( getAdapterId(), id ) ) );

        String physicalIndexName = getPhysicalIndexName( catalogIndex.key.tableId, catalogIndex.id );

        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
            StringBuilder builder = new StringBuilder();
            builder.append( "CREATE " );
            if ( catalogIndex.unique ) {
                builder.append( "UNIQUE INDEX " );
            } else {
                builder.append( "INDEX " );
            }

            builder.append( dialect.quoteIdentifier( physicalIndexName + "_" + partitionPlacement.partitionId ) );
            builder.append( " ON " )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalSchemaName ) )
                    .append( "." )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalTableName ) );

            builder.append( " USING " );
            switch ( catalogIndex.method ) {
                case "btree":
                case "btree_unique":
                    builder.append( "btree" );
                    break;
                case "hash":
                case "hash_unique":
                    builder.append( "hash" );
                    break;
                case "gin":
                case "gin_unique":
                    builder.append( "gin" );
                    break;
                case "brin":
                    builder.append( "brin" );
                    break;
            }

            builder.append( "(" );
            boolean first = true;
            for ( long columnId : catalogIndex.key.columnIds ) {
                if ( !first ) {
                    builder.append( ", " );
                }
                first = false;
                builder.append( dialect.quoteIdentifier( getPhysicalColumnName( columnId ) ) ).append( " " );
            }
            builder.append( ")" );

            executeUpdate( builder, context );
        }
        Catalog.getInstance().setIndexPhysicalName( catalogIndex.id, physicalIndexName );
    }


    @Override
    public void dropIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds ) {
        List<CatalogPartitionPlacement> partitionPlacements = new ArrayList<>();
        partitionIds.forEach( id -> partitionPlacements.add( catalog.getPartitionPlacement( getAdapterId(), id ) ) );

        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
            StringBuilder builder = new StringBuilder();
            builder.append( "DROP INDEX " );
            builder.append( dialect.quoteIdentifier( catalogIndex.physicalName + "_" + partitionPlacement.partitionId ) );
            executeUpdate( builder, context );
        }
    }


    @Override
    public List<AvailableIndexMethod> getAvailableIndexMethods() {
        return ImmutableList.of(
                new AvailableIndexMethod( "btree", "B-TREE" ),
                new AvailableIndexMethod( "hash", "HASH" ),
                new AvailableIndexMethod( "gin", "GIN (Generalized Inverted Index)" ),
                new AvailableIndexMethod( "brin", "BRIN (Block Range index)" )
        );
    }


    @Override
    public AvailableIndexMethod getDefaultIndexMethod() {
        return getAvailableIndexMethods().get( 0 );
    }


    @Override
    public List<FunctionalIndexInfo> getFunctionalIndexes( CatalogTable catalogTable ) {
        return ImmutableList.of();
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // TODO: Implement disconnect and reconnect to PostgreSQL instance.
    }


    @Override
    protected void createColumnDefinition( CatalogColumn catalogColumn, StringBuilder builder ) {
        builder.append( " " ).append( getTypeString( catalogColumn.type ) );
        if ( catalogColumn.length != null ) {
            builder.append( "(" ).append( catalogColumn.length );
            if ( catalogColumn.scale != null ) {
                builder.append( "," ).append( catalogColumn.scale );
            }
            builder.append( ")" );
        }
        if ( catalogColumn.collectionsType != null ) {
            for ( int i = 0; i < catalogColumn.dimension; i++ ) {
                builder.append( "[" ).append( catalogColumn.cardinality ).append( "]" );
            }
        }
    }


    @Override
    protected String getTypeString( PolyType type ) {
        if ( type.getFamily() == PolyTypeFamily.MULTIMEDIA ) {
            return "BYTEA";
        }
        switch ( type ) {
            case BOOLEAN:
                return "BOOLEAN";
            case VARBINARY:
                return "VARBINARY";
            case TINYINT:
                return "SMALLINT";
            case SMALLINT:
                return "SMALLINT";
            case INTEGER:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case REAL:
                return "REAL";
            case DOUBLE:
                return "FLOAT";
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
                return "ARRAY";
        }
        throw new RuntimeException( "Unknown type: " + type.name() );
    }


    @Override
    protected String getDefaultPhysicalSchemaName() {
        return "public";
    }


    private static String getConnectionUrl( final String dbHostname, final int dbPort, final String dbName ) {
        return String.format( "jdbc:postgresql://%s:%d/%s", dbHostname, dbPort, dbName );
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

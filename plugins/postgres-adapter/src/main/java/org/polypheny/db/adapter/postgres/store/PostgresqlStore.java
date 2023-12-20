/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.adapter.postgres.store;


import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbcp2.BasicDataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandler;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.adapter.jdbc.connection.TransactionalConnectionFactory;
import org.polypheny.db.adapter.jdbc.stores.AbstractJdbcStore;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.docker.DockerContainer;
import org.polypheny.db.docker.DockerContainer.HostAndPort;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.plugins.PolyPluginManager;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.language.dialect.PostgresqlSqlDialect;
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.PUID.Type;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.PasswordGenerator;


@Slf4j
@AdapterProperties(
        name = "PostgreSQL",
        description = "Relational database system optimized for transactional workload that provides an advanced set of features. PostgreSQL is fully ACID compliant and ensures that all requirements are met.",
        usedModes = { DeployMode.REMOTE, DeployMode.DOCKER },
        defaultMode = DeployMode.DOCKER)
@AdapterSettingString(name = "host", defaultValue = "localhost", position = 1,
        description = "Hostname or IP address of the remote PostgreSQL instance.", appliesTo = DeploySetting.REMOTE)
@AdapterSettingInteger(name = "port", defaultValue = 5432, position = 2,
        description = "JDBC port number on the remote PostgreSQL instance.", appliesTo = DeploySetting.REMOTE)
@AdapterSettingString(name = "database", defaultValue = "polypheny", position = 3,
        description = "Name of the database to connect to.", appliesTo = DeploySetting.REMOTE)
@AdapterSettingString(name = "username", defaultValue = "polypheny", position = 4,
        description = "Username to be used for authenticating at the remote instance.", appliesTo = DeploySetting.REMOTE)
@AdapterSettingString(name = "password", defaultValue = "polypheny", position = 5,
        description = "Password to be used for authenticating at the remote instance.", appliesTo = DeploySetting.REMOTE)
@AdapterSettingInteger(name = "maxConnections", defaultValue = 25, position = 6,
        description = "Maximum number of concurrent JDBC connections.")
public class PostgresqlStore extends AbstractJdbcStore {


    private String host;
    private int port;
    private String database;
    private String username;
    private DockerContainer container;


    public PostgresqlStore( long storeId, String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, PostgresqlSqlDialect.DEFAULT, true );
    }


    @Getter
    private final List<PolyType> unsupportedTypes = ImmutableList.of( PolyType.ARRAY, PolyType.MAP );


    @Override
    public ConnectionFactory deployDocker( int instanceId ) {
        database = "postgres";
        username = "postgres";

        if ( settings.getOrDefault( "deploymentId", "" ).isEmpty() ) {
            if ( settings.getOrDefault( "password", "polypheny" ).equals( "polypheny" ) ) {
                settings.put( "password", PasswordGenerator.generatePassword( 256 ) );
                updateSettings( settings );
            }

            DockerInstance instance = DockerManager.getInstance().getInstanceById( instanceId )
                    .orElseThrow( () -> new GenericRuntimeException( "No docker instance with id " + instanceId ) );
            try {
                // kartoza/postgis:14
                // postgis/postgis:14-3.4-alpine
                container = instance.newBuilder( "postgis/postgis:14-3.4-alpine", getUniqueName() )
                        .withEnvironmentVariable( "POSTGRES_PASSWORD", settings.get( "password" ) )
                        .withEnvironmentVariable( "POSTGRES_USER", username )
                        .withEnvironmentVariable( "POSTGRES_DBNAME", database )
                        .createAndStart();
            } catch ( IOException e ) {
                throw new GenericRuntimeException( e );
            }

            if ( !container.waitTillStarted( this::testDockerConnection, 15000 ) ) {
                container.destroy();
                throw new GenericRuntimeException( "Failed to connect to postgres container" );
            }

            deploymentId = container.getContainerId();
            settings.put( "deploymentId", deploymentId );
            updateSettings( settings );
        } else {
            deploymentId = settings.get( "deploymentId" );
            DockerManager.getInstance(); // Make sure docker instances are loaded. Very hacky, but it works.
            container = DockerContainer.getContainerByUUID( deploymentId )
                    .orElseThrow( () -> new GenericRuntimeException( "Could not find docker container with id " + deploymentId ) );
            if ( !testDockerConnection() ) {
                throw new GenericRuntimeException( "Could not connect to container" );
            }
        }

        return createConnectionFactory();
    }


    @Override
    protected ConnectionFactory deployRemote() {
        host = settings.get( "host" );
        port = Integer.parseInt( settings.get( "port" ) );
        database = settings.get( "database" );
        username = settings.get( "username" );
        if ( !testConnection() ) {
            throw new GenericRuntimeException( "Unable to connect" );
        }
        return createConnectionFactory();
    }


    private ConnectionFactory createConnectionFactory() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName( "org.postgresql.Driver" );

        final String connectionUrl = getConnectionUrl( host, port, database );
        dataSource.setUrl( connectionUrl );
        dataSource.setUsername( username );
        dataSource.setPassword( settings.get( "password" ) );
        dataSource.setDefaultAutoCommit( false );
        dataSource.setDefaultTransactionIsolation( Connection.TRANSACTION_READ_UNCOMMITTED );
        dataSource.setDriverClassLoader( PolyPluginManager.getMainClassLoader() );
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
    public void updateColumnType( Context context, long allocId, LogicalColumn newCol ) {
        PhysicalColumn column = storeCatalog.updateColumnType( allocId, newCol );

        PhysicalTable physicalTable = storeCatalog.getTable( allocId );
        //List<CatalogPartitionPlacement> partitionPlacements = catalog.getPartitionPlacementsByTableOnAdapter( getAdapterId(), catalogColumn.tableId );

        //for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( physicalTable.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physicalTable.name ) );
        builder.append( " ALTER COLUMN " ).append( dialect.quoteIdentifier( column.name ) );
        builder.append( " TYPE " ).append( getTypeString( column.type ) );
        if ( column.collectionsType != null ) {
            builder.append( " " ).append( column.collectionsType );
        }
        if ( column.length != null && doesTypeUseLength( column.type ) ) {
            builder.append( "(" );
            builder.append( column.length );
            if ( column.scale != null ) {
                builder.append( "," ).append( column.scale );
            }
            builder.append( ")" );
        }
        builder.append( " USING " )
                .append( dialect.quoteIdentifier( column.name ) )
                .append( "::" )
                .append( getTypeString( column.type ) );

        if ( column.collectionsType != null ) {
            builder.append( " " ).append( column.collectionsType );
        }
        executeUpdate( builder, context );
        //}

    }


    @Override
    public String addIndex( Context context, LogicalIndex index, AllocationTable allocation ) {
        // List<CatalogPartitionPlacement> partitionPlacements = new ArrayList<>();
        // partitionIds.forEach( id -> partitionPlacements.add( catalog.getPartitionPlacement( getAdapterId(), id ) ) );

        String physicalIndexName = getPhysicalIndexName( index.key.tableId, index.id );
        PhysicalTable physical = storeCatalog.fromAllocation( allocation.id );

        //for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "CREATE " );
        if ( index.unique ) {
            builder.append( "UNIQUE INDEX " );
        } else {
                builder.append( "INDEX " );
            }

        builder.append( dialect.quoteIdentifier( physicalIndexName ) );//+ "_" + partitionPlacement.partitionId ) );
        builder.append( " ON " )
                .append( dialect.quoteIdentifier( physical.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physical.name ) );

            builder.append( " USING " );
        switch ( index.method ) {
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
                case "gist":
                    builder.append( "gist" );
                    break;
            }

            builder.append( "(" );
            boolean first = true;
        for ( long columnId : index.key.columnIds ) {
            if ( !first ) {
                builder.append( ", " );
            }
            first = false;
            builder.append( dialect.quoteIdentifier( getPhysicalColumnName( columnId ) ) ).append( " " );
        }
        builder.append( ")" );

        executeUpdate( builder, context );
        //}
        //Catalog.getInstance().setIndexPhysicalName( catalogIndex.id, physicalIndexName );
        return physicalIndexName;
    }


    @Override
    public void dropIndex( Context context, LogicalIndex index, long allocId ) {
        PhysicalTable table = storeCatalog.fromAllocation( allocId );
        //for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "DROP INDEX " );
        builder.append( dialect.quoteIdentifier( index.physicalName + "_" + table.id ) );
        executeUpdate( builder, context );
        // }
    }


    @Override
    public List<IndexMethodModel> getAvailableIndexMethods() {
        return ImmutableList.of(
                new IndexMethodModel( "btree", "B-TREE" ),
                new IndexMethodModel( "hash", "HASH" ),
                new IndexMethodModel( "gin", "GIN (Generalized Inverted Index)" ),
                new IndexMethodModel( "brin", "BRIN (Block Range index)" ),
                new IndexMethodModel( "gist", "GiST (Generalized Search Tree)" )
        );
    }


    @Override
    public IndexMethodModel getDefaultIndexMethod() {
        return getAvailableIndexMethods().get( 0 );
    }


    @Override
    public List<FunctionalIndexInfo> getFunctionalIndexes( LogicalTable catalogTable ) {
        return ImmutableList.of();
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        // TODO: Implement disconnect and reconnect to PostgreSQL instance.
    }


    /*@Override
    protected void createColumnDefinition( LogicalColumn catalogColumn, StringBuilder builder ) {
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
    }*/


    @Override
    protected String getTypeString( PolyType type ) {
        if ( type.getFamily() == PolyTypeFamily.MULTIMEDIA ) {
            return "BYTEA";
        }
        switch ( type ) {
            case BOOLEAN:
                return "BOOLEAN";
            case VARBINARY:
                return "BYTEA";
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
                return "VARCHAR";
            case GEOMETRY:
                return "GEOMETRY";
            case JSON:
                return "TEXT";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case ARRAY:
                return "ARRAY";
        }
        throw new GenericRuntimeException( "Unknown type: " + type.name() );
    }


    @Override
    public boolean doesTypeUseLength( PolyType type ) {
        switch ( type ) {
            case VARBINARY:
                return false;
        }
        return super.doesTypeUseLength( type );
    }


    @Override
    public String getDefaultPhysicalNamespaceName() {
        return "public";
    }


    private static String getConnectionUrl( final String dbHostname, final int dbPort, final String dbName ) {
        return String.format( "jdbc:postgresql://%s:%d/%s", dbHostname, dbPort, dbName );
    }


    private boolean testDockerConnection() {
        if ( container == null ) {
            return false;
        }

        HostAndPort hp = container.connectToContainer( 5432 );
        this.host = hp.getHost();
        this.port = hp.getPort();

        return testConnection();
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

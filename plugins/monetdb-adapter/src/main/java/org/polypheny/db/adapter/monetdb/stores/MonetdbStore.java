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

package org.polypheny.db.adapter.monetdb.stores;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
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
import org.polypheny.db.adapter.monetdb.MonetdbSqlDialect;
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
import org.polypheny.db.transaction.PUID;
import org.polypheny.db.transaction.PUID.Type;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.PasswordGenerator;


@Slf4j
@AdapterProperties(
        name = "MonetDB",
        description = "MonetDB is an execute-source column-oriented database management system. It is based on an optimistic concurrency control.",
        usedModes = { DeployMode.REMOTE, DeployMode.DOCKER },
        defaultMode = DeployMode.DOCKER)
@AdapterSettingString(name = "host", defaultValue = "localhost", description = "Hostname or IP address of the remote MonetDB instance.", position = 1, appliesTo = DeploySetting.REMOTE)
@AdapterSettingInteger(name = "port", defaultValue = 50000, description = "JDBC port number on the remote MonetDB instance.", position = 2, appliesTo = DeploySetting.REMOTE)
@AdapterSettingString(name = "database", defaultValue = "polypheny", description = "Name of the database to connect to.", position = 3, appliesTo = DeploySetting.REMOTE)
@AdapterSettingString(name = "username", defaultValue = "polypheny", description = "Username to be used for authenticating at the remote instance.", position = 4, appliesTo = DeploySetting.REMOTE)
@AdapterSettingString(name = "password", defaultValue = "polypheny", description = "Password to be used for authenticating at the remote instance.", appliesTo = DeploySetting.REMOTE)
@AdapterSettingInteger(name = "maxConnections", defaultValue = 25, description = "Maximum number of concurrent connections opened by Polypheny-DB to this data store.")
public class MonetdbStore extends AbstractJdbcStore {

    private String host;
    private int port;
    private String database;
    private String username;
    private DockerContainer container;


    public MonetdbStore( final long storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, MonetdbSqlDialect.DEFAULT, true );
    }


    @Override
    protected ConnectionFactory deployDocker( int dockerInstanceId ) {
        database = "monetdb";
        username = "monetdb";

        if ( settings.getOrDefault( "deploymentId", "" ).isEmpty() ) {
            if ( settings.getOrDefault( "password", "polypheny" ).equals( "polypheny" ) ) {
                settings.put( "password", PasswordGenerator.generatePassword() );
                updateSettings( settings );
            }

            DockerInstance instance = DockerManager.getInstance().getInstanceById( dockerInstanceId )
                    .orElseThrow( () -> new GenericRuntimeException( "No docker instance with id " + dockerInstanceId ) );
            try {
                this.container = instance.newBuilder( "polypheny/monet:latest", getUniqueName() )
                        .withEnvironmentVariable( "MONETDB_PASSWORD", settings.get( "password" ) )
                        .withEnvironmentVariable( "MONET_DATABASE", "monetdb" )
                        .createAndStart();
            } catch ( IOException e ) {
                throw new GenericRuntimeException( e );
            }

            if ( !container.waitTillStarted( this::testDockerConnection, 15000 ) ) {
                container.destroy();
                throw new GenericRuntimeException( "Failed to connect to monetdb container" );
            }

            deploymentId = container.getContainerId();
            settings.put( "deploymentId", deploymentId );
            updateSettings( settings );
        } else {
            deploymentId = settings.get( "deploymentId" );
            DockerManager.getInstance(); // Make sure docker instances are loaded.  Very hacky, but it works.
            container = DockerContainer.getContainerByUUID( deploymentId )
                    .orElseThrow( () -> new GenericRuntimeException( "Could not find docker container with id " + deploymentId ) );
            if ( !testDockerConnection() ) {
                throw new GenericRuntimeException( "Could not connect to container" );
            }
        }

        ConnectionFactory connectionFactory = createConnectionFactory();
        createDefaultSchema( connectionFactory );
        return connectionFactory;
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
            throw new GenericRuntimeException( "Exception while creating default schema on monetdb store!", e );
        }
    }


    private ConnectionFactory createConnectionFactory() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName( "nl.cwi.monetdb.jdbc.MonetDriver" );

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
    public void updateColumnType( Context context, long allocId, LogicalColumn newCol ) {
        if ( !this.dialect.supportsNestedArrays() && newCol.collectionsType != null ) {
            return;
        }
        PhysicalColumn column = adapterCatalog.updateColumnType( allocId, newCol );

        PhysicalTable table = adapterCatalog.fromAllocation( allocId );
        // MonetDB does not support updating the column type directly. We need to do a work-around

        String tmpColName = column.name + "tmp";
        StringBuilder builder;

        // (1) Create a temporary column `alter table tabX add column colXtemp NEW_TYPE;`
        builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( table.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( table.name ) );
        builder.append( " ADD COLUMN " )
                .append( dialect.quoteIdentifier( tmpColName ) )
                .append( " " )
                .append( getTypeString( column.type ) );
        executeUpdate( builder, context );

        // (2) Set data in temporary column to original data `update tabX set colXtemp=colX;`
        builder = new StringBuilder();
        builder.append( "UPDATE " )
                .append( dialect.quoteIdentifier( table.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( table.name ) );
        builder.append( " SET " )
                .append( dialect.quoteIdentifier( tmpColName ) )
                .append( "=" )
                .append( dialect.quoteIdentifier( column.name ) );
        executeUpdate( builder, context );

        // (3) Remove the original column `alter table tabX drop column colX;`
        builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( table.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( table.name ) );
        builder.append( " DROP COLUMN " )
                .append( dialect.quoteIdentifier( column.name ) );
        executeUpdate( builder, context );

        // (4) Re-create the original column with the new type `alter table tabX add column colX NEW_TYPE;
        builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( table.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( table.name ) );
        builder.append( " ADD COLUMN " )
                .append( dialect.quoteIdentifier( column.name ) )
                .append( " " )
                .append( getTypeString( column.type ) );
        executeUpdate( builder, context );

        // (5) Move data from temporary column to new column `update tabX set colX=colXtemp`;
        builder = new StringBuilder();
        builder.append( "UPDATE " )
                .append( dialect.quoteIdentifier( table.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( table.name ) );
        builder.append( " SET " )
                .append( dialect.quoteIdentifier( column.name ) )
                .append( "=" )
                .append( dialect.quoteIdentifier( tmpColName ) );
        executeUpdate( builder, context );

        // (6) Drop the temporary column `alter table tabX drop column colXtemp;`
        builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( table.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( table.name ) );
        builder.append( " DROP COLUMN " )
                .append( dialect.quoteIdentifier( tmpColName ) );

        executeUpdate( builder, context );

        updateNativePhysical( allocId );
    }


    @Override
    public String addIndex( Context context, LogicalIndex index, AllocationTable allocation ) {
        throw new GenericRuntimeException( "MonetDB adapter does not support adding indexes" );
    }


    @Override
    public void dropIndex( Context context, LogicalIndex index, long allocId ) {
        throw new GenericRuntimeException( "MonetDB adapter does not support dropping indexes" );
    }


    @Override
    public List<IndexMethodModel> getAvailableIndexMethods() {
        // According to the MonetDB documentation, MonetDB takes create index statements only as an advice and often freely
        // neglects them. Indexes are created and removed automatically. We therefore decided to not support manually creating
        // indexes on MonetDB.
        return ImmutableList.of();
    }


    @Override
    public IndexMethodModel getDefaultIndexMethod() {
        throw new GenericRuntimeException( "MonetDB adapter does not support adding indexes" );
    }


    @Override
    public List<FunctionalIndexInfo> getFunctionalIndexes( LogicalTable catalogTable ) {
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
        return switch ( type ) {
            case BOOLEAN -> "BOOLEAN";
            case VARBINARY -> "VARCHAR";//throw new GenericRuntimeException( "Unsupported datatype: " + type.name() );
            case TINYINT -> "SMALLINT"; // there seems to be an issue with tinyints and the jdbc driver
            case SMALLINT -> "SMALLINT";
            case INTEGER -> "INT";
            case BIGINT -> "BIGINT";
            case REAL -> "REAL";
            case DOUBLE -> "DOUBLE";
            case DECIMAL -> "DECIMAL";
            case VARCHAR -> "VARCHAR";
            case JSON, ARRAY, TEXT -> "TEXT";
            case DATE -> "DATE";
            case TIME -> "TIME";
            case TIMESTAMP -> "TIMESTAMP";
            default -> throw new GenericRuntimeException( "Unknown type: " + type.name() );
        };
    }


    @Override
    public String getDefaultPhysicalSchemaName() {
        return "public";
    }


    private static String getConnectionUrl( final String dbHostname, final int dbPort, final String dbName ) {
        return String.format( "jdbc:monetdb://%s:%d/%s", dbHostname, dbPort, dbName );
    }


    private boolean testDockerConnection() {
        if ( container == null ) {
            return false;
        }

        HostAndPort hp = container.connectToContainer( 50000 );
        this.host = hp.host();
        this.port = hp.port();

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

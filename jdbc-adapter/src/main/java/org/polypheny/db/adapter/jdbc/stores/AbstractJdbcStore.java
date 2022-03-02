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


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.jdbc.JdbcSchema;
import org.polypheny.db.adapter.jdbc.JdbcUtils;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.sql.sql.SqlDialect;
import org.polypheny.db.sql.sql.SqlLiteral;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;


@Slf4j
public abstract class AbstractJdbcStore extends DataStore {

    protected SqlDialect dialect;
    protected JdbcSchema currentJdbcSchema;

    protected ConnectionFactory connectionFactory;

    protected int dockerInstanceId;


    public AbstractJdbcStore(
            int storeId,
            String uniqueName,
            Map<String, String> settings,
            SqlDialect dialect,
            boolean persistent ) {
        super( storeId, uniqueName, settings, persistent );
        this.dialect = dialect;

        if ( deployMode == DeployMode.DOCKER ) {
            dockerInstanceId = Integer.parseInt( settings.get( "instanceId" ) );
            connectionFactory = deployDocker( dockerInstanceId );
        } else if ( deployMode == DeployMode.EMBEDDED ) {
            connectionFactory = deployEmbedded();
        } else if ( deployMode == DeployMode.REMOTE ) {
            connectionFactory = deployRemote();
        } else {
            throw new RuntimeException( "Unknown deploy mode: " + deployMode.name() );
        }

        // Register the JDBC Pool Size as information in the information manager and enable it
        registerJdbcInformation();

        // Create udfs
        createUdfs();
    }


    protected ConnectionFactory deployDocker( int dockerInstanceId ) {
        throw new UnsupportedOperationException();
    }


    protected ConnectionFactory deployEmbedded() {
        throw new UnsupportedOperationException();
    }


    protected ConnectionFactory deployRemote() {
        throw new UnsupportedOperationException();
    }


    protected void registerJdbcInformation() {
        JdbcUtils.addInformationPoolSize( informationPage, informationGroups, informationElements, connectionFactory, getUniqueName() );
        addInformationPhysicalNames();
        enableInformationPage();
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        currentJdbcSchema = JdbcSchema.create( rootSchema, name, connectionFactory, dialect, this );
    }


    public void createUdfs() {

    }


    protected abstract String getTypeString( PolyType polyType );


    @Override
    public void createTable( Context context, CatalogTable catalogTable, List<Long> partitionIds ) {
        List<String> qualifiedNames = new LinkedList<>();
        qualifiedNames.add( catalogTable.getSchemaName() );
        qualifiedNames.add( catalogTable.name );

        List<CatalogColumnPlacement> existingPlacements = catalog.getColumnPlacementsOnAdapterPerTable( getAdapterId(), catalogTable.id );

        // Remove the unpartitioned table name again, otherwise it would cause, table already exist due to create statement
        for ( long partitionId : partitionIds ) {
            String physicalTableName = getPhysicalTableName( catalogTable.id, partitionId );

            if ( log.isDebugEnabled() ) {
                log.debug( "[{}] createTable: Qualified names: {}, physicalTableName: {}", getUniqueName(), qualifiedNames, physicalTableName );
            }
            StringBuilder query = buildCreateTableQuery( getDefaultPhysicalSchemaName(), physicalTableName, catalogTable );
            if ( RuntimeConfig.DEBUG.getBoolean() ) {
                log.info( "{} on store {}", query.toString(), this.getUniqueName() );
            }
            executeUpdate( query, context );

            catalog.updatePartitionPlacementPhysicalNames(
                    getAdapterId(),
                    partitionId,
                    getDefaultPhysicalSchemaName(),
                    physicalTableName );

            for ( CatalogColumnPlacement placement : existingPlacements ) {
                catalog.updateColumnPlacementPhysicalNames(
                        getAdapterId(),
                        placement.columnId,
                        getDefaultPhysicalSchemaName(),
                        getPhysicalColumnName( placement.columnId ),
                        true );
            }
        }
    }


    protected StringBuilder buildCreateTableQuery( String schemaName, String physicalTableName, CatalogTable catalogTable ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "CREATE TABLE " )
                .append( dialect.quoteIdentifier( schemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physicalTableName ) )
                .append( " ( " );
        boolean first = true;
        for ( CatalogColumnPlacement placement : catalog.getColumnPlacementsOnAdapterPerTable( getAdapterId(), catalogTable.id ) ) {
            CatalogColumn catalogColumn = catalog.getColumn( placement.columnId );
            if ( !first ) {
                builder.append( ", " );
            }
            first = false;
            builder.append( dialect.quoteIdentifier( getPhysicalColumnName( placement.columnId ) ) ).append( " " );
            createColumnDefinition( catalogColumn, builder );
            builder.append( " NULL" );
        }
        builder.append( " )" );
        return builder;
    }


    @Override
    public void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        String physicalColumnName = getPhysicalColumnName( catalogColumn.id );
        for ( CatalogPartitionPlacement partitionPlacement : catalog.getPartitionPlacementsByTableOnAdapter( this.getAdapterId(), catalogTable.id ) ) {
            String physicalTableName = partitionPlacement.physicalTableName;
            String physicalSchemaName = partitionPlacement.physicalSchemaName;
            StringBuilder query = buildAddColumnQuery( physicalSchemaName, physicalTableName, physicalColumnName, catalogTable, catalogColumn );
            executeUpdate( query, context );
            // Insert default value
            if ( catalogColumn.defaultValue != null ) {
                query = buildInsertDefaultValueQuery( physicalSchemaName, physicalTableName, physicalColumnName, catalogColumn );
                executeUpdate( query, context );
            }
            // Add physical name to placement
            catalog.updateColumnPlacementPhysicalNames(
                    getAdapterId(),
                    catalogColumn.id,
                    physicalSchemaName,
                    physicalColumnName,
                    false );
        }
    }


    protected StringBuilder buildAddColumnQuery( String physicalSchemaName, String physicalTableName, String physicalColumnName, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physicalTableName ) );
        builder.append( " ADD " ).append( dialect.quoteIdentifier( physicalColumnName ) ).append( " " );
        createColumnDefinition( catalogColumn, builder );
        builder.append( " NULL" );
        return builder;
    }


    protected void createColumnDefinition( CatalogColumn catalogColumn, StringBuilder builder ) {
        if ( !this.dialect.supportsNestedArrays() && catalogColumn.collectionsType != null ) {
            // Returns e.g. TEXT if arrays are not supported
            builder.append( getTypeString( PolyType.ARRAY ) );
        } else {
            builder.append( " " ).append( getTypeString( catalogColumn.type ) );
            if ( catalogColumn.length != null ) {
                builder.append( "(" ).append( catalogColumn.length );
                if ( catalogColumn.scale != null ) {
                    builder.append( "," ).append( catalogColumn.scale );
                }
                builder.append( ")" );
            }
            if ( catalogColumn.collectionsType != null ) {
                builder.append( " " ).append( getTypeString( catalogColumn.collectionsType ) );
            }
        }
    }


    protected StringBuilder buildInsertDefaultValueQuery( String physicalSchemaName, String physicalTableName, String physicalColumnName, CatalogColumn catalogColumn ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "UPDATE " )
                .append( dialect.quoteIdentifier( physicalSchemaName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physicalTableName ) );
        builder.append( " SET " ).append( dialect.quoteIdentifier( physicalColumnName ) ).append( " = " );

        if ( catalogColumn.collectionsType == PolyType.ARRAY ) {
            throw new RuntimeException( "Default values are not supported for array types" );
        }

        SqlLiteral literal;
        switch ( catalogColumn.defaultValue.type ) {
            case BOOLEAN:
                literal = SqlLiteral.createBoolean( Boolean.parseBoolean( catalogColumn.defaultValue.value ), ParserPos.ZERO );
                break;
            case INTEGER:
            case DECIMAL:
            case BIGINT:
                literal = SqlLiteral.createExactNumeric( catalogColumn.defaultValue.value, ParserPos.ZERO );
                break;
            case REAL:
            case DOUBLE:
                literal = SqlLiteral.createApproxNumeric( catalogColumn.defaultValue.value, ParserPos.ZERO );
                break;
            case VARCHAR:
                literal = SqlLiteral.createCharString( catalogColumn.defaultValue.value, ParserPos.ZERO );
                break;
            default:
                throw new PolyphenyDbException( "Not yet supported default value type: " + catalogColumn.defaultValue.type );
        }
        builder.append( literal.toSqlString( dialect ) );
        return builder;
    }


    // Make sure to update overridden methods as well
    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn, PolyType oldType ) {
        if ( !this.dialect.supportsNestedArrays() && catalogColumn.collectionsType != null ) {
            return;
        }
        for ( CatalogPartitionPlacement partitionPlacement : catalog.getPartitionPlacementsByTableOnAdapter( columnPlacement.adapterId, columnPlacement.tableId ) ) {
            StringBuilder builder = new StringBuilder();
            builder.append( "ALTER TABLE " )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalSchemaName ) )
                    .append( "." )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalTableName ) );
            builder.append( " ALTER COLUMN " ).append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) );
            builder.append( " " ).append( getTypeString( catalogColumn.type ) );
            if ( catalogColumn.length != null ) {
                builder.append( "(" );
                builder.append( catalogColumn.length );
                if ( catalogColumn.scale != null ) {
                    builder.append( "," ).append( catalogColumn.scale );
                }
                builder.append( ")" );
            }
            executeUpdate( builder, context );
        }
    }


    @Override
    public void dropTable( Context context, CatalogTable catalogTable, List<Long> partitionIds ) {
        // We get the physical schema / table name by checking existing column placements of the same logical table placed on this store.
        // This works because there is only one physical table for each logical table on JDBC stores. The reason for choosing this
        // approach rather than using the default physical schema / table names is that this approach allows dropping linked tables.
        String physicalTableName;
        String physicalSchemaName;

        List<CatalogPartitionPlacement> partitionPlacements = new ArrayList<>();
        partitionIds.forEach( id -> partitionPlacements.add( catalog.getPartitionPlacement( getAdapterId(), id ) ) );

        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
            catalog.deletePartitionPlacement( getAdapterId(), partitionPlacement.partitionId );
            physicalSchemaName = partitionPlacement.physicalSchemaName;
            physicalTableName = partitionPlacement.physicalTableName;

            StringBuilder builder = new StringBuilder();

            builder.append( "DROP TABLE " )
                    .append( dialect.quoteIdentifier( physicalSchemaName ) )
                    .append( "." )
                    .append( dialect.quoteIdentifier( physicalTableName ) );

            if ( RuntimeConfig.DEBUG.getBoolean() ) {
                log.info( "{} from store {}", builder.toString(), this.getUniqueName() );
            }
            executeUpdate( builder, context );
        }
    }


    @Override
    public void dropColumn( Context context, CatalogColumnPlacement columnPlacement ) {
        for ( CatalogPartitionPlacement partitionPlacement : catalog.getPartitionPlacementsByTableOnAdapter( columnPlacement.adapterId, columnPlacement.tableId ) ) {
            StringBuilder builder = new StringBuilder();
            builder.append( "ALTER TABLE " )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalSchemaName ) )
                    .append( "." )
                    .append( dialect.quoteIdentifier( partitionPlacement.physicalTableName ) );
            builder.append( " DROP " ).append( dialect.quoteIdentifier( columnPlacement.physicalColumnName ) );
            executeUpdate( builder, context );
        }
    }


    @Override
    public void truncate( Context context, CatalogTable catalogTable ) {
        // We get the physical schema / table name by checking existing column placements of the same logical table placed on this store.
        // This works because there is only one physical table for each logical table on JDBC stores. The reason for choosing this
        // approach rather than using the default physical schema / table names is that this approach allows truncating linked tables.
        for ( CatalogPartitionPlacement partitionPlacement : catalog.getPartitionPlacementsByTableOnAdapter( getAdapterId(), catalogTable.id ) ) {
            String physicalTableName = partitionPlacement.physicalTableName;
            String physicalSchemaName = partitionPlacement.physicalSchemaName;
            StringBuilder builder = new StringBuilder();
            builder.append( "TRUNCATE TABLE " )
                    .append( dialect.quoteIdentifier( physicalSchemaName ) )
                    .append( "." )
                    .append( dialect.quoteIdentifier( physicalTableName ) );
            executeUpdate( builder, context );
        }
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
            log.warn( "There is no connection to prepare (Uniquename: {}, XID: {})! Returning true.", getUniqueName(), xid );
            return true;
        }
    }


    @SneakyThrows
    @Override
    public void commit( PolyXid xid ) {
        if ( connectionFactory.hasConnectionHandler( xid ) ) {
            connectionFactory.getConnectionHandler( xid ).commit();
        } else {
            log.warn( "There is no connection to commit (Uniquename: {}, XID: {})!", getUniqueName(), xid );
        }
    }


    @SneakyThrows
    @Override
    public void rollback( PolyXid xid ) {
        if ( connectionFactory.hasConnectionHandler( xid ) ) {
            connectionFactory.getConnectionHandler( xid ).rollback();
        } else {
            log.warn( "There is no connection to rollback (Uniquename: {}, XID: {})!", getUniqueName(), xid );
        }
    }


    @Override
    public void shutdown() {
        try {
            DockerInstance.getInstance().destroyAll( getAdapterId() );
            removeInformationPage();
            connectionFactory.close();
        } catch ( SQLException e ) {
            log.warn( "Exception while shutting down {}", getUniqueName(), e );
        }
    }


    protected String getPhysicalTableName( long tableId, long partitionId ) {
        String physicalTableName = "tab" + tableId;
        if ( partitionId >= 0 ) {
            physicalTableName += "_part" + partitionId;
        }
        return physicalTableName;
    }


    protected String getPhysicalColumnName( long columnId ) {
        return "col" + columnId;
    }


    protected String getPhysicalIndexName( long tableId, long indexId ) {
        return "idx" + tableId + "_" + indexId;
    }


    protected abstract String getDefaultPhysicalSchemaName();

}

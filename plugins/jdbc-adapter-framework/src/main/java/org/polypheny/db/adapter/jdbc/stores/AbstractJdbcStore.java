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

package org.polypheny.db.adapter.jdbc.stores;


import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.ExtensionPoint;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.jdbc.JdbcSchema;
import org.polypheny.db.adapter.jdbc.JdbcUtils;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.catalog.catalogs.RelStoreCatalog;
import org.polypheny.db.catalog.entity.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;


@Slf4j
public abstract class AbstractJdbcStore extends DataStore<RelStoreCatalog> implements ExtensionPoint {

    protected SqlDialect dialect;
    protected JdbcSchema currentJdbcSchema;

    protected ConnectionFactory connectionFactory;

    protected int dockerInstanceId;


    public AbstractJdbcStore(
            long storeId,
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
    public void createNewSchema( RelStoreCatalog snapshot, String name, long id ) {
        currentJdbcSchema = JdbcSchema.create( id, snapshot, name, connectionFactory, dialect, this );
    }


    public void createUdfs() {

    }


    protected abstract String getTypeString( PolyType polyType );


    @Override
    public void createTable( RelStoreCatalog snapshot, Context context, LogicalTable logical, List<LogicalColumn> lColumns, AllocationTable allocation, List<AllocationColumn> columns ) {
        String namespaceName = getDefaultPhysicalSchemaName();
        String tableName = getPhysicalTableName( allocation.id, 0 );

        if ( snapshot.getNamespace( allocation.namespaceId ) == null ) {
            createNewSchema( snapshot, namespaceName, allocation.namespaceId );
            snapshot.addNamespace( allocation.namespaceId, currentJdbcSchema );
        }

        PhysicalTable table = snapshot.createTable(
                namespaceName,
                tableName,
                columns.stream().collect( Collectors.toMap( c -> c.columnId, c -> getPhysicalColumnName( c.columnId ) ) ),
                logical,
                lColumns.stream().collect( Collectors.toMap( c -> c.id, c -> c ) ),
                allocation,
                columns );

        if ( log.isDebugEnabled() ) {
            log.debug( "[{}] createTable: Qualified names: {}, physicalTableName: {}", getUniqueName(), namespaceName, tableName );
        }
        StringBuilder query = buildCreateTableQuery( table );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            log.info( "{} on store {}", query.toString(), this.getUniqueName() );
        }
        executeUpdate( query, context );
    }


    @Override
    public void updateTable( RelStoreCatalog storeCatalog, Context context, long allocId ) {
        PhysicalTable template = storeCatalog.getTable( allocId );
        storeCatalog.addTable( this.currentJdbcSchema.createJdbcTable( storeCatalog, template ) );
    }


    protected StringBuilder buildCreateTableQuery( PhysicalTable table ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "CREATE TABLE " )
                .append( dialect.quoteIdentifier( table.name ) )
                .append( "." )
                .append( dialect.quoteIdentifier( table.namespaceName ) )
                .append( " ( " );
        boolean first = true;
        for ( PhysicalColumn column : table.columns ) {
            if ( !first ) {
                builder.append( ", " );
            }
            first = false;
            builder.append( dialect.quoteIdentifier( column.name ) ).append( " " );
            createColumnDefinition( column, builder );
            builder.append( " NULL" );
        }
        builder.append( " )" );
        return builder;
    }


    @Override
    public void addColumn( RelStoreCatalog snapshot, Context context, long allocId, LogicalColumn logicalColumn ) {
        String physicalColumnName = getPhysicalColumnName( logicalColumn.id );
        PhysicalTable table = snapshot.getTable( allocId );
        PhysicalColumn column = snapshot.addColumn( physicalColumnName, allocId, adapterId, table.columns.size(), logicalColumn );

        StringBuilder query = buildAddColumnQuery( table, column );
        executeUpdate( query, context );
        // Insert default value
        if ( column.defaultValue != null ) {
            query = buildInsertDefaultValueQuery( table, column );
            executeUpdate( query, context );
        }

    }


    protected StringBuilder buildAddColumnQuery( PhysicalTable table, PhysicalColumn column ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( table.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( table.name ) );
        builder.append( " ADD " ).append( dialect.quoteIdentifier( column.name ) ).append( " " );
        createColumnDefinition( column, builder );
        builder.append( " NULL" );
        return builder;
    }


    protected void createColumnDefinition( PhysicalColumn column, StringBuilder builder ) {
        if ( !this.dialect.supportsNestedArrays() && column.collectionsType == PolyType.ARRAY ) {
            // Returns e.g. TEXT if arrays are not supported
            builder.append( getTypeString( PolyType.ARRAY ) );
        } else if ( column.collectionsType == PolyType.MAP ) {
            builder.append( getTypeString( PolyType.ARRAY ) );
        } else {
            builder.append( " " ).append( getTypeString( column.type ) );
            if ( column.length != null ) {
                builder.append( "(" ).append( column.length );
                if ( column.scale != null ) {
                    builder.append( "," ).append( column.scale );
                }
                builder.append( ")" );
            }
            if ( column.collectionsType != null ) {
                builder.append( " " ).append( getTypeString( column.collectionsType ) );
            }
        }
    }


    protected StringBuilder buildInsertDefaultValueQuery( PhysicalTable table, PhysicalColumn column ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "UPDATE " )
                .append( dialect.quoteIdentifier( table.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( table.name ) );
        builder.append( " SET " ).append( dialect.quoteIdentifier( column.name ) ).append( " = " );

        if ( column.collectionsType == PolyType.ARRAY ) {
            throw new RuntimeException( "Default values are not supported for array types" );
        }

        SqlLiteral literal;
        switch ( column.defaultValue.type ) {
            case BOOLEAN:
                literal = SqlLiteral.createBoolean( Boolean.parseBoolean( column.defaultValue.value ), ParserPos.ZERO );
                break;
            case INTEGER:
            case DECIMAL:
            case BIGINT:
                literal = SqlLiteral.createExactNumeric( column.defaultValue.value, ParserPos.ZERO );
                break;
            case REAL:
            case DOUBLE:
                literal = SqlLiteral.createApproxNumeric( column.defaultValue.value, ParserPos.ZERO );
                break;
            case VARCHAR:
                literal = SqlLiteral.createCharString( column.defaultValue.value, ParserPos.ZERO );
                break;
            default:
                throw new PolyphenyDbException( "Not yet supported default value type: " + column.defaultValue.type );
        }
        builder.append( literal.toSqlString( dialect ) );
        return builder;
    }


    // Make sure to update overridden methods as well
    @Override
    public void updateColumnType( RelStoreCatalog snapshot, Context context, long allocId, LogicalColumn newCol ) {
        PhysicalColumn column = snapshot.updateColumnType( allocId, newCol );

        if ( !this.dialect.supportsNestedArrays() && column.collectionsType != null ) {
            return;
        }
        PhysicalTable physicalTable = snapshot.getTable( allocId );

        StringBuilder builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( physicalTable.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physicalTable.name ) );
        builder.append( " ALTER COLUMN " ).append( dialect.quoteIdentifier( column.name ) );
        builder.append( " " ).append( getTypeString( column.type ) );
        if ( column.length != null ) {
            builder.append( "(" );
            builder.append( column.length );
            if ( column.scale != null ) {
                builder.append( "," ).append( column.scale );
            }
            builder.append( ")" );
        }
        executeUpdate( builder, context );

    }


    @Override
    public void dropTable( RelStoreCatalog snapshot, Context context, long allocId ) {
        // We get the physical schema / table name by checking existing column placements of the same logical table placed on this store.
        // This works because there is only one physical table for each logical table on JDBC stores. The reason for choosing this
        // approach rather than using the default physical schema / table names is that this approach allows dropping linked tables.

        //List<CatalogPartitionPlacement> partitionPlacements = new ArrayList<>();
        //partitionIds.forEach( id -> partitionPlacements.add( context.getSnapshot().alloc().getPartitionPlacement( getAdapterId(), id ) ) );

        //for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
        // catalog.getAllocRel( catalogTable.namespaceId ).deletePartitionPlacement( getAdapterId(), partitionPlacement.partitionId );
        // physicalSchemaName = partitionPlacement.physicalSchemaName;
        // physicalTableName = partitionPlacement.physicalTableName;
        PhysicalTable physical = snapshot.getTable( allocId );
        StringBuilder builder = new StringBuilder();

        builder.append( "DROP TABLE " )
                .append( dialect.quoteIdentifier( physical.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physical.name ) );

        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            log.info( "{} from store {}", builder, this.getUniqueName() );
        }
        executeUpdate( builder, context );
        // }
    }


    @Override
    public void dropColumn( RelStoreCatalog snapshot, Context context, long allocId, long columnId ) {
        //for ( CatalogPartitionPlacement partitionPlacement : context.getSnapshot().alloc().getAllocation( columnPlacement.tableId ) ) {
        PhysicalTable table = snapshot.getTable( allocId );
        PhysicalColumn column = snapshot.getColumn( columnId );
        StringBuilder builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( table.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( table.name ) );
        builder.append( " DROP " ).append( dialect.quoteIdentifier( column.name ) );
        executeUpdate( builder, context );

        //}
    }


    @Override
    public void truncate( RelStoreCatalog snapshot, Context context, long allocId ) {
        // We get the physical schema / table name by checking existing column placements of the same logical table placed on this store.
        // This works because there is only one physical table for each logical table on JDBC stores. The reason for choosing this
        // approach rather than using the default physical schema / table names is that this approach allows truncating linked tables.
        PhysicalTable physical = snapshot.getTable( allocId );

        StringBuilder builder = new StringBuilder();
        builder.append( "TRUNCATE TABLE " )
                .append( dialect.quoteIdentifier( physical.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physical.name ) );
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


    public String getPhysicalTableName( long tableId, long partitionId ) {
        String physicalTableName = "tab" + tableId;
        if ( partitionId >= 0 ) {
            physicalTableName += "_part" + partitionId;
        }
        return physicalTableName;
    }


    public String getPhysicalColumnName( long columnId ) {
        return "col" + columnId;
    }


    protected String getPhysicalIndexName( long tableId, long indexId ) {
        return "idx" + tableId + "_" + indexId;
    }


    public abstract String getDefaultPhysicalSchemaName();

}

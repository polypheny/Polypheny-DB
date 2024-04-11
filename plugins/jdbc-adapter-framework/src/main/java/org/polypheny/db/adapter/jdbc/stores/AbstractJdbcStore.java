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

package org.polypheny.db.adapter.jdbc.stores;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.ExtensionPoint;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalModifyDelegate;
import org.polypheny.db.adapter.jdbc.JdbcSchema;
import org.polypheny.db.adapter.jdbc.JdbcTable;
import org.polypheny.db.adapter.jdbc.JdbcUtils;
import org.polypheny.db.adapter.jdbc.connection.ConnectionFactory;
import org.polypheny.db.adapter.jdbc.connection.ConnectionHandlerException;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.DockerContainer;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;


@Slf4j
public abstract class AbstractJdbcStore extends DataStore<RelAdapterCatalog> implements ExtensionPoint {

    @Delegate(excludes = Exclude.class)
    private final RelationalModifyDelegate delegate;

    protected SqlDialect dialect;
    protected JdbcSchema currentJdbcSchema;

    protected ConnectionFactory connectionFactory;

    protected int dockerInstanceId;


    public AbstractJdbcStore(
            final long storeId,
            final String uniqueName,
            final Map<String, String> settings,
            final SqlDialect dialect,
            final boolean persistent ) {
        super( storeId, uniqueName, settings, persistent, new RelAdapterCatalog( storeId ) );
        this.dialect = dialect;

        if ( deployMode == DeployMode.DOCKER ) {
            dockerInstanceId = Integer.parseInt( settings.get( "instanceId" ) );
            connectionFactory = deployDocker( dockerInstanceId );
        } else if ( deployMode == DeployMode.EMBEDDED ) {
            connectionFactory = deployEmbedded();
        } else if ( deployMode == DeployMode.REMOTE ) {
            connectionFactory = deployRemote();
        } else {
            throw new GenericRuntimeException( "Unknown deploy mode: " + deployMode.name() );
        }

        // Register the JDBC Pool Size as information in the information manager and enable it
        registerJdbcInformation();

        // Create udfs
        createUdfs();

        this.delegate = new RelationalModifyDelegate( this, adapterCatalog );
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
    public void updateNamespace( String name, long id ) {
        if ( adapterCatalog.getNamespace( id ) == null ) {
            currentJdbcSchema = JdbcSchema.create( id, getDefaultPhysicalSchemaName(), connectionFactory, dialect, this );
            adapterCatalog.addNamespace( id, currentJdbcSchema );
        }
        putNamespace( currentJdbcSchema );
    }


    public void createUdfs() {

    }


    @Override
    public Namespace getCurrentNamespace() {
        return currentJdbcSchema;
    }


    protected abstract String getTypeString( PolyType polyType );


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocationWrapper ) {
        AllocationTable allocation = allocationWrapper.table;
        String namespaceName = getDefaultPhysicalSchemaName();
        String tableName = getPhysicalTableName( allocation.id );

        updateNamespace( logical.table.getNamespaceName(), logical.table.namespaceId );

        PhysicalTable table = adapterCatalog.createTable(
                namespaceName,
                tableName,
                allocationWrapper.columns.stream().collect( Collectors.toMap( c -> c.columnId, c -> getPhysicalColumnName( c.columnId ) ) ),
                logical.table,
                logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c ) ),
                logical.pkIds, allocationWrapper );

        executeCreateTable( context, table, logical.pkIds );

        JdbcTable physical = this.currentJdbcSchema.createJdbcTable( table );
        adapterCatalog.replacePhysical( physical );
        return List.of( physical );
    }


    public void executeCreateTable( Context context, PhysicalTable table, List<Long> pkIds ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "[{}] createTable: Qualified names: {}, physicalTableName: {}", getUniqueName(), table.namespaceName, table.name );
        }
        StringBuilder query = buildCreateTableQuery( table, pkIds );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            log.info( "{} on store {}", query.toString(), this.getUniqueName() );
        }
        executeUpdate( query, context );
    }


    protected StringBuilder buildCreateTableQuery( PhysicalTable table, List<Long> pkIds ) {
        StringBuilder builder = new StringBuilder();
        builder.append( "CREATE TABLE " )
                .append( dialect.quoteIdentifier( table.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( table.name ) )
                .append( " ( " );
        boolean first = true;
        List<String> pkNames = new ArrayList<>();
        for ( PhysicalColumn column : table.columns ) {
            if ( !first ) {
                builder.append( ", " );
            }
            first = false;
            String name = dialect.quoteIdentifier( column.name );
            if ( pkIds.contains( column.id ) ) {
                pkNames.add( name );
            }

            builder.append( name ).append( " " );
            createColumnDefinition( column, builder );
            builder.append( " NULL" );
        }

        attachPrimaryKey( pkNames, builder );
        builder.append( " )" );
        return builder;
    }


    public void attachPrimaryKey( List<String> pkNames, StringBuilder builder ) {
        // empty on purpose
    }


    @Override
    public void addColumn( Context context, long allocId, LogicalColumn logicalColumn ) {
        String physicalColumnName = getPhysicalColumnName( logicalColumn.id );
        PhysicalTable table = adapterCatalog.fromAllocation( allocId );
        int max = adapterCatalog.getColumns( allocId ).stream().max( Comparator.comparingInt( a -> a.position ) ).orElseThrow().position;
        PhysicalColumn column = adapterCatalog.addColumn( physicalColumnName, allocId, max + 1, logicalColumn );

        StringBuilder query = buildAddColumnQuery( table, column );
        executeUpdate( query, context );
        // Insert default value
        if ( column.defaultValue != null ) {
            query = buildInsertDefaultValueQuery( table, column );
            executeUpdate( query, context );
        }

        updateNativePhysical( allocId );

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
        boolean supportsThisArray = column.collectionsType == PolyType.ARRAY && column.dimension != null && this.dialect.supportsArrays() && (this.dialect.supportsNestedArrays() || column.dimension == 1);
        if ( supportsThisArray ) {
            // Returns e.g. TEXT if arrays are not supported
            builder.append( getTypeString( column.type ) ).append( " " ).append( getTypeString( PolyType.ARRAY ).repeat( column.dimension ) );
        } else if ( column.collectionsType == PolyType.MAP ) {
            builder.append( getTypeString( PolyType.ARRAY ) );
        } else {
            PolyType type = column.dimension != null ? PolyType.TEXT : column.type; // nested array was not supported
            PolyType collectionsType = column.collectionsType == PolyType.ARRAY ? null : column.collectionsType; // nested array was not suppored

            builder.append( " " ).append( getTypeString( type ) );
            if ( column.length != null && doesTypeUseLength( type ) ) {
                builder.append( "(" ).append( column.length );
                if ( column.scale != null ) {
                    builder.append( "," ).append( column.scale );
                }
                builder.append( ")" );
            }
            if ( collectionsType != null ) {
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
            throw new GenericRuntimeException( "Default values are not supported for array types" );
        }

        SqlLiteral literal = switch ( Objects.requireNonNull( column.defaultValue ).type ) {
            case BOOLEAN -> SqlLiteral.createBoolean( Boolean.parseBoolean( column.defaultValue.value.toJson() ), ParserPos.ZERO );
            case INTEGER, DECIMAL, BIGINT -> SqlLiteral.createExactNumeric( column.defaultValue.value.toJson(), ParserPos.ZERO );
            case REAL, DOUBLE -> SqlLiteral.createApproxNumeric( column.defaultValue.value.toJson(), ParserPos.ZERO );
            case VARCHAR -> SqlLiteral.createCharString( column.defaultValue.value.toJson(), ParserPos.ZERO );
            default -> throw new PolyphenyDbException( "Not yet supported default value type: " + column.defaultValue.type );
        };
        builder.append( literal.toSqlString( dialect ) );
        return builder;
    }


    // Make sure to update overridden methods as well
    @Override
    public void updateColumnType( Context context, long allocId, LogicalColumn newCol ) {
        PhysicalColumn column = adapterCatalog.updateColumnType( allocId, newCol );

        if ( !this.dialect.supportsNestedArrays() && column.collectionsType != null ) {
            return;
        }
        PhysicalTable physicalTable = adapterCatalog.fromAllocation( allocId );

        StringBuilder builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( physicalTable.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( physicalTable.name ) );
        builder.append( " ALTER COLUMN " ).append( dialect.quoteIdentifier( column.name ) );
        builder.append( " " ).append( getTypeString( column.type ) );
        if ( column.length != null && doesTypeUseLength( column.type ) ) {
            builder.append( "(" );
            builder.append( column.length );
            if ( column.scale != null ) {
                builder.append( "," ).append( column.scale );
            }
            builder.append( ")" );
        }
        executeUpdate( builder, context );

        updateNativePhysical( allocId );

    }


    public boolean doesTypeUseLength( PolyType type ) {
        return type != PolyType.TEXT;
    }


    @Override
    public void dropTable( Context context, long allocId ) {
        PhysicalTable table = adapterCatalog.fromAllocation( allocId );
        StringBuilder builder = new StringBuilder();

        builder.append( "DROP TABLE " )
                .append( dialect.quoteIdentifier( table.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( table.name ) );

        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            log.info( "{} from store {}", builder, this.getUniqueName() );
        }
        executeUpdate( builder, context );
        adapterCatalog.removeAllocAndPhysical( allocId );
    }


    @Override
    public void renameLogicalColumn( long id, String newColumnName ) {
        adapterCatalog.renameLogicalColumn( id, newColumnName );
        adapterCatalog.fields.values().stream().filter( c -> c.id == id ).forEach( c -> updateNativePhysical( c.allocId ) );
    }


    @Override
    public void dropColumn( Context context, long allocId, long columnId ) {
        PhysicalTable table = adapterCatalog.fromAllocation( allocId );
        PhysicalColumn column = adapterCatalog.getColumn( columnId, allocId );
        StringBuilder builder = new StringBuilder();
        builder.append( "ALTER TABLE " )
                .append( dialect.quoteIdentifier( table.namespaceName ) )
                .append( "." )
                .append( dialect.quoteIdentifier( table.name ) );
        builder.append( " DROP " ).append( dialect.quoteIdentifier( column.name ) );
        executeUpdate( builder, context );
        adapterCatalog.dropColumn( allocId, columnId );

        updateNativePhysical( allocId );
    }


    protected void updateNativePhysical( long allocId ) {
        PhysicalTable table = adapterCatalog.fromAllocation( allocId );
        adapterCatalog.replacePhysical( this.currentJdbcSchema.createJdbcTable( table ) );
    }


    @Override
    public void truncate( Context context, long allocId ) {
        PhysicalTable physical = adapterCatalog.fromAllocation( allocId );

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
            throw new GenericRuntimeException( e );
        }
    }


    @Override
    public void restoreTable( AllocationTable alloc, List<PhysicalEntity> entities, Context context ) {
        for ( PhysicalEntity entity : entities ) {
            PhysicalTable table = entity.unwrap( PhysicalTable.class ).orElseThrow();
            if ( !isPersistent() ) {
                executeCreateTable( context, table, table.uniqueFieldIds );
            }

            updateNamespace( table.namespaceName, table.namespaceId );
            adapterCatalog.addPhysical( alloc, currentJdbcSchema.createJdbcTable( table.unwrap( PhysicalTable.class ).orElseThrow() ) );
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
            if ( deployMode == DeployMode.DOCKER ) {
                // This call is supposed to destroy all containers belonging to this adapterId
                DockerContainer.getContainerByUUID( deploymentId ).ifPresent( DockerContainer::destroy );
            }
            removeInformationPage();
            connectionFactory.close();
        } catch ( SQLException e ) {
            log.warn( "Exception while shutting down {}", getUniqueName(), e );
        }
    }


    public String getPhysicalTableName( long tableId ) {
        return "tab" + tableId;
    }


    public String getPhysicalColumnName( long columnId ) {
        return "col" + columnId;
    }


    protected String getPhysicalIndexName( long physicalId, long indexId ) {
        return "idx" + physicalId + "_" + indexId;
    }


    public abstract String getDefaultPhysicalSchemaName();


    @SuppressWarnings("unused")
    public interface Exclude {

        void dropColumn( Context context, long allocId, long columnId );

        void dropTable( Context context, long allocId );

        void updateColumnType( Context context, long allocId, LogicalColumn newCol );

        void addColumn( Context context, long allocId, LogicalColumn logicalColumn );

        void refreshTable( long allocId );

        void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocationWrapper );

        void restoreTable( AllocationTable alloc, List<PhysicalEntity> entities );

        void renameLogicalColumn( long id, String newName );

    }

}

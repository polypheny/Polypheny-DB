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

package org.polypheny.db.adapter.cottontail;

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.Expose;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalModifyDelegate;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.cottontail.util.CottontailNameUtil;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.vitrivr.cottontail.CottontailKt;
import org.vitrivr.cottontail.client.iterators.TupleIterator;
import org.vitrivr.cottontail.config.CacheConfig;
import org.vitrivr.cottontail.config.Config;
import org.vitrivr.cottontail.config.ExecutionConfig;
import org.vitrivr.cottontail.config.MapDBConfig;
import org.vitrivr.cottontail.config.ServerConfig;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnDefinition;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.CreateEntityMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.CreateIndexMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.DropEntityMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.DropIndexMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Engine;
import org.vitrivr.cottontail.grpc.CottontailGrpc.EntityDefinition;
import org.vitrivr.cottontail.grpc.CottontailGrpc.EntityName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.From;
import org.vitrivr.cottontail.grpc.CottontailGrpc.IndexDefinition;
import org.vitrivr.cottontail.grpc.CottontailGrpc.IndexName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.IndexType;
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage.InsertElement;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Literal;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Metadata;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Query;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Scan;
import org.vitrivr.cottontail.grpc.CottontailGrpc.SchemaName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.TruncateEntityMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Type;
import org.vitrivr.cottontail.server.grpc.CottontailGrpcServer;

@Slf4j
@AdapterProperties(
        name = "Cottontail-DB",
        description = "Cottontail-DB is a column store aimed at multimedia retrieval. It is optimized for classical boolean as well as vector-space retrieval.",
        usedModes = { DeployMode.EMBEDDED, DeployMode.REMOTE },
        defaultMode = DeployMode.EMBEDDED)
@AdapterSettingString(name = "host", defaultValue = "localhost", position = 1)
@AdapterSettingInteger(name = "port", defaultValue = 1865, position = 2)
@AdapterSettingString(name = "database", defaultValue = "cottontail", position = 3)
@AdapterSettingList(name = "engine", options = { "MAPDB", "HARE" }, defaultValue = "MAPDB", position = 4)
public class CottontailStore extends DataStore<RelAdapterCatalog> {

    @Delegate(excludes = Exclude.class)
    private final RelationalModifyDelegate delegate;

    public static final String DEFAULT_DATABASE = "public";

    // Running embedded
    private final boolean isEmbedded;
    @Expose(serialize = false, deserialize = false)
    private transient final CottontailGrpcServer embeddedServer;

    private final String dbHostname;
    private final int dbPort;
    private final String dbName;
    private final Engine engine;

    @Getter
    private CottontailNamespace currentNamespace;
    @Expose(serialize = false, deserialize = false)
    private final transient CottontailWrapper wrapper;


    public CottontailStore( long storeId, String uniqueName, Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true, new RelAdapterCatalog( storeId ) );

        this.dbName = settings.get( "database" );
        this.isEmbedded = settings.get( "mode" ).equalsIgnoreCase( "embedded" );
        this.dbPort = Integer.parseInt( settings.get( "port" ) );

        engine = Engine.valueOf( settings.get( "engine" ).trim() );

        if ( this.isEmbedded ) {
            PolyphenyHomeDirManager fileSystemManager = PolyphenyHomeDirManager.getInstance();
            File adapterRoot = fileSystemManager.registerNewFolder( "data/cottontaildb-store" );

            File embeddedDir = fileSystemManager.registerNewFolder( adapterRoot, "store" + getAdapterId() );

            final File dataFolder = fileSystemManager.registerNewFolder( embeddedDir, "data" );

            final Config config = new Config(
                    Paths.get( dataFolder.getAbsolutePath() ),
                    false,
                    false,
                    null,
                    new MapDBConfig( true, true, 22, 1000L ),
                    new ServerConfig( dbPort, null, null ),
                    new ExecutionConfig(),
                    new CacheConfig()
            );

            this.embeddedServer = CottontailKt.embedded( config );
            this.dbHostname = "localhost";
        } else {
            this.embeddedServer = null;
            this.dbHostname = settings.get( "host" );
        }

        addInformationPhysicalNames();
        enableInformationPage();

        final ManagedChannel channel = NettyChannelBuilder.forAddress( this.dbHostname, this.dbPort ).usePlaintext().build();
        this.wrapper = new CottontailWrapper( channel, this );
        this.wrapper.checkedCreateSchemaBlocking(
                CottontailGrpc.CreateSchemaMessage.newBuilder().setSchema( SchemaName.newBuilder().setName( this.dbName ) ).build() );

        this.delegate = new RelationalModifyDelegate( this, adapterCatalog );
    }


    @Override
    public void updateNamespace( String name, long id ) {
        this.currentNamespace = CottontailNamespace.create( id, name, this.wrapper, this );
    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocationWrapper ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );

        final String physicalTableName = CottontailNameUtil.createPhysicalTableName( allocationWrapper.table.id, 0 );

        if ( this.currentNamespace == null ) {
            updateNamespace( DEFAULT_DATABASE, allocationWrapper.table.id );
            adapterCatalog.addNamespace( allocationWrapper.table.namespaceId, currentNamespace );
        }

        PhysicalTable table = adapterCatalog.createTable(
                logical.table.getNamespaceName(),
                physicalTableName,
                allocationWrapper.columns.stream().collect( Collectors.toMap( c -> c.columnId, c -> CottontailNameUtil.createPhysicalColumnName( c.columnId ) ) ),
                logical.table,
                logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c ) ),
                logical.pkIds,
                allocationWrapper );

        /* Prepare CREATE TABLE message. */
        final List<ColumnDefinition> columns = this.buildColumnDefinitions( table.columns );

        final EntityName tableEntity = EntityName.newBuilder()
                .setSchema( this.currentNamespace.getCottontailSchema() )
                .setName( physicalTableName )
                .build();
        final EntityDefinition definition = EntityDefinition.newBuilder()
                .setEntity( tableEntity )
                .addAllColumns( columns )
                .build();

        CreateEntityMessage createEntityMessage = CreateEntityMessage.newBuilder()
                .setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() )
                .setDefinition( definition ).build();
        boolean success = this.wrapper.createEntityBlocking( createEntityMessage );
        if ( !success ) {
            throw new GenericRuntimeException( "Unable to create table." );
        }

        // adapterCatalog.replacePhysical( new CottontailEntity( currentNamespace, this.currentNamespace.getCottontailSchema().getName(), table, this ) );

        updateNativePhysical( table.allocationId );

        return List.of( table );
    }


    private List<ColumnDefinition> buildColumnDefinitions( List<PhysicalColumn> lColumns ) {
        final List<ColumnDefinition> columns = new ArrayList<>();

        for ( PhysicalColumn column : lColumns ) {
            final ColumnDefinition.Builder columnBuilder = ColumnDefinition.newBuilder();
            columnBuilder.setName( ColumnName.newBuilder().setName( CottontailNameUtil.createPhysicalColumnName( column.id ) ) );
            final CottontailGrpc.Type columnType = CottontailTypeUtil.getPhysicalTypeRepresentation(
                    column.type,
                    column.collectionsType,
                    (column.dimension != null) ? column.dimension : 0 );
            columnBuilder.setType( columnType );
            if ( column.dimension != null && column.dimension == 1 && columnType.getNumber() != Type.STRING.getNumber() ) {
                columnBuilder.setLength( column.cardinality );
            }
            columnBuilder.setNullable( column.nullable );
            columnBuilder.setEngine( engine );
            columns.add( columnBuilder.build() );
        }

        return columns;
    }


    @Override
    public void dropTable( Context context, long allocId ) {
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );


        /* Prepare DROP TABLE message. */
        PhysicalTable table = adapterCatalog.fromAllocation( allocId );
        final EntityName tableEntity = EntityName.newBuilder()
                .setSchema( this.currentNamespace.getCottontailSchema() )
                .setName( table.name )
                .build();

        this.wrapper.dropEntityBlocking( DropEntityMessage.newBuilder()
                .setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() )
                .setEntity( tableEntity ).build() );

        adapterCatalog.removeAllocAndPhysical( allocId );
    }


    @Override
    public void addColumn( Context context, long allocId, LogicalColumn logicalColumn ) {
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );
        final String newPhysicalColumnName = CottontailNameUtil.createPhysicalColumnName( logicalColumn.id );
        PhysicalTable table = adapterCatalog.fromAllocation( allocId );
        PhysicalColumn column = adapterCatalog.addColumn( newPhysicalColumnName, allocId, table.columns.size() - 1, logicalColumn );

        List<PhysicalColumn> pColumns = new ArrayList<>( adapterCatalog.getColumns( allocId ) );
        final List<ColumnDefinition> columns = this.buildColumnDefinitions( pColumns );
        //Since only one partition is available
        final String currentPhysicalTableName = table.name;

        final String newPhysicalTableName = CottontailNameUtil.incrementNameRevision( currentPhysicalTableName );

        final EntityName tableEntity = EntityName.newBuilder()
                .setSchema( this.currentNamespace.getCottontailSchema() )
                .setName( currentPhysicalTableName )
                .build();
        final EntityName newTableEntity = EntityName.newBuilder()
                .setSchema( this.currentNamespace.getCottontailSchema() )
                .setName( newPhysicalTableName )
                .build();

        final CreateEntityMessage message = CreateEntityMessage.newBuilder()
                .setMetadata( Metadata.newBuilder().setTransactionId( txId ) )
                .setDefinition( EntityDefinition.newBuilder()
                        .setEntity( newTableEntity )
                        .addAllColumns( columns ) ).build();

        if ( !this.wrapper.createEntityBlocking( message ) ) {
            throw new GenericRuntimeException( "Unable to create table." );
        }

        PolyType actualDefaultType;
        PolyValue defaultValue;
        if ( column.defaultValue != null ) {
            actualDefaultType = (column.collectionsType != null)
                    ? column.collectionsType
                    : column.type;
            defaultValue = column.defaultValue.value;
        } else {
            defaultValue = null;
            actualDefaultType = null;
        }
        final CottontailGrpc.Literal defaultData = CottontailTypeUtil.toData( defaultValue, actualDefaultType, null );
        final QueryMessage query = QueryMessage.newBuilder()
                .setMetadata( Metadata.newBuilder().setTransactionId( txId ) )
                .setQuery( Query.newBuilder().setFrom( From.newBuilder().setScan( Scan.newBuilder().setEntity( tableEntity ) ) ) )
                .build();
        final TupleIterator iterator = this.wrapper.query( query );
        final List<String> columnNames = iterator.getSimpleNames();

        iterator.forEachRemaining( t -> {
            final InsertMessage.Builder insert = InsertMessage.newBuilder()
                    .setMetadata( Metadata.newBuilder().setTransactionId( txId ) )
                    .setFrom( From.newBuilder().setScan( Scan.newBuilder().setEntity( newTableEntity ) ) );
            int i = 0;
            for ( Literal literal : t.getRaw().getDataList() ) {
                insert.addElementsBuilder()
                        .setColumn( ColumnName.newBuilder().setName( columnNames.get( i++ ) ) )
                        .setValue( literal );
            }
            insert.addElementsBuilder()
                    .setColumn( ColumnName.newBuilder().setName( newPhysicalColumnName ).build() )
                    .setValue( defaultData );
            if ( !this.wrapper.insert( insert.build() ) ) {
                throw new GenericRuntimeException( "Unable to migrate data." );
            }
        } );
        // Delete old table
        this.wrapper.dropEntityBlocking( DropEntityMessage.newBuilder().setMetadata( Metadata.newBuilder().setTransactionId( txId ) ).setEntity( tableEntity ).build() );

        // Update column placement physical table names
        adapterCatalog.replacePhysical( new PhysicalTable( table.id, table.allocationId, table.logicalId, newPhysicalTableName, pColumns, table.namespaceId, table.namespaceName, table.uniqueFieldIds, table.adapterId ) );

        updateNativePhysical( allocId );
    }


    protected void updateNativePhysical( long allocId ) {
        PhysicalTable table = adapterCatalog.fromAllocation( allocId );
        adapterCatalog.replacePhysical( new CottontailEntity( currentNamespace, this.currentNamespace.getCottontailSchema().getName(), table, this ) );
    }


    @Override
    public void dropColumn( Context context, long allocId, long columnId ) {
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );

        PhysicalTable table = adapterCatalog.fromAllocation( allocId );
        PhysicalColumn column = adapterCatalog.getColumn( columnId, allocId );
        adapterCatalog.dropColumn( allocId, columnId );
        List<PhysicalColumn> pColumns = adapterCatalog.getColumns( allocId ).stream().filter( c -> c.id != columnId ).sorted( Comparator.comparingInt( a -> a.position ) ).toList();
        final List<ColumnDefinition> columns = this.buildColumnDefinitions( pColumns );

        final String currentPhysicalTableName = table.name;
        final String newPhysicalTableName = CottontailNameUtil.incrementNameRevision( currentPhysicalTableName );
        final String oldPhysicalColumnName = column.name;

        final EntityName tableEntity = EntityName.newBuilder()
                .setSchema( this.currentNamespace.getCottontailSchema() )
                .setName( currentPhysicalTableName )
                .build();
        final EntityName newTableEntity = EntityName.newBuilder()
                .setSchema( this.currentNamespace.getCottontailSchema() )
                .setName( newPhysicalTableName )
                .build();

        final CreateEntityMessage message = CreateEntityMessage.newBuilder()
                .setMetadata( Metadata.newBuilder().setTransactionId( txId ) )
                .setDefinition( EntityDefinition.newBuilder().setEntity( newTableEntity ).addAllColumns( columns ) )
                .build();

        if ( !this.wrapper.createEntityBlocking( message ) ) {
            throw new GenericRuntimeException( "Unable to create table." );
        }
        final QueryMessage query = QueryMessage.newBuilder()
                .setMetadata( Metadata.newBuilder().setTransactionId( txId ) )
                .setQuery( Query.newBuilder().setFrom( From.newBuilder().setScan( Scan.newBuilder().setEntity( tableEntity ) ) ) )
                .build();
        final TupleIterator iterator = this.wrapper.query( query );
        final List<String> columnNames = iterator.getSimpleNames();

        /* Create insert messages for remaining columns- */
        iterator.forEachRemaining( t -> {
            final InsertMessage.Builder insert = InsertMessage.newBuilder()
                    .setMetadata( Metadata.newBuilder().setTransactionId( txId ) )
                    .setFrom( From.newBuilder().setScan( Scan.newBuilder().setEntity( newTableEntity ) ) );
            for ( int i = 0; i < columnNames.size(); i++ ) {
                final String name = columnNames.get( i );
                if ( !name.equals( oldPhysicalColumnName ) ) {
                    insert.addElementsBuilder()
                            .setColumn( ColumnName.newBuilder().setName( columnNames.get( i ) ) )
                            .setValue( t.getRaw().getData( i ) );
                }
            }
            if ( !this.wrapper.insert( insert.build() ) ) {
                throw new GenericRuntimeException( "Failed to migrate data." );
            }
        } );

        adapterCatalog.replacePhysical( new PhysicalTable( table.id, table.allocationId, table.logicalId, newPhysicalTableName, pColumns, table.namespaceId, table.namespaceName, table.uniqueFieldIds, table.adapterId ) );

        // Delete old table
        this.wrapper.dropEntityBlocking( DropEntityMessage.newBuilder().setMetadata( Metadata.newBuilder().setTransactionId( txId ) ).setEntity( tableEntity ).build() );

        // Update column placement physical table names
        updateNativePhysical( allocId );

    }


    @Override
    public String addIndex( Context context, LogicalIndex index, AllocationTable allocation ) {
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );
        PhysicalTable physical = adapterCatalog.fromAllocation( allocation.id );

        /* Prepare CREATE INDEX message. */
        final IndexType indexType;
        try {
            indexType = IndexType.valueOf( index.method.toUpperCase() );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Unknown index type: " + index.method );
        }
        final IndexName.Builder indexName = IndexName.newBuilder()
                .setName( "idx" + index.id ).setEntity(
                        EntityName.newBuilder()
                                .setSchema( this.currentNamespace.getCottontailSchema() )
                                .setName( physical.name ) );

        final IndexDefinition.Builder definition = IndexDefinition.newBuilder().setType( indexType ).setName( indexName );
        for ( long columnId : index.key.fieldIds ) {
            PhysicalColumn column = adapterCatalog.getColumn( columnId, allocation.id );
            definition.addColumns( ColumnName.newBuilder().setName( column.name ) );
        }

        final CreateIndexMessage createIndex = CreateIndexMessage.newBuilder()
                .setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() )
                .setDefinition( definition ).build();
        this.wrapper.createIndexBlocking( createIndex );
        return createIndex.getDefinition().getName().getName();
    }


    @Override
    public void dropIndex( Context context, LogicalIndex catalogIndex, long allocId ) {
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );
        PhysicalTable table = adapterCatalog.fromAllocation( allocId );

        /* Prepare DROP INDEX message. */
        final DropIndexMessage.Builder dropIndex = DropIndexMessage.newBuilder().setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() );
        final IndexName indexName = IndexName.newBuilder()
                .setEntity( EntityName.newBuilder().setName( table.name ).setSchema( currentNamespace.getCottontailSchema() ) )
                .setName( "idx" + catalogIndex.id )
                .build();

        dropIndex.setIndex( indexName );
        this.wrapper.dropIndexBlocking( dropIndex.build() );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "Cottontail does not support prepare." );
        return false;
    }


    @Override
    public void commit( PolyXid xid ) {
        this.wrapper.commit( xid );
    }


    @Override
    public void rollback( PolyXid xid ) {
        this.wrapper.rollback( xid );
    }


    @Override
    public void truncate( Context context, long allocId ) {
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );
        PhysicalTable physical = adapterCatalog.fromAllocation( allocId );


        /* Prepare TRUNCATE message. */
        final String physicalTableName = physical.name;
        final TruncateEntityMessage truncate = TruncateEntityMessage.newBuilder()
                .setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() )
                .setEntity( EntityName.newBuilder().setSchema( this.currentNamespace.getCottontailSchema() ).setName( physicalTableName ) )
                .buildPartial();
        this.wrapper.truncateEntityBlocking( truncate );

    }


    @Override
    public void updateColumnType( Context context, long allocId, LogicalColumn newCol ) {
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );

        PhysicalColumn column = adapterCatalog.updateColumnType( allocId, newCol );
        PhysicalTable physicalTable = adapterCatalog.fromAllocation( allocId );
        List<PhysicalColumn> pColumns = adapterCatalog.getColumns( allocId ).stream().map( c -> c.id == column.id ? column : c ).toList();

        final List<ColumnDefinition> columns = this.buildColumnDefinitions( pColumns );

        final String currentPhysicalTableName = physicalTable.name;
        final String newPhysicalTableName = CottontailNameUtil.incrementNameRevision( currentPhysicalTableName );

        final EntityName tableEntity = EntityName.newBuilder()
                .setSchema( this.currentNamespace.getCottontailSchema() )
                .setName( currentPhysicalTableName )
                .build();

        final EntityName newTableEntity = EntityName.newBuilder()
                .setSchema( this.currentNamespace.getCottontailSchema() )
                .setName( newPhysicalTableName )
                .build();

        final CreateEntityMessage create = CreateEntityMessage.newBuilder()
                .setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() )
                .setDefinition( EntityDefinition.newBuilder().setEntity( newTableEntity ).addAllColumns( columns ) )
                .build();

        if ( !this.wrapper.createEntityBlocking( create ) ) {
            throw new GenericRuntimeException( "Unable to create table." );
        }

        final QueryMessage query = QueryMessage.newBuilder()
                .setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() )
                .setQuery( Query.newBuilder().setFrom( From.newBuilder().setScan( Scan.newBuilder().setEntity( tableEntity ).build() ) ) )
                .build();

        final TupleIterator iterator = this.wrapper.query( query );
        final From from = From.newBuilder().setScan( Scan.newBuilder().setEntity( newTableEntity ).build() ).build();
        final List<String> columnNames = iterator.getSimpleNames();
        iterator.forEachRemaining( t -> {
            final InsertMessage.Builder insert = InsertMessage.newBuilder()
                    .setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() )
                    .setFrom( from );
            int i = 0;
            for ( Literal d : t.getRaw().getDataList() ) {
                insert.addElements( InsertElement.newBuilder()
                        .setColumn( ColumnName.newBuilder().setName( columnNames.get( i++ ) ) )
                        .setValue( d ) );
            }
            this.wrapper.insert( insert.build() );
        } );

        this.wrapper.dropEntityBlocking( DropEntityMessage.newBuilder()
                .setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() )
                .setEntity( tableEntity ).build() );

        adapterCatalog.replacePhysical( new PhysicalTable( physicalTable.id, physicalTable.allocationId, physicalTable.logicalId, newPhysicalTableName, pColumns, physicalTable.namespaceId, physicalTable.namespaceName, physicalTable.uniqueFieldIds, physicalTable.adapterId ) );

        updateNativePhysical( allocId );
    }


    @Override
    public List<IndexMethodModel> getAvailableIndexMethods() {
        ArrayList<IndexMethodModel> available = new ArrayList<>();
        for ( IndexType indexType : IndexType.values() ) {
            available.add( new IndexMethodModel( indexType.name().toLowerCase(), indexType.name() ) );
        }
        return ImmutableList.copyOf( available );
    }


    @Override
    public void renameLogicalColumn( long id, String newColumnName ) {
        long allocId = adapterCatalog.fields.values().stream().filter( c -> c.id == id ).map( c -> c.allocId ).findFirst().orElseThrow();
        CottontailEntity table = adapterCatalog.fromAllocation( allocId ).unwrap( CottontailEntity.class ).orElseThrow();

        adapterCatalog.renameLogicalColumn( id, newColumnName );

        adapterCatalog.fields.values().stream().filter( c -> c.id == id ).forEach( c -> updateNativePhysical( c.allocId ) );

        updateNativePhysical( allocId );
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
    public void shutdown() {
        this.wrapper.close();
        if ( this.isEmbedded ) {
            this.embeddedServer.stop();
        }
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {

    }


    @SuppressWarnings("unused")
    public interface Exclude {

        void dropIndex( Context context, LogicalIndex catalogIndex, long allocId );

        String addIndex( Context context, LogicalIndex index, AllocationTable allocation );

        void dropColumn( Context context, long allocId, long columnId );

        void dropTable( Context context, long allocId );

        void updateColumnType( Context context, long allocId, LogicalColumn newCol );

        void addColumn( Context context, long allocId, LogicalColumn logicalColumn );

        void refreshTable( long allocId );

        void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocationWrapper );

    }

}

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

package org.polypheny.db.adapter.cottontail;


import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.Expose;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.Adapter.AdapterSettingList;
import org.polypheny.db.adapter.Adapter.AdapterSettingString;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.cottontail.util.CottontailNameUtil;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
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
        usedModes = { DeployMode.EMBEDDED, DeployMode.REMOTE })
@AdapterSettingString(name = "host", defaultValue = "localhost", position = 1)
@AdapterSettingInteger(name = "port", defaultValue = 1865, position = 2)
@AdapterSettingString(name = "database", defaultValue = "cottontail", position = 3)
@AdapterSettingList(name = "engine", options = { "MAPDB", "HARE" }, position = 4)
public class CottontailStore extends DataStore {

    // Running embedded
    private final boolean isEmbedded;
    @Expose(serialize = false, deserialize = false)
    private transient final CottontailGrpcServer embeddedServer;

    private final String dbHostname;
    private final int dbPort;
    private final String dbName;
    private final Engine engine;

    private CottontailSchema currentSchema;
    @Expose(serialize = false, deserialize = false)
    private final transient CottontailWrapper wrapper;


    public CottontailStore( int storeId, String uniqueName, Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true );

        this.dbName = settings.get( "database" );
        this.isEmbedded = settings.get( "mode" ).equalsIgnoreCase( "embedded" );
        this.dbPort = Integer.parseInt( settings.get( "port" ) );

        engine = Engine.valueOf( settings.get( "engine" ).trim() );
        if ( engine == null ) {
            throw new RuntimeException( "Unknown engine: " + engine );
        }

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
                CottontailGrpc.CreateSchemaMessage.newBuilder().setSchema( SchemaName.newBuilder().setName( this.dbName ) ).build()
        );
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        this.currentSchema = CottontailSchema.create( rootSchema, name, this.wrapper, this );
    }


    @Override
    public Table createTableSchema( CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
        final AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        final AlgDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        List<String> logicalColumnNames = new LinkedList<>();
        List<String> physicalColumnNames = new LinkedList<>();
        String physicalSchemaName = null;
        String physicalTableName = null;

        if ( physicalSchemaName == null ) {
            physicalSchemaName = partitionPlacement.physicalTableName != null
                    ? partitionPlacement.physicalSchemaName
                    : this.dbName;
        }
        if ( physicalTableName == null ) {
            physicalTableName = partitionPlacement.physicalTableName != null
                    ? partitionPlacement.physicalTableName
                    : CottontailNameUtil.createPhysicalTableName( combinedTable.id, partitionPlacement.partitionId );
        }

        for ( CatalogColumnPlacement placement : columnPlacementsOnStore ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( placement.columnId );

            AlgDataType sqlType = catalogColumn.getAlgDataType( typeFactory );
            fieldInfo.add( catalogColumn.name, placement.physicalColumnName, sqlType ).nullable( catalogColumn.nullable );
            logicalColumnNames.add( catalogColumn.name );
            physicalColumnNames.add( placement.physicalColumnName != null
                    ? placement.physicalColumnName
                    : CottontailNameUtil.createPhysicalColumnName( placement.columnId ) );
        }

        CottontailTable table = new CottontailTable(
                this.currentSchema,
                combinedTable.getSchemaName(),
                combinedTable.name,
                logicalColumnNames,
                AlgDataTypeImpl.proto( fieldInfo.build() ),
                physicalSchemaName,
                physicalTableName,
                physicalColumnNames
        );

        return table;
    }


    @Override
    public Schema getCurrentSchema() {
        return this.currentSchema;
    }


    @Override
    public void createTable( Context context, CatalogTable combinedTable, List<Long> partitionIds ) {

        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );

        /* Prepare CREATE TABLE message. */
        final List<ColumnDefinition> columns = this.buildColumnDefinitions( this.catalog.getColumnPlacementsOnAdapterPerTable( this.getAdapterId(), combinedTable.id ) );

        for ( long partitionId : partitionIds ) {
            final String physicalTableName = CottontailNameUtil.createPhysicalTableName( combinedTable.id, partitionId );
            catalog.updatePartitionPlacementPhysicalNames(
                    getAdapterId(),
                    partitionId,
                    this.dbName,
                    physicalTableName );

            final EntityName tableEntity = EntityName.newBuilder()
                    .setSchema( this.currentSchema.getCottontailSchema() )
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
                throw new RuntimeException( "Unable to create table." );
            }

        }
        for ( CatalogColumnPlacement placement : this.catalog.getColumnPlacementsOnAdapterPerTable( this.getAdapterId(), combinedTable.id ) ) {
            this.catalog.updateColumnPlacementPhysicalNames(
                    this.getAdapterId(),
                    placement.columnId,
                    this.dbName,
                    CottontailNameUtil.createPhysicalColumnName( placement.columnId ),
                    true );
        }
    }


    private List<CottontailGrpc.ColumnDefinition> buildColumnDefinitions( List<CatalogColumnPlacement> placements ) {
        final List<ColumnDefinition> columns = new LinkedList<>();

        for ( CatalogColumnPlacement placement : placements ) {
            final ColumnDefinition.Builder columnBuilder = ColumnDefinition.newBuilder();
            final CatalogColumn catalogColumn = catalog.getColumn( placement.columnId );
            columnBuilder.setName( ColumnName.newBuilder().setName( CottontailNameUtil.createPhysicalColumnName( placement.columnId ) ) );
            final CottontailGrpc.Type columnType = CottontailTypeUtil.getPhysicalTypeRepresentation(
                    catalogColumn.type,
                    catalogColumn.collectionsType,
                    (catalogColumn.dimension != null) ? catalogColumn.dimension : 0 );
            columnBuilder.setType( columnType );
            if ( catalogColumn.dimension != null && catalogColumn.dimension == 1 && columnType.getNumber() != Type.STRING.getNumber() ) {
                columnBuilder.setLength( catalogColumn.cardinality );
            }
            columnBuilder.setNullable( catalogColumn.nullable );
            columnBuilder.setEngine( engine );
            columns.add( columnBuilder.build() );
        }

        return columns;
    }


    @Override
    public void dropTable( Context context, CatalogTable combinedTable, List<Long> partitionIds ) {
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );

        List<CatalogPartitionPlacement> partitionPlacements = new ArrayList<>();
        partitionIds.forEach( id -> partitionPlacements.add( catalog.getPartitionPlacement( getAdapterId(), id ) ) );

        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
            /* Prepare DROP TABLE message. */
            final String physicalTableName = partitionPlacement.physicalTableName;
            final EntityName tableEntity = EntityName.newBuilder()
                    .setSchema( this.currentSchema.getCottontailSchema() )
                    .setName( physicalTableName )
                    .build();

            this.wrapper.dropEntityBlocking( DropEntityMessage.newBuilder()
                    .setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() )
                    .setEntity( tableEntity ).build() );
        }
    }


    @Override
    public void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );

        final List<CatalogColumnPlacement> placements = this.catalog.getColumnPlacementsOnAdapterPerTable( this.getAdapterId(), catalogTable.id );
        final List<ColumnDefinition> columns = this.buildColumnDefinitions( placements );
        final List<CatalogPartitionPlacement> partitionPlacements = catalog.getPartitionPlacementByTable( getAdapterId(), catalogTable.id );
        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {

            //Since only one partition is available
            final String currentPhysicalTableName = partitionPlacement.physicalTableName;

            final String newPhysicalTableName = CottontailNameUtil.incrementNameRevision( currentPhysicalTableName );
            final String newPhysicalColumnName = CottontailNameUtil.createPhysicalColumnName( catalogColumn.id );

            final EntityName tableEntity = EntityName.newBuilder()
                    .setSchema( this.currentSchema.getCottontailSchema() )
                    .setName( currentPhysicalTableName )
                    .build();
            final EntityName newTableEntity = EntityName.newBuilder()
                    .setSchema( this.currentSchema.getCottontailSchema() )
                    .setName( newPhysicalTableName )
                    .build();

            final CreateEntityMessage message = CreateEntityMessage.newBuilder()
                    .setMetadata( Metadata.newBuilder().setTransactionId( txId ) )
                    .setDefinition( EntityDefinition.newBuilder()
                            .setEntity( newTableEntity )
                            .addAllColumns( columns ) ).build();

            if ( !this.wrapper.createEntityBlocking( message ) ) {
                throw new RuntimeException( "Unable to create table." );
            }

            PolyType actualDefaultType;
            Object defaultValue;
            if ( catalogColumn.defaultValue != null ) {
                actualDefaultType = (catalogColumn.collectionsType != null)
                        ? catalogColumn.collectionsType
                        : catalogColumn.type;
                defaultValue = CottontailTypeUtil.defaultValueParser( catalogColumn.defaultValue, actualDefaultType );
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
                    throw new RuntimeException( "Unable to migrate data." );
                }
            } );

            catalog.updatePartitionPlacementPhysicalNames(
                    getAdapterId(),
                    partitionPlacement.partitionId,
                    partitionPlacement.physicalSchemaName,
                    newPhysicalTableName );

            // Delete old table
            this.wrapper.dropEntityBlocking( DropEntityMessage.newBuilder().setMetadata( Metadata.newBuilder().setTransactionId( txId ) ).setEntity( tableEntity ).build() );

        }
        // Update column placement physical table names
        for ( CatalogColumnPlacement placement : this.catalog.getColumnPlacementsOnAdapterPerTable( this.getAdapterId(), catalogTable.id ) ) {
            this.catalog.updateColumnPlacementPhysicalNames(
                    this.getAdapterId(),
                    placement.columnId,
                    this.dbName,
                    CottontailNameUtil.createPhysicalColumnName( placement.columnId ),
                    true );
        }


    }


    @Override
    public void dropColumn( Context context, CatalogColumnPlacement columnPlacement ) {
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );

        final List<CatalogColumnPlacement> placements = this.catalog.getColumnPlacementsOnAdapterPerTable( this.getAdapterId(), columnPlacement.tableId );
        placements.removeIf( it -> it.columnId == columnPlacement.columnId );
        final List<ColumnDefinition> columns = this.buildColumnDefinitions( placements );
        final CatalogTable catalogTable = catalog.getTable( placements.get( 0 ).tableId );
        final List<CatalogPartitionPlacement> partitionPlacements = catalog.getPartitionPlacementByTable( getAdapterId(), catalogTable.id );

        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {

            final String currentPhysicalTableName = partitionPlacement.physicalTableName;
            final String newPhysicalTableName = CottontailNameUtil.incrementNameRevision( currentPhysicalTableName );
            final String oldPhysicalColumnName = columnPlacement.physicalColumnName;

            final EntityName tableEntity = EntityName.newBuilder()
                    .setSchema( this.currentSchema.getCottontailSchema() )
                    .setName( currentPhysicalTableName )
                    .build();
            final EntityName newTableEntity = EntityName.newBuilder()
                    .setSchema( this.currentSchema.getCottontailSchema() )
                    .setName( newPhysicalTableName )
                    .build();

            final CreateEntityMessage message = CreateEntityMessage.newBuilder()
                    .setMetadata( Metadata.newBuilder().setTransactionId( txId ) )
                    .setDefinition( EntityDefinition.newBuilder().setEntity( newTableEntity ).addAllColumns( columns ) )
                    .build();

            if ( !this.wrapper.createEntityBlocking( message ) ) {
                throw new RuntimeException( "Unable to create table." );
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
                    throw new RuntimeException( "Failed to migrate data." );
                }
            } );

            catalog.updatePartitionPlacementPhysicalNames(
                    getAdapterId(),
                    partitionPlacement.partitionId,
                    partitionPlacement.physicalSchemaName,
                    newPhysicalTableName );

            // Delete old table
            this.wrapper.dropEntityBlocking( DropEntityMessage.newBuilder().setMetadata( Metadata.newBuilder().setTransactionId( txId ) ).setEntity( tableEntity ).build() );
        }

        // Update column placement physical table names
        for ( CatalogColumnPlacement placement : this.catalog.getColumnPlacementsOnAdapterPerTable( this.getAdapterId(), columnPlacement.tableId ) ) {
            this.catalog.updateColumnPlacementPhysicalNames(
                    this.getAdapterId(),
                    placement.columnId,
                    this.dbName,
                    CottontailNameUtil.createPhysicalColumnName( placement.columnId ),
                    true );
        }

    }


    @Override
    public void addIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds ) {
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );

        List<CatalogPartitionPlacement> partitionPlacements = new ArrayList<>();
        partitionIds.forEach( id -> partitionPlacements.add( catalog.getPartitionPlacement( getAdapterId(), id ) ) );
        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {

            /* Prepare CREATE INDEX message. */
            final IndexType indexType;
            try {
                indexType = IndexType.valueOf( catalogIndex.method.toUpperCase() );
            } catch ( Exception e ) {
                throw new RuntimeException( "Unknown index type: " + catalogIndex.method );
            }
            final IndexName.Builder indexName = IndexName.newBuilder()
                    .setName( "idx" + catalogIndex.id ).setEntity(
                            EntityName.newBuilder()
                                    .setSchema( this.currentSchema.getCottontailSchema() )
                                    .setName( partitionPlacement.physicalTableName ) );

            final IndexDefinition.Builder definition = IndexDefinition.newBuilder().setType( indexType ).setName( indexName );
            for ( long columnId : catalogIndex.key.columnIds ) {
                CatalogColumnPlacement placement = Catalog.getInstance().getColumnPlacement( getAdapterId(), columnId );
                definition.addColumns( ColumnName.newBuilder().setName( placement.physicalColumnName ) );
            }

            final CreateIndexMessage createIndex = CreateIndexMessage.newBuilder()
                    .setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() )
                    .setDefinition( definition ).build();
            this.wrapper.createIndexBlocking( createIndex );
        }
    }


    @Override
    public void dropIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds ) {
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );
        CatalogPartitionPlacement partitionPlacement = catalog.getPartitionPlacement(
                getAdapterId(),
                catalog.getTable( catalogIndex.key.tableId ).partitionProperty.partitionIds.get( 0 ) );
        /* Prepare DROP INDEX message. */
        final DropIndexMessage.Builder dropIndex = DropIndexMessage.newBuilder().setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() );
        final IndexName indexName = IndexName.newBuilder()
                .setEntity( EntityName.newBuilder().setName( partitionPlacement.physicalTableName ).setSchema( currentSchema.getCottontailSchema() ) )
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
    public void truncate( Context context, CatalogTable table ) {
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );

        for ( CatalogPartitionPlacement partitionPlacement : catalog.getPartitionPlacementByTable( getAdapterId(), table.id ) ) {
            /* Prepare TRUNCATE message. */
            final String physicalTableName = partitionPlacement.physicalTableName;
            final TruncateEntityMessage truncate = TruncateEntityMessage.newBuilder()
                    .setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() )
                    .setEntity( EntityName.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( physicalTableName ) )
                    .buildPartial();
            this.wrapper.truncateEntityBlocking( truncate );
        }
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn, PolyType oldType ) {
        /* Begin or continue Cottontail DB transaction. */
        final long txId = this.wrapper.beginOrContinue( context.getStatement().getTransaction() );

        final List<CatalogColumnPlacement> placements = this.catalog.getColumnPlacementsOnAdapterSortedByPhysicalPosition( this.getAdapterId(), catalogColumn.tableId );
        final List<ColumnDefinition> columns = this.buildColumnDefinitions( placements );

        List<CatalogPartitionPlacement> partitionPlacements = catalog.getPartitionPlacementByTable( getAdapterId(), catalogColumn.tableId );

        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {

            final String currentPhysicalTableName = partitionPlacement.physicalTableName;
            final String newPhysicalTableName = CottontailNameUtil.incrementNameRevision( currentPhysicalTableName );

            final EntityName tableEntity = EntityName.newBuilder()
                    .setSchema( this.currentSchema.getCottontailSchema() )
                    .setName( currentPhysicalTableName )
                    .build();

            final EntityName newTableEntity = EntityName.newBuilder()
                    .setSchema( this.currentSchema.getCottontailSchema() )
                    .setName( newPhysicalTableName )
                    .build();

            final CreateEntityMessage create = CreateEntityMessage.newBuilder()
                    .setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() )
                    .setDefinition( EntityDefinition.newBuilder().setEntity( newTableEntity ).addAllColumns( columns ) )
                    .build();

            if ( !this.wrapper.createEntityBlocking( create ) ) {
                throw new RuntimeException( "Unable to create table." );
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

            catalog.updatePartitionPlacementPhysicalNames(
                    getAdapterId(),
                    partitionPlacement.partitionId,
                    partitionPlacement.physicalSchemaName,
                    newPhysicalTableName );

            this.wrapper.dropEntityBlocking( DropEntityMessage.newBuilder()
                    .setMetadata( Metadata.newBuilder().setTransactionId( txId ).build() )
                    .setEntity( tableEntity ).build() );
        }
        for ( CatalogColumnPlacement ccp : placements ) {
            catalog.updateColumnPlacementPhysicalNames(
                    getAdapterId(),
                    ccp.columnId,
                    ccp.physicalSchemaName,
                    ccp.physicalColumnName,
                    false );
        }
    }


    @Override
    public List<AvailableIndexMethod> getAvailableIndexMethods() {
        ArrayList<AvailableIndexMethod> available = new ArrayList<>();
        for ( IndexType indexType : IndexType.values() ) {
            available.add( new AvailableIndexMethod( indexType.name().toLowerCase(), indexType.name() ) );
        }
        return ImmutableList.copyOf( available );
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
    public void shutdown() {
        this.wrapper.close();
        if ( this.isEmbedded ) {
            this.embeddedServer.stop();
        }
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {

    }

}

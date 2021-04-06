/*
 * Copyright 2019-2021 The Polypheny Project
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.cottontail.util.CottontailNameUtil;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeImpl;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.FileSystemManager;
import org.vitrivr.cottontail.CottontailKt;
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
import org.vitrivr.cottontail.grpc.CottontailGrpc.Query;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryResponseMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryResponseMessage.Tuple;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Scan;
import org.vitrivr.cottontail.grpc.CottontailGrpc.SchemaName;
import org.vitrivr.cottontail.grpc.CottontailGrpc.TruncateEntityMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Type;
import org.vitrivr.cottontail.server.grpc.CottontailGrpcServer;


@Slf4j
public class CottontailStore extends DataStore {

    public static final String ADAPTER_NAME = "Cottontail DB";

    public static final String DESCRIPTION = "Cottontail DB is a column store aimed at multimedia retrieval. It is optimized for classical boolean as well as vector-space retrieval.";

    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingList( "type", false, true, false, ImmutableList.of( "Embedded", "Standalone" ) ),
            new AdapterSettingString( "host", false, true, false, "localhost" ),
            new AdapterSettingInteger( "port", false, true, false, 1865 ),
            new AdapterSettingString( "database", false, true, false, "cottontail" )
    );

    // Running embedded
    private final boolean isEmbedded;
    @Expose(serialize = false, deserialize = false)
    private transient final CottontailGrpcServer embeddedServer;

    private final String dbHostname;
    private final int dbPort;
    private final String dbName;

    private CottontailSchema currentSchema;
    @Expose(serialize = false, deserialize = false)
    private final transient CottontailWrapper wrapper;


    public CottontailStore( int storeId, String uniqueName, Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true );
        this.dbName = settings.get( "database" );
        this.isEmbedded = settings.get( "type" ).equalsIgnoreCase( "Embedded" );
        this.dbPort = Integer.parseInt( settings.get( "port" ) );

        if ( this.isEmbedded ) {
            File adapterRoot = FileSystemManager.getInstance().registerNewFolder( "data/cottontaildb-store" );
            File embeddedDir = new File( adapterRoot, "store" + getAdapterId() );

            if ( !embeddedDir.exists() ) {
                if ( !embeddedDir.mkdirs() ) {
                    throw new RuntimeException( "Could not create root directory" );
                }
            }

            final File dataFolder = new File( embeddedDir, "data" );
            final Config config = new Config(
                    Paths.get( dataFolder.getAbsolutePath() ),
                    false,
                    false,
                    null,
                    new MapDBConfig( true, true, 22, 1000L ),
                    new ServerConfig(),
                    new ExecutionConfig(),
                    new CacheConfig()
            );

            this.embeddedServer = CottontailKt.embedded( config );
            this.dbHostname = "localhost";
        } else {
            this.embeddedServer = null;
            this.dbHostname = settings.get( "host" );

        }

        ManagedChannel channel = NettyChannelBuilder
                .forAddress( this.dbHostname, this.dbPort )
                .usePlaintext()
                .maxInboundMetadataSize( CottontailWrapper.maxMessageSize )
                .maxInboundMessageSize( CottontailWrapper.maxMessageSize )
                .build();
        this.wrapper = new CottontailWrapper( channel );
        this.wrapper.checkedCreateSchemaBlocking(
                CottontailGrpc.CreateSchemaMessage.newBuilder().setSchema( SchemaName.newBuilder().setName( this.dbName ) ).build()
        );
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        this.currentSchema = CottontailSchema.create( rootSchema, name, this.wrapper, this );
    }


    @Override
    public Table createTableSchema( CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        final RelDataTypeFactory typeFactory = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        List<String> logicalColumnNames = new LinkedList<>();
        List<String> physicalColumnNames = new LinkedList<>();
        String physicalSchemaName = null;
        String physicalTableName = null;
        for ( CatalogColumnPlacement placement : columnPlacementsOnStore ) {
            CatalogColumn catalogColumn = Catalog.getInstance().getColumn( placement.columnId );
            if ( physicalSchemaName == null ) {
                physicalSchemaName = placement.physicalTableName != null ? placement.physicalSchemaName : this.dbName;
            }
            if ( physicalTableName == null ) {
                physicalTableName = placement.physicalTableName != null ? placement.physicalTableName : "tab" + combinedTable.id;
            }
            RelDataType sqlType = catalogColumn.getRelDataType( typeFactory );
            fieldInfo.add( catalogColumn.name, placement.physicalColumnName, sqlType ).nullable( catalogColumn.nullable );
            logicalColumnNames.add( catalogColumn.name );
            physicalColumnNames.add( placement.physicalColumnName != null ? placement.physicalColumnName : "col" + placement.columnId );
        }

        CottontailTable table = new CottontailTable(
                this.currentSchema,
                combinedTable.getSchemaName(),
                combinedTable.name,
                logicalColumnNames,
                RelDataTypeImpl.proto( fieldInfo.build() ),
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
    public void createTable( Context context, CatalogTable combinedTable ) {

        final List<ColumnDefinition> columns = this.buildColumnDefinitions( this.catalog.getColumnPlacementsOnAdapter( this.getAdapterId(), combinedTable.id ) );
        final String physicalTableName = CottontailNameUtil.createPhysicalTableName( combinedTable.id );
        final EntityName tableEntity = EntityName.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( physicalTableName ).build();
        final EntityDefinition definition = EntityDefinition.newBuilder()
                .setEntity( tableEntity )
                .addAllColumns( columns ).build();

        if ( !this.wrapper.createEntityBlocking( CreateEntityMessage.newBuilder().setDefinition( definition ).build() ) ) {
            throw new RuntimeException( "Unable to create table." );
        }

        for ( CatalogColumnPlacement placement : this.catalog.getColumnPlacementsOnAdapter( this.getAdapterId(), combinedTable.id ) ) {
            this.catalog.updateColumnPlacementPhysicalNames(
                    this.getAdapterId(),
                    placement.columnId,
                    this.dbName,
                    physicalTableName,
                    CottontailNameUtil.createPhysicalColumnName( placement.columnId ),
                    true );
        }
    }


    private List<CottontailGrpc.ColumnDefinition> buildColumnDefinitions( List<CatalogColumnPlacement> placements ) {
        final List<ColumnDefinition> columns = new LinkedList<>();

        for ( CatalogColumnPlacement placement : placements ) {
            final ColumnDefinition.Builder columnBuilder = ColumnDefinition.newBuilder();
            final CatalogColumn catalogColumn = catalog.getColumn( placement.columnId );
            columnBuilder.setName( CottontailNameUtil.createPhysicalColumnName( placement.columnId ) );
            final CottontailGrpc.Type columnType = CottontailTypeUtil.getPhysicalTypeRepresentation(
                    catalogColumn.type,
                    catalogColumn.collectionsType,
                    (catalogColumn.dimension != null) ? catalogColumn.dimension : 0 );
            columnBuilder.setType( columnType );
            if ( catalogColumn.dimension != null && catalogColumn.dimension == 1 && columnType.getNumber() != Type.STRING.getNumber() ) {
                columnBuilder.setLength( catalogColumn.cardinality );
            }
            columnBuilder.setNullable( catalogColumn.nullable );
            columnBuilder.setEngine( Engine.MAPDB );
            columns.add( columnBuilder.build() );
        }

        return columns;
    }


    @Override
    public void dropTable( Context context, CatalogTable combinedTable ) {
        final String physicalTableName = CottontailNameUtil.getPhysicalTableName( this.getAdapterId(), combinedTable.id );
        final EntityName tableEntity = EntityName.newBuilder()
                .setSchema( this.currentSchema.getCottontailSchema() )
                .setName( physicalTableName )
                .build();

        this.wrapper.dropEntityBlocking( DropEntityMessage.newBuilder().setEntity( tableEntity ).build() );
    }


    @Override
    public void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        final List<CatalogColumnPlacement> placements = this.catalog.getColumnPlacementsOnAdapter( this.getAdapterId(), catalogTable.id );
        final List<ColumnDefinition> columns = this.buildColumnDefinitions( placements );

        final String currentPhysicalTableName;
        if ( placements.get( 0 ).columnId == catalogColumn.id ) {
            currentPhysicalTableName = placements.get( 1 ).physicalTableName;
        } else {
            currentPhysicalTableName = placements.get( 0 ).physicalTableName;
        }
        final String newPhysicalTableName = CottontailNameUtil.incrementNameRevision( currentPhysicalTableName );
        final String newPhysicalColumnName = CottontailNameUtil.createPhysicalColumnName( catalogColumn.id );

        final EntityName tableEntity = EntityName.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( currentPhysicalTableName ).build();
        final EntityName newTableEntity = EntityName.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( newPhysicalTableName ).build();

        final CreateEntityMessage message = CreateEntityMessage.newBuilder().setDefinition( EntityDefinition.newBuilder()
                .setEntity( newTableEntity )
                .addAllColumns( columns ) ).build();

        if ( !this.wrapper.createEntityBlocking( message ) ) {
            throw new RuntimeException( "Unable to create table." );
        }

        PolyType actualDefaultType;
        Object defaultValue;
        if ( catalogColumn.defaultValue != null ) {
            actualDefaultType = (catalogColumn.collectionsType != null) ? catalogColumn.collectionsType : catalogColumn.type;
            defaultValue = CottontailTypeUtil.defaultValueParser( catalogColumn.defaultValue, actualDefaultType );
        } else {
            defaultValue = null;
            actualDefaultType = null;
        }
        CottontailGrpc.Literal defaultData = CottontailTypeUtil.toData( defaultValue, actualDefaultType );

        final QueryMessage query = QueryMessage.newBuilder().setQuery( Query.newBuilder().setFrom( From.newBuilder().setScan( Scan.newBuilder().setEntity( tableEntity ) ) ) ).build();
        final Iterator<QueryResponseMessage> queryResponse = this.wrapper.query( query );
        queryResponse.forEachRemaining( responseMessage -> {
            for ( Tuple tuple : responseMessage.getTuplesList() ) {
                final InsertMessage.Builder insert = InsertMessage.newBuilder().setFrom(
                        From.newBuilder().setScan( Scan.newBuilder().setEntity( newTableEntity ) )
                );
                int i = 0;
                for ( CottontailGrpc.Literal literal : tuple.getDataList() ) {
                    insert.addInsertsBuilder().setColumn( responseMessage.getColumns( i++ ) ).setValue( literal );
                }
                insert.addInsertsBuilder().setColumn( ColumnName.newBuilder().setName( newPhysicalColumnName ).build() ).setValue( defaultData );

                if ( !this.wrapper.insert( insert.build() ) ) {
                    throw new RuntimeException( "Unable to migrate data." );
                }
            }
        } );

        // Update column placement physical table names
        for ( CatalogColumnPlacement placement : this.catalog.getColumnPlacementsOnAdapter( this.getAdapterId(), catalogTable.id ) ) {
            this.catalog.updateColumnPlacementPhysicalNames(
                    this.getAdapterId(),
                    placement.columnId,
                    this.dbName,
                    newPhysicalTableName,
                    CottontailNameUtil.createPhysicalColumnName( placement.columnId ),
                    true );
        }

        // Delete old table
        this.wrapper.dropEntityBlocking( DropEntityMessage.newBuilder().setEntity( tableEntity ).build() );

    }


    /**
     * TODO: js(ct): Add dropColumn to cottontail
     *
     * @param context
     * @param columnPlacement
     */
    @Override
    public void dropColumn( Context context, CatalogColumnPlacement columnPlacement ) {
        final List<CatalogColumnPlacement> placements = this.catalog.getColumnPlacementsOnAdapter( this.getAdapterId(), columnPlacement.tableId );
        placements.removeIf( it -> it.columnId == columnPlacement.columnId );
        final List<ColumnDefinition> columns = this.buildColumnDefinitions( placements );

        final String currentPhysicalTableName = placements.get( 0 ).physicalTableName;

        final String newPhysicalTableName = CottontailNameUtil.incrementNameRevision( currentPhysicalTableName );
        final String oldPhysicalColumnName = columnPlacement.physicalColumnName;

        final EntityName tableEntity = EntityName.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( currentPhysicalTableName ).build();
        final EntityName newTableEntity = EntityName.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( newPhysicalTableName ).build();

        final CreateEntityMessage message = CreateEntityMessage.newBuilder().setDefinition(
                EntityDefinition.newBuilder().setEntity( newTableEntity ).addAllColumns( columns )
        ).build();

        if ( !this.wrapper.createEntityBlocking( message ) ) {
            throw new RuntimeException( "Unable to create table." );
        }

        final Query query = Query.newBuilder().setFrom( From.newBuilder().setScan( Scan.newBuilder().setEntity( tableEntity ) ) ).build();
        final Iterator<QueryResponseMessage> queryResponse = this.wrapper.query( QueryMessage.newBuilder().setQuery( query ).build() );
        queryResponse.forEachRemaining( responseMessage -> {
            int droppedIndex = 0;
            for ( ColumnName c : responseMessage.getColumnsList() ) {
                if ( c.getName().equals( oldPhysicalColumnName ) ) {
                    break;
                }
                droppedIndex++;
            }
            for ( Tuple tuple : responseMessage.getTuplesList() ) {
                final InsertMessage.Builder insert = InsertMessage.newBuilder().setFrom( From.newBuilder().setScan( Scan.newBuilder().setEntity( newTableEntity ) ) );
                int i = 0;
                for ( Literal l : tuple.getDataList() ) {
                    if ( i != droppedIndex ) {
                        insert.addInsertsBuilder().setColumn( responseMessage.getColumns( i ) ).setValue( l );
                    }
                    i++;
                }
                if ( !this.wrapper.insert( insert.build() ) ) {
                    throw new RuntimeException( "Failed to migrate data." );
                }
            }
        } );

        // Update column placement physical table names
        for ( CatalogColumnPlacement placement : this.catalog.getColumnPlacementsOnAdapter( this.getAdapterId(), columnPlacement.tableId ) ) {
            this.catalog.updateColumnPlacementPhysicalNames(
                    this.getAdapterId(),
                    placement.columnId,
                    this.dbName,
                    newPhysicalTableName,
                    CottontailNameUtil.createPhysicalColumnName( placement.columnId ),
                    true );
        }

        // Delete old table
        this.wrapper.dropEntityBlocking( DropEntityMessage.newBuilder().setEntity( tableEntity ).build() );
    }


    @Override
    public void addIndex( Context context, CatalogIndex catalogIndex ) {
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
                                .setName( Catalog.getInstance().getColumnPlacement( getAdapterId(), catalogIndex.key.columnIds.get( 0 ) ).physicalTableName ) );

        final IndexDefinition.Builder definition = IndexDefinition.newBuilder().setType( indexType ).setName( indexName );
        for ( long columnId : catalogIndex.key.columnIds ) {
            CatalogColumnPlacement placement = Catalog.getInstance().getColumnPlacement( getAdapterId(), columnId );
            definition.addColumns( ColumnName.newBuilder().setName( placement.physicalColumnName ) );
        }

        final CreateIndexMessage createIndex = CreateIndexMessage.newBuilder().setDefinition( definition ).build();
        this.wrapper.createIndexBlocking( createIndex );
    }


    @Override
    public void dropIndex( Context context, CatalogIndex catalogIndex ) {
        final DropIndexMessage.Builder dropIndex = DropIndexMessage.newBuilder();
        final IndexName indexName = IndexName.newBuilder()
                .setEntity( EntityName.newBuilder().setName( Catalog.getInstance().getColumnPlacement( getAdapterId(), catalogIndex.key.columnIds.get( 0 ) ).physicalTableName ).setSchema( currentSchema.getCottontailSchema() ) )
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
        log.debug( "Cottontail does not support commit." );
    }


    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "Cottontail does not support rollback." );
    }


    @Override
    public void truncate( Context context, CatalogTable table ) {
        final String physicalTableName = CottontailNameUtil.getPhysicalTableName( this.getAdapterId(), table.id );
        final TruncateEntityMessage truncate = TruncateEntityMessage.newBuilder().setEntity(
                EntityName.newBuilder()
                        .setSchema( this.currentSchema.getCottontailSchema() )
                        .setName( physicalTableName )
        ).buildPartial();
        this.wrapper.truncateEntityBlocking( truncate );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn, PolyType oldType ) {
        final List<CatalogColumnPlacement> placements = this.catalog.getColumnPlacementsOnAdapterSortedByPhysicalPosition( this.getAdapterId(), catalogColumn.tableId );
        final List<ColumnDefinition> columns = this.buildColumnDefinitions( placements );

        final String currentPhysicalTableName = placements.get( 0 ).physicalTableName;
        final String newPhysicalTableName = CottontailNameUtil.incrementNameRevision( currentPhysicalTableName );

        EntityName tableEntity = EntityName.newBuilder()
                .setSchema( this.currentSchema.getCottontailSchema() )
                .setName( currentPhysicalTableName )
                .build();

        EntityName newTableEntity = EntityName.newBuilder()
                .setSchema( this.currentSchema.getCottontailSchema() )
                .setName( newPhysicalTableName )
                .build();

        final CreateEntityMessage create = CreateEntityMessage.newBuilder()
                .setDefinition( EntityDefinition.newBuilder().setEntity( newTableEntity ).addAllColumns( columns ) )
                .build();

        if ( !this.wrapper.createEntityBlocking( create ) ) {
            throw new RuntimeException( "Unable to create table." );
        }

        final Query query = Query.newBuilder().setFrom( From.newBuilder().setScan( Scan.newBuilder().setEntity( tableEntity ).build() ) ).build();
        final Iterator<QueryResponseMessage> queryResponse = this.wrapper.query( QueryMessage.newBuilder().setQuery( query ).build() );

        final From from = From.newBuilder().setScan( Scan.newBuilder().setEntity( newTableEntity ).build() ).build();
        queryResponse.forEachRemaining( response -> {
            for ( Tuple tuple : response.getTuplesList() ) {
                final InsertMessage.Builder insert = InsertMessage.newBuilder().setFrom( from );
                int i = 0;
                for ( Literal d : tuple.getDataList() ) {
                    insert.addInserts( InsertElement.newBuilder()
                            .setColumn( response.getColumns( i++ ) )
                            .setValue( d ) );
                }
                this.wrapper.insert( insert.build() );
            }
        } );

        for ( CatalogColumnPlacement ccp : placements ) {
            catalog.updateColumnPlacementPhysicalNames(
                    getAdapterId(),
                    ccp.columnId,
                    ccp.physicalSchemaName,
                    newPhysicalTableName,
                    ccp.physicalColumnName,
                    false );
        }

        this.wrapper.dropEntityBlocking( DropEntityMessage.newBuilder().setEntity( tableEntity ).build() );
    }


    @Override
    public String getAdapterName() {
        return ADAPTER_NAME;
    }


    @Override
    public List<AdapterSetting> getAvailableSettings() {
        return AVAILABLE_SETTINGS;
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

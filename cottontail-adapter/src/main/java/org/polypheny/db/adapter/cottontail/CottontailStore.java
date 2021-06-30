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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.Adapter.AdapterSettingString;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.cottontail.util.CottontailNameUtil;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
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
import org.polypheny.db.type.PolyTypeConversionUtil;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.FileSystemManager;
import org.vitrivr.cottontail.CottontailKt;
import org.vitrivr.cottontail.config.Config;
import org.vitrivr.cottontail.config.ExecutionConfig;
import org.vitrivr.cottontail.config.MapDBConfig;
import org.vitrivr.cottontail.config.ServerConfig;
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnDefinition;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Data;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Entity;
import org.vitrivr.cottontail.grpc.CottontailGrpc.EntityDefinition;
import org.vitrivr.cottontail.grpc.CottontailGrpc.From;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Index;
import org.vitrivr.cottontail.grpc.CottontailGrpc.IndexDefinition;
import org.vitrivr.cottontail.grpc.CottontailGrpc.IndexDefinition.Builder;
import org.vitrivr.cottontail.grpc.CottontailGrpc.IndexType;
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Query;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryResponseMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Tuple;
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
public class CottontailStore extends DataStore {

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
        this.isEmbedded = settings.get( "mode" ).equalsIgnoreCase( "embedded" );
        this.dbPort = Integer.parseInt( settings.get( "port" ) );

        if ( this.isEmbedded ) {
            File adapterRoot = FileSystemManager.getInstance().registerNewFolder( "data/cottontaildb-store" );
            File embeddedDir = new File( adapterRoot, "store" + getAdapterId() );

            if ( !embeddedDir.exists() ) {
                if ( !embeddedDir.mkdirs() ) {
                    throw new RuntimeException( "Could not create root directory" );
                }
            }

            File dataFolder = new File( embeddedDir, "data" );
            Config config = new Config(
                    dataFolder.toPath(),
                    false,
                    new ServerConfig(),
                    new MapDBConfig(),
                    new ExecutionConfig()
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
        this.wrapper.checkedCreateSchemaBlocking( CottontailGrpc.Schema.newBuilder().setName( this.dbName ).build() );
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        this.currentSchema = CottontailSchema.create( rootSchema, name, this.wrapper, this );
    }


    @Override
    public Table createTableSchema( CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
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

//        ColumnDefinition.Builder columnBuilder = ColumnDefinition.newBuilder();
        /*for ( CatalogColumnPlacement placement : this.catalog.getColumnPlacementsOnStore( this.getStoreId(), combinedTable.id ) ) {
            CatalogColumn catalogColumn;
            catalogColumn = catalog.getColumn( placement.columnId );

            columnBuilder.setName( CottontailNameUtil.createPhysicalColumnName( placement.columnId ) );
            CottontailGrpc.Type columnType = CottontailTypeUtil.getPhysicalTypeRepresentation( catalogColumn.type, catalogColumn.collectionsType, ( catalogColumn.dimension != null) ? catalogColumn.dimension : 0 );
            columnBuilder.setType( columnType );
            if ( catalogColumn.dimension != null && catalogColumn.dimension == 1 && columnType.getNumber() != Type.STRING.getNumber() ) {
                columnBuilder.setLength( catalogColumn.cardinality );
            }
            columns.add( columnBuilder.build() );
        }*/

        List<ColumnDefinition> columns = this.buildColumnDefinitions( this.catalog.getColumnPlacementsOnAdapterPerTable( this.getAdapterId(), combinedTable.id ) );

        String physicalTableName = CottontailNameUtil.createPhysicalTableName( combinedTable.id );
        Entity tableEntity = Entity.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( physicalTableName ).build();

        EntityDefinition message = EntityDefinition.newBuilder()
                .setEntity( tableEntity )
                .addAllColumns( columns ).build();

        if ( !this.wrapper.createEntityBlocking( message ) ) {
            throw new RuntimeException( "Unable to create table." );
        }

        for ( CatalogColumnPlacement placement : this.catalog.getColumnPlacementsOnAdapterPerTable( this.getAdapterId(), combinedTable.id ) ) {
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
        List<ColumnDefinition> columns = new ArrayList<>();
        ColumnDefinition.Builder columnBuilder = ColumnDefinition.newBuilder();

        for ( CatalogColumnPlacement placement : placements ) {
            CatalogColumn catalogColumn;
            catalogColumn = catalog.getColumn( placement.columnId );

            columnBuilder.clear();

            columnBuilder.setName( CottontailNameUtil.createPhysicalColumnName( placement.columnId ) );
            CottontailGrpc.Type columnType = CottontailTypeUtil.getPhysicalTypeRepresentation(
                    catalogColumn.type,
                    catalogColumn.collectionsType,
                    (catalogColumn.dimension != null) ? catalogColumn.dimension : 0 );
            columnBuilder.setType( columnType );
            if ( catalogColumn.dimension != null && catalogColumn.dimension == 1 && columnType.getNumber() != Type.STRING.getNumber() ) {
                columnBuilder.setLength( catalogColumn.cardinality );
            }
            columnBuilder.setNullable( catalogColumn.nullable );
            columns.add( columnBuilder.build() );
        }

        return columns;
    }


    @Override
    public void dropTable( Context context, CatalogTable combinedTable ) {
        String physicalTableName = CottontailNameUtil.getPhysicalTableName( this.getAdapterId(), combinedTable.id );
        Entity tableEntity = Entity.newBuilder()
                .setSchema( this.currentSchema.getCottontailSchema() )
                .setName( physicalTableName )
                .build();

        this.wrapper.dropEntityBlocking( tableEntity );
    }


    @Override
    public void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        final List<CatalogColumnPlacement> placements = this.catalog.getColumnPlacementsOnAdapterPerTable( this.getAdapterId(), catalogTable.id );
        final List<ColumnDefinition> columns = this.buildColumnDefinitions( placements );

        final String currentPhysicalTableName;
        if ( placements.get( 0 ).columnId == catalogColumn.id ) {
            currentPhysicalTableName = placements.get( 1 ).physicalTableName;
        } else {
            currentPhysicalTableName = placements.get( 0 ).physicalTableName;
        }
        final String newPhysicalTableName = CottontailNameUtil.incrementNameRevision( currentPhysicalTableName );
        final String newPhysicalColumnName = CottontailNameUtil.createPhysicalColumnName( catalogColumn.id );

        Entity tableEntity = Entity.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( currentPhysicalTableName ).build();
        Entity newTableEntity = Entity.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( newPhysicalTableName ).build();

        EntityDefinition message = EntityDefinition.newBuilder()
                .setEntity( newTableEntity )
                .addAllColumns( columns ).build();

        if ( !this.wrapper.createEntityBlocking( message ) ) {
            throw new RuntimeException( "Unable to create table." );
        }

        Query query = Query.newBuilder().setFrom( From.newBuilder().setEntity( tableEntity ).build() ).build();

        Iterator<QueryResponseMessage> queryResponse = this.wrapper.query( QueryMessage.newBuilder().setQuery( query ).build() );

        List<InsertMessage> inserts = new ArrayList<>();
        From from = From.newBuilder().setEntity( newTableEntity ).build();

        PolyType actualDefaultType;
        Object defaultValue;
        if ( catalogColumn.defaultValue != null ) {
            actualDefaultType = (catalogColumn.collectionsType != null) ? catalogColumn.collectionsType : catalogColumn.type;
            defaultValue = CottontailTypeUtil.defaultValueParser( catalogColumn.defaultValue, actualDefaultType );
        } else {
            defaultValue = null;
            actualDefaultType = null;
        }
        CottontailGrpc.Data defaultData = CottontailTypeUtil.toData( defaultValue, actualDefaultType );

        queryResponse.forEachRemaining( queryResponseMessage -> {
            for ( Tuple tuple : queryResponseMessage.getResultsList() ) {
                Map<String, Data> dataMap = new HashMap<>( tuple.getDataMap() );
                dataMap.put( newPhysicalColumnName, defaultData );

                inserts.add( InsertMessage.newBuilder().setTuple( Tuple.newBuilder().putAllData( dataMap ).build() ).setFrom( from ).build() );
            }
        } );

        if ( !this.wrapper.insert( inserts ) ) {
            throw new RuntimeException( "Unable to migrate data." );
        }

        // Update column placement physical table names
        for ( CatalogColumnPlacement placement : this.catalog.getColumnPlacementsOnAdapterPerTable( this.getAdapterId(), catalogTable.id ) ) {
            this.catalog.updateColumnPlacementPhysicalNames(
                    this.getAdapterId(),
                    placement.columnId,
                    this.dbName,
                    newPhysicalTableName,
                    CottontailNameUtil.createPhysicalColumnName( placement.columnId ),
                    true );
        }

        // Delete old table
        this.wrapper.dropEntityBlocking( tableEntity );

    }


    @Override
    public void dropColumn( Context context, CatalogColumnPlacement columnPlacement ) {
        final List<CatalogColumnPlacement> placements = this.catalog.getColumnPlacementsOnAdapterPerTable( this.getAdapterId(), columnPlacement.tableId );
        placements.removeIf( it -> it.columnId == columnPlacement.columnId );
        final List<ColumnDefinition> columns = this.buildColumnDefinitions( placements );

        final String currentPhysicalTableName = placements.get( 0 ).physicalTableName;

        final String newPhysicalTableName = CottontailNameUtil.incrementNameRevision( currentPhysicalTableName );
        final String oldPhysicalColumnName = columnPlacement.physicalColumnName;

        Entity tableEntity = Entity.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( currentPhysicalTableName ).build();
        Entity newTableEntity = Entity.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( newPhysicalTableName ).build();

        EntityDefinition message = EntityDefinition.newBuilder()
                .setEntity( newTableEntity )
                .addAllColumns( columns ).build();

        if ( !this.wrapper.createEntityBlocking( message ) ) {
            throw new RuntimeException( "Unable to create table." );
        }

        Query query = Query.newBuilder().setFrom( From.newBuilder().setEntity( tableEntity ).build() ).build();

        Iterator<QueryResponseMessage> queryResponse = this.wrapper.query( QueryMessage.newBuilder().setQuery( query ).build() );

        List<InsertMessage> inserts = new ArrayList<>();
        From from = From.newBuilder().setEntity( newTableEntity ).build();

        queryResponse.forEachRemaining( queryResponseMessage -> {
            for ( Tuple tuple : queryResponseMessage.getResultsList() ) {
                Map<String, Data> dataMap = new HashMap<>( tuple.getDataMap() );
                dataMap.remove( oldPhysicalColumnName );

                inserts.add( InsertMessage.newBuilder().setTuple( Tuple.newBuilder().putAllData( dataMap ).build() ).setFrom( from ).build() );
            }
        } );

        if ( !this.wrapper.insert( inserts ) ) {
            throw new RuntimeException( "Unable to migrate data." );
        }

        // Update column placement physical table names
        for ( CatalogColumnPlacement placement : this.catalog.getColumnPlacementsOnAdapterPerTable( this.getAdapterId(), columnPlacement.tableId ) ) {
            this.catalog.updateColumnPlacementPhysicalNames(
                    this.getAdapterId(),
                    placement.columnId,
                    this.dbName,
                    newPhysicalTableName,
                    CottontailNameUtil.createPhysicalColumnName( placement.columnId ),
                    true );
        }

        // Delete old table
        this.wrapper.dropEntityBlocking( tableEntity );
    }


    @Override
    public void addIndex( Context context, CatalogIndex catalogIndex ) {
        IndexType indexType;
        try {
            indexType = IndexType.valueOf( catalogIndex.method.toUpperCase() );
        } catch ( Exception e ) {
            throw new RuntimeException( "Unknown index type: " + catalogIndex.method );
        }
        Builder indexBuilder = IndexDefinition.newBuilder();
        Entity tableEntity = Entity.newBuilder()
                .setSchema( this.currentSchema.getCottontailSchema() )
                .setName( Catalog.getInstance().getColumnPlacement( getAdapterId(), catalogIndex.key.columnIds.get( 0 ) ).physicalTableName )
                .build();
        indexBuilder.getIndexBuilder()
                .setEntity( tableEntity )
                .setType( indexType )
                .setName( "idx" + catalogIndex.id );
        for ( long columnId : catalogIndex.key.columnIds ) {
            CatalogColumnPlacement placement = Catalog.getInstance().getColumnPlacement( getAdapterId(), columnId );
            indexBuilder.addColumns( placement.physicalColumnName );
        }
        this.wrapper.createIndexBlocking( indexBuilder.build() );
    }


    @Override
    public void dropIndex( Context context, CatalogIndex catalogIndex ) {
        Index.Builder indexBuilder = Index.newBuilder();
        Entity tableEntity = Entity.newBuilder()
                .setSchema( this.currentSchema.getCottontailSchema() )
                .setName( Catalog.getInstance().getColumnPlacement( getAdapterId(), catalogIndex.key.columnIds.get( 0 ) ).physicalTableName )
                .build();
        indexBuilder
                .setEntity( tableEntity )
                .setName( "idx" + catalogIndex.id );
        this.wrapper.dropIndexBlocking( indexBuilder.build() );
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
        String physicalTableName = CottontailNameUtil.getPhysicalTableName( this.getAdapterId(), table.id );
        Entity tableEntity = Entity.newBuilder()
                .setSchema( this.currentSchema.getCottontailSchema() )
                .setName( physicalTableName )
                .build();

        this.wrapper.truncateEntityBlocking( tableEntity );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn, PolyType oldType ) {
        final List<CatalogColumnPlacement> placements = this.catalog.getColumnPlacementsOnAdapterSortedByPhysicalPosition( this.getAdapterId(), catalogColumn.tableId );
        final List<ColumnDefinition> columns = this.buildColumnDefinitions( placements );

        final String currentPhysicalTableName = placements.get( 0 ).physicalTableName;
        final String newPhysicalTableName = CottontailNameUtil.incrementNameRevision( currentPhysicalTableName );

        Entity tableEntity = Entity.newBuilder()
                .setSchema( this.currentSchema.getCottontailSchema() )
                .setName( currentPhysicalTableName )
                .build();
        Entity newTableEntity = Entity.newBuilder()
                .setSchema( this.currentSchema.getCottontailSchema() )
                .setName( newPhysicalTableName )
                .build();

        EntityDefinition message = EntityDefinition.newBuilder()
                .setEntity( newTableEntity )
                .addAllColumns( columns )
                .build();

        if ( !this.wrapper.createEntityBlocking( message ) ) {
            throw new RuntimeException( "Unable to create table." );
        }

        Query query = Query.newBuilder().setFrom( From.newBuilder().setEntity( tableEntity ).build() ).build();

        Iterator<QueryResponseMessage> queryResponse = this.wrapper.query( QueryMessage.newBuilder().setQuery( query ).build() );

        List<InsertMessage> inserts = new ArrayList<>();
        From from = From.newBuilder().setEntity( newTableEntity ).build();

        queryResponse.forEachRemaining( queryResponseMessage -> {
            for ( Tuple tuple : queryResponseMessage.getResultsList() ) {
                Map<String, Data> dataMap = new HashMap<>( tuple.getDataMap() );
                Object o = PolyTypeConversionUtil.convertValue(
                        CottontailTypeUtil.dataToValue( dataMap.get( columnPlacement.physicalColumnName ), oldType ),
                        oldType,
                        catalogColumn.type );

                dataMap.put( columnPlacement.physicalColumnName, CottontailTypeUtil.toData( o, catalogColumn.type ) );

                InsertMessage insertMessage = InsertMessage.newBuilder()
                        .setTuple( Tuple.newBuilder().putAllData( dataMap ).build() )
                        .setFrom( from )
                        .build();
                inserts.add( insertMessage );

                if ( inserts.size() > 100 ) {
                    this.wrapper.insert( inserts );
                }
            }
        } );

        if ( inserts.size() > 0 ) {
            this.wrapper.insert( inserts );
        }

        for ( CatalogColumnPlacement ccp : placements ) {
            catalog.updateColumnPlacementPhysicalNames(
                    getAdapterId(),
                    ccp.columnId,
                    ccp.physicalSchemaName,
                    newPhysicalTableName,
                    ccp.physicalColumnName,
                    false );
        }

        this.wrapper.dropEntityBlocking( tableEntity );
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

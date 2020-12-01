/*
 * Copyright 2019-2020 The Polypheny Project
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
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Store;
import org.polypheny.db.adapter.cottontail.util.CottontailNameUtil;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeConversionUtil;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnPlacementException;
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
import org.vitrivr.cottontail.grpc.CottontailGrpc;
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnDefinition;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Data;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Entity;
import org.vitrivr.cottontail.grpc.CottontailGrpc.EntityDefinition;
import org.vitrivr.cottontail.grpc.CottontailGrpc.From;
import org.vitrivr.cottontail.grpc.CottontailGrpc.InsertMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Query;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.QueryResponseMessage;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Tuple;
import org.vitrivr.cottontail.grpc.CottontailGrpc.Type;
import org.vitrivr.cottontail.server.grpc.CottontailGrpcServer;


@Slf4j
public class CottontailStore extends Store {

    public static final String DESCRIPTION = "Cottontail-DB is a column store aimed at multimedia retrieval. It is optimized for classical boolean as well as vector-space retrieval.";

    public static final String ADAPTER_NAME = "Cottontail-DB";

    public static final List<AdapterSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new AdapterSettingList( "type", false, true, false, ImmutableList.of( "Standalone", "Embedded" ) ),
            new AdapterSettingString( "host", false, true, false, "localhost" ),
            new AdapterSettingInteger( "port", false, true, false, 1865 ),
            new AdapterSettingString( "database",  false, true, false, "cottontail")
    );

    // Running embedded
    private final boolean isEmbedded;
    @Expose(serialize = false, deserialize = false)
    private transient final CottontailGrpcServer embeddedServer;

    private String dbHostname;
    private int dbPort;
    private String dbName;

    private CottontailSchema currentSchema;
    @Expose(serialize = false, deserialize = false)
    private transient ManagedChannel channel;
    @Expose(serialize = false, deserialize = false)
    private transient CottontailWrapper wrapper;

    public CottontailStore( int storeId, String uniqueName, Map<String, String> settings ) {
        super( storeId, uniqueName, settings, false, false, true );
        this.dbHostname = settings.get( "host" );
        this.dbPort = Integer.parseInt( settings.get( "port" ) );
        this.dbName = settings.get( "database" );
        this.isEmbedded = settings.get( "type" ).equalsIgnoreCase( "Embedded" );

        if ( this.isEmbedded ) {
            File adapterRoot; //  = FileSystemManager.getInstance().registerDataFolder( "cottontail-db" );

            if ( Catalog.memoryCatalog ) {
                adapterRoot = FileSystemManager.getInstance().registerNewFolder( "data/temp-cottontaildb-store" );
            } else {
                adapterRoot = FileSystemManager.getInstance().registerNewFolder( "data/cottontaildb-store" );
            }
            File embeddedDir = new File( adapterRoot, "store" + getStoreId() );

            if ( !embeddedDir.exists() ) {
                if ( !embeddedDir.mkdirs() ) {
                    throw new RuntimeException( "Could not create root directory" );
                }
            }
            if ( Catalog.memoryCatalog ) {
                FileSystemManager.getInstance().recursiveDeleteFolderOnExit( "data/temp-cottontaildb-store" );
            }
//            File embeddedDir = new File( baseDir, this.dbName );
//            if ( !embeddedDir.exists() ) {
//                embeddedDir.mkdir();
//            }

            File configFile = new File( embeddedDir, "config.json" );
            if ( !configFile.exists() ) {
                try {
                    configFile.createNewFile();
                    File dataFolder = new File( embeddedDir, "data" );
                    FileWriter fileWriter = new FileWriter( configFile );
                    fileWriter.write( "{" );
                    fileWriter.write( "\"root\": \"" + dataFolder.getAbsolutePath() + "\"," );
                    fileWriter.write( "\"mapdb\": {" );
                    fileWriter.write( "\"enableMmap\": true," );
                    fileWriter.write( "\"forceUnmap\": true," );
                    fileWriter.write( "\"pageShift\": 22" );
                    fileWriter.write( "} }" );
                    fileWriter.close();
                } catch ( IOException e ) {
                    e.printStackTrace();
                }
            }
//            this.embeddedServer = null;
            this.embeddedServer = CottontailKt.embedded( configFile.getAbsolutePath() );
//            this.embeddedServer.start();
            this.dbHostname = "localhost";
            this.dbPort = 1865;
        } else {
            this.embeddedServer = null;
        }

        this.channel = NettyChannelBuilder.forAddress( this.dbHostname, this.dbPort ).usePlaintext().maxInboundMetadataSize( CottontailWrapper.maxMessageSize ).maxInboundMessageSize( CottontailWrapper.maxMessageSize ).build();
        this.wrapper = new CottontailWrapper( this.channel );
        this.wrapper.checkedCreateSchemaBlocking( CottontailGrpc.Schema.newBuilder().setName( this.dbName ).build() );
//        this.wrapper.createSchema( CottontailGrpc.Schema.newBuilder().setName( this.dbName ).build() );
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

        List<ColumnDefinition> columns = this.buildColumnDefinitions( this.catalog.getColumnPlacementsOnStore( this.getStoreId(), combinedTable.id ) );

        String physicalTableName = CottontailNameUtil.createPhysicalTableName( combinedTable.id );
        Entity tableEntity = Entity.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( physicalTableName ).build();

        EntityDefinition message = EntityDefinition.newBuilder()
                .setEntity( tableEntity )
                .addAllColumns( columns ).build();


        if ( !this.wrapper.createEntityBlocking( message ) ) {
            throw new RuntimeException( "Unable to create table." );
        }

        for ( CatalogColumnPlacement placement : this.catalog.getColumnPlacementsOnStore( this.getStoreId(), combinedTable.id ) ) {
            try {
                this.catalog.updateColumnPlacementPhysicalNames(
                        this.getStoreId(),
                        placement.columnId,
                        this.dbName,
                        physicalTableName,
                        CottontailNameUtil.createPhysicalColumnName( placement.columnId ) );
            } catch ( GenericCatalogException | UnknownColumnPlacementException e ) {
                throw new RuntimeException( e );
            }
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
            CottontailGrpc.Type columnType = CottontailTypeUtil.getPhysicalTypeRepresentation( catalogColumn.type, catalogColumn.collectionsType, ( catalogColumn.dimension != null) ? catalogColumn.dimension : 0 );
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
        String physicalTableName = CottontailNameUtil.getPhysicalTableName( this.getStoreId(), combinedTable.id );
        Entity tableEntity = Entity.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( physicalTableName ).build();

        this.wrapper.dropEntityBlocking( tableEntity );
    }


    @Override
    public void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        // TODO js(ct): Add addColumn to cottontail
        final List<CatalogColumnPlacement> placements = this.catalog.getColumnPlacementsOnStore( this.getStoreId(), catalogTable.id );
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


        // DONE TODO js(ct): Create the new table over here!
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
        for ( CatalogColumnPlacement placement : this.catalog.getColumnPlacementsOnStore( this.getStoreId(), catalogTable.id ) ) {
            try {
                this.catalog.updateColumnPlacementPhysicalNames(
                        this.getStoreId(),
                        placement.columnId,
                        this.dbName,
                        newPhysicalTableName,
                        CottontailNameUtil.createPhysicalColumnName( placement.columnId ) );
            } catch ( GenericCatalogException | UnknownColumnPlacementException e ) {
                throw new RuntimeException( e );
            }
        }

        // Delete old table
        this.wrapper.dropEntityBlocking( tableEntity );

    }


    @Override
    public void dropColumn( Context context, CatalogColumnPlacement columnPlacement ) {
        // TODO js(ct): Add dropColumn to cottontail
        final List<CatalogColumnPlacement> placements = this.catalog.getColumnPlacementsOnStore( this.getStoreId(), columnPlacement.tableId );
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


        // DONE TODO js(ct): Create the new table over here!
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
        for ( CatalogColumnPlacement placement : this.catalog.getColumnPlacementsOnStore( this.getStoreId(), columnPlacement.tableId ) ) {
            try {
                this.catalog.updateColumnPlacementPhysicalNames(
                        this.getStoreId(),
                        placement.columnId,
                        this.dbName,
                        newPhysicalTableName,
                        CottontailNameUtil.createPhysicalColumnName( placement.columnId ) );
            } catch ( GenericCatalogException | UnknownColumnPlacementException e ) {
                throw new RuntimeException( e );
            }
        }

        // Delete old table
        this.wrapper.dropEntityBlocking( tableEntity );
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
        String physicalTableName = CottontailNameUtil.getPhysicalTableName( this.getStoreId(), table.id );
        Entity tableEntity = Entity.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( physicalTableName ).build();

        this.wrapper.truncateEntityBlocking( tableEntity );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn ) {
        // TODO js(ct): Add updateColumnType to cottontail
        log.error( "UPDATE COLUMN TYPE IS NOT SUPPORTED YET!" );
        final List<CatalogColumnPlacement> placements = this.catalog.getColumnPlacementsOnStore( this.getStoreId(), catalogColumn.tableId );
        final List<ColumnDefinition> columns = this.buildColumnDefinitions( placements );
        final CatalogColumn newColumn = this.catalog.getColumn( catalogColumn.id );

        final String currentPhysicalTableName = placements.get( 0 ).physicalTableName;

        final String newPhysicalTableName = CottontailNameUtil.incrementNameRevision( currentPhysicalTableName );
        final String newPhysicalColumnName = CottontailNameUtil.createPhysicalColumnName( catalogColumn.id );

        Entity tableEntity = Entity.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( currentPhysicalTableName ).build();
        Entity newTableEntity = Entity.newBuilder().setSchema( this.currentSchema.getCottontailSchema() ).setName( newPhysicalTableName ).build();

        EntityDefinition message = EntityDefinition.newBuilder()
                .setEntity( newTableEntity )
                .addAllColumns( columns ).build();


        // DONE TODO js(ct): Create the new table over here!
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
                Object value = dataMap.get( newPhysicalColumnName );
//                dataMap.put( newPhysicalColumnName, CottontailTypeConversionUtil.convertValue( value,  ) );

                inserts.add( InsertMessage.newBuilder().setTuple( Tuple.newBuilder().putAllData( dataMap ).build() ).setFrom( from ).build() );
            }
        } );
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
    public void shutdown() {
        this.wrapper.close();
        if (this.isEmbedded) {
            this.embeddedServer.stop();
        }
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {

    }
}

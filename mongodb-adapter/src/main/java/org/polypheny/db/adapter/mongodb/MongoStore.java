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

package org.polypheny.db.adapter.mongodb;

import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.ByteString;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingBoolean;
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.Adapter.AdapterSettingString;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.adapter.mongodb.util.MongoTypeUtil;
import org.polypheny.db.catalog.Adapter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDefaultValue;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.ConfigDocker;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.docker.DockerManager.ContainerBuilder;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;

@Slf4j
@AdapterProperties(
        name = "MongoDB",
        description = "MongoDB is a document-based database system.",
        usedModes = { DeployMode.REMOTE, DeployMode.DOCKER })
@AdapterSettingBoolean(name = "persistent", defaultValue = false)
@AdapterSettingInteger(name = "port", defaultValue = 27017)
@AdapterSettingString(name = "host", defaultValue = "localhost", appliesTo = DeploySetting.REMOTE)
@AdapterSettingInteger(name = "trxLifetimeLimit", defaultValue = 1209600) // two weeks
public class MongoStore extends DataStore {

    private final String host;
    private final int port;
    private transient MongoClient client;
    private final transient TransactionProvider transactionProvider;
    private transient MongoSchema currentSchema;
    private String currentUrl;
    private final int dockerInstanceId;


    public MongoStore( int adapterId, String uniqueName, Map<String, String> settings ) {
        super( adapterId, uniqueName, settings, Boolean.parseBoolean( settings.get( "persistent" ) ) );

        this.port = Integer.parseInt( settings.get( "port" ) );

        DockerManager.Container container = new ContainerBuilder( getAdapterId(), "mongo:4.4.7", getUniqueName(), Integer.parseInt( settings.get( "instanceId" ) ) )
                .withMappedPort( 27017, port )
                .withInitCommands( Arrays.asList( "mongod", "--replSet", "poly" ) )
                .withReadyTest( this::testConnection, 20000 )
                .withAfterCommands( Arrays.asList( "mongo", "--eval", "rs.initiate()" ) )
                .build();
        this.host = container.getHost();

        DockerManager.getInstance().initialize( container ).start();

        addInformationPhysicalNames();
        enableInformationPage();

        dockerInstanceId = container.getDockerInstanceId();

        resetDockerConnection( RuntimeConfig.DOCKER_INSTANCES.getWithId( ConfigDocker.class, Integer.parseInt( settings.get( "instanceId" ) ) ) );

        this.transactionProvider = new TransactionProvider( this.client );
        MongoDatabase db = this.client.getDatabase( "admin" );
        Document configs = new Document( "setParameter", 1 );
        String trxLifetimeLimit;
        if ( settings.containsKey( "trxLifetimeLimit" ) ) {
            trxLifetimeLimit = settings.get( "trxLifetimeLimit" );
        } else {
            trxLifetimeLimit = Adapter.MONGODB.getDefaultSettings().get( "trxLifetimeLimit" );
        }
        configs.put( "transactionLifetimeLimitSeconds", Integer.parseInt( trxLifetimeLimit ) );
        db.runCommand( configs );
    }


    @Override
    public void resetDockerConnection( ConfigDocker c ) {
        if ( c.id != dockerInstanceId || c.getHost().equals( currentUrl ) ) {
            return;
        }

        MongoClientSettings mongoSettings = MongoClientSettings
                .builder()
                .applyToConnectionPoolSettings( builder -> builder
                        .maxConnectionIdleTime( 4, TimeUnit.HOURS )
                        .maxWaitTime( 1, TimeUnit.HOURS )
                        .maxConnectionLifeTime( 4, TimeUnit.HOURS ) )
                .applyToSocketSettings( builder -> builder
                        .connectTimeout( 4, TimeUnit.HOURS )
                        .readTimeout( 30, TimeUnit.MINUTES ) )
                .applyToClusterSettings( builder ->
                        builder.hosts( Collections.singletonList( new ServerAddress( c.getHost() ) ) )
                )
                .build();

        this.client = MongoClients.create( mongoSettings );
        if ( transactionProvider != null ) {
            transactionProvider.setClient( client );
        }
        this.currentUrl = c.getHost();
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        String[] splits = name.split( "_" );
        String database = splits[0] + "_" + splits[1];
        currentSchema = new MongoSchema( database, this.client, transactionProvider );
    }


    @Override
    public Table createTableSchema( CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        return currentSchema.createTable( combinedTable, columnPlacementsOnStore, getAdapterId() );
    }


    @Override
    public Schema getCurrentSchema() {
        return this.currentSchema;
    }


    @Override
    public void truncate( Context context, CatalogTable table ) {
        commitAll();
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        // DDL is auto-committed
        currentSchema.database.getCollection( getPhysicalTableName( table.id ) ).deleteMany( new Document() );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        transactionProvider.commit( xid );
    }


    public void commitAll() {
        transactionProvider.commitAll();
    }


    @Override
    public void rollback( PolyXid xid ) {
        transactionProvider.rollback( xid );
    }


    @Override
    public void shutdown() {
        DockerInstance.getInstance().destroyAll( getAdapterId() );

        removeInformationPage();
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {

    }


    @Override
    public void createTable( Context context, CatalogTable catalogTable ) {
        Catalog catalog = Catalog.getInstance();
        commitAll();
        //ClientSession session = transactionProvider.startTransaction( context.getStatement().getTransaction().getXid() );
        //context.getStatement().getTransaction().registerInvolvedAdapter( this );
        this.currentSchema.database.createCollection( getPhysicalTableName( catalogTable.id ) );

        for ( CatalogColumnPlacement placement : catalog.getColumnPlacementsOnAdapter( getAdapterId(), catalogTable.id ) ) {
            catalog.updateColumnPlacementPhysicalNames(
                    getAdapterId(),
                    placement.columnId,
                    catalogTable.getSchemaName(),
                    catalogTable.name,
                    getPhysicalColumnName( placement.getLogicalColumnName(), placement.columnId ),
                    true );
        }
    }


    @Override
    public void dropTable( Context context, CatalogTable combinedTable ) {
        commitAll();
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        //transactionProvider.startTransaction();
        this.currentSchema.database.getCollection( getPhysicalTableName( combinedTable.id ) ).drop();
    }


    @Override
    public void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        commitAll();
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        // updates all columns with this field if a default value is provided
        Document field;
        if ( catalogColumn.defaultValue != null ) {
            CatalogDefaultValue defaultValue = catalogColumn.defaultValue;
            BsonValue value;
            if ( catalogColumn.type.getFamily() == PolyTypeFamily.CHARACTER ) {
                value = new BsonString( defaultValue.value );
            } else if ( PolyType.INT_TYPES.contains( catalogColumn.type ) ) {
                value = new BsonInt32( Integer.parseInt( defaultValue.value ) );
            } else if ( PolyType.FRACTIONAL_TYPES.contains( catalogColumn.type ) ) {
                value = new BsonDouble( Double.parseDouble( defaultValue.value ) );
            } else if ( catalogColumn.type.getFamily() == PolyTypeFamily.BOOLEAN ) {
                value = new BsonBoolean( Boolean.parseBoolean( defaultValue.value ) );
            } else if ( catalogColumn.type.getFamily() == PolyTypeFamily.DATE ) {
                try {
                    value = new BsonInt64( new SimpleDateFormat( "yyyy-MM-dd" ).parse( defaultValue.value ).getTime() );
                } catch ( ParseException e ) {
                    throw new RuntimeException( e );
                }
            } else if ( catalogColumn.type.getFamily() == PolyTypeFamily.TIME ) {
                value = new BsonInt32( (int) Time.valueOf( defaultValue.value ).getTime() );
            } else if ( catalogColumn.type.getFamily() == PolyTypeFamily.TIMESTAMP ) {
                value = new BsonInt64( Timestamp.valueOf( defaultValue.value ).getTime() );
            } else if ( catalogColumn.type.getFamily() == PolyTypeFamily.BINARY ) {
                value = new BsonBinary( ByteString.parseBase64( defaultValue.value ) );
            } else {
                value = new BsonString( defaultValue.value );
            }
            if ( catalogColumn.collectionsType == PolyType.ARRAY ) {
                throw new RuntimeException( "Default values are not supported for array types" );
            }

            field = new Document().append( getPhysicalColumnName( catalogColumn ), value );
        } else {
            field = new Document().append( getPhysicalColumnName( catalogColumn ), null );
        }
        Document update = new Document().append( "$set", field );

        // DDL is auto-commit
        this.currentSchema.database.getCollection( getPhysicalTableName( catalogTable.id ) ).updateMany( new Document(), update );

        // Add physical name to placement
        catalog.updateColumnPlacementPhysicalNames(
                getAdapterId(),
                catalogColumn.id,
                currentSchema.getDatabase().getName(),
                catalogTable.name,
                getPhysicalColumnName( catalogColumn ),
                false );

    }


    private String getPhysicalColumnName( CatalogColumn catalogColumn ) {
        return getPhysicalColumnName( catalogColumn.name, catalogColumn.id );
    }


    private String getPhysicalColumnName( CatalogColumnPlacement columnPlacement ) {
        return getPhysicalColumnName( columnPlacement.getLogicalColumnName(), columnPlacement.columnId );
    }


    @Override
    public void dropColumn( Context context, CatalogColumnPlacement columnPlacement ) {
        commitAll();
        Document field = new Document().append( getPhysicalColumnName( columnPlacement ), 1 );
        Document filter = new Document().append( "$unset", field );

        context.getStatement().getTransaction().registerInvolvedAdapter( AdapterManager.getInstance().getStore( getAdapterId() ) );
        // DDL is auto-commit
        this.currentSchema.database.getCollection( getPhysicalTableName( columnPlacement.tableId ) ).updateMany( new Document(), filter );
    }


    @Override
    public void addIndex( Context context, CatalogIndex catalogIndex ) {
        commitAll();
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        HASH_FUNCTION type = HASH_FUNCTION.valueOf( catalogIndex.method.toUpperCase( Locale.ROOT ) );
        switch ( type ) {
            case SINGLE:
                List<String> columns = catalogIndex.key.getColumnNames();
                if ( columns.size() > 1 ) {
                    throw new RuntimeException( "A \"SINGLE INDEX\" can not have multiple columns." );
                }
                addCompositeIndex( catalogIndex, columns );
                break;

            case DEFAULT:
            case COMPOUND:
                addCompositeIndex( catalogIndex, catalogIndex.key.getColumnNames() );
                break;

            case MULTIKEY:
                //array
            case GEOSPATIAL:
            case TEXT:
                // stemd and stop words removed
            case HASHED:
                throw new UnsupportedOperationException( "The mongodb adapter does not yet support this index" );
        }

        Catalog.getInstance().setIndexPhysicalName( catalogIndex.id, catalogIndex.name );
    }


    private void addCompositeIndex( CatalogIndex catalogIndex, List<String> columns ) {
        Document doc = new Document();
        columns.forEach( name -> doc.append( name, 1 ) );

        IndexOptions options = new IndexOptions();
        options.unique( catalogIndex.unique );
        options.name( catalogIndex.name );

        this.currentSchema.database
                .getCollection( getPhysicalTableName( catalogIndex.key.tableId ) )
                .createIndex( doc, options );
    }


    @Override
    public void dropIndex( Context context, CatalogIndex catalogIndex ) {
        commitAll();
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        this.currentSchema.database.getCollection( getPhysicalTableName( catalogIndex.key.tableId ) ).dropIndex( catalogIndex.name );
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn, PolyType polyType ) {
        String name = columnPlacement.physicalColumnName;
        BsonDocument filter = new BsonDocument();
        List<BsonDocument> updates = Collections.singletonList( new BsonDocument( "$set", new BsonDocument( name, new BsonDocument( "$convert", new BsonDocument()
                .append( "input", new BsonString( "$" + name ) )
                .append( "to", new BsonInt32( MongoTypeUtil.getTypeNumber( catalogColumn.type ) ) ) ) ) ) );

        this.currentSchema.database.getCollection( columnPlacement.physicalTableName ).updateMany( filter, updates );
    }


    @Override
    public List<AvailableIndexMethod> getAvailableIndexMethods() {
        return Arrays.stream( HASH_FUNCTION.values() )
                .map( HASH_FUNCTION::asMethod )
                .collect( Collectors.toList() );
    }


    @Override
    public AvailableIndexMethod getDefaultIndexMethod() {
        return HASH_FUNCTION.COMPOUND.asMethod();
    }


    @Override
    public List<FunctionalIndexInfo> getFunctionalIndexes( CatalogTable catalogTable ) {
        return ImmutableList.of();
    }


    public static String getPhysicalColumnName( String name, long id ) {
        if ( name.startsWith( "_" ) ) {
            return name;
        }
        // we can simply use ids as our physical column names as MongoDB allows this
        return "col" + id;
    }


    public static String getPhysicalTableName( long id ) {
        return "tab-" + id;
    }


    private boolean testConnection() {
        MongoClient client = null;
        try {
            MongoClientSettings mongoSettings = MongoClientSettings
                    .builder()
                    .applyToClusterSettings( builder ->
                            builder.hosts( Collections.singletonList( new ServerAddress( host, port ) ) )
                    )
                    .build();

            client = MongoClients.create( mongoSettings );
            MongoDatabase database = client.getDatabase( "admin" );
            Document serverStatus = database.runCommand( new Document( "serverStatus", 1 ) );
            Map<?, ?> connections = (Map<?, ?>) serverStatus.get( "connections" );
            connections.get( "current" );
            client.close();
            return true;
        } catch ( Exception e ) {
            if ( client != null ) {
                client.close();
            }
            return false;
        }
    }


    private enum HASH_FUNCTION {
        DEFAULT,
        COMPOUND,
        SINGLE,
        MULTIKEY,
        GEOSPATIAL,
        TEXT,
        HASHED;


        public AvailableIndexMethod asMethod() {
            return new AvailableIndexMethod( name().toLowerCase( Locale.ROOT ), name() + " INDEX" );
        }
    }

}

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

package org.polypheny.db.adapter.mongodb;

import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.pf4j.Extension;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.DeployMode.DeploySetting;
import org.polypheny.db.adapter.DocumentModifyDelegate;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.catalog.catalogs.DocAdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalDefaultValue;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalField;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.docker.DockerContainer;
import org.polypheny.db.docker.DockerContainer.HostAndPort;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.types.ModifiableTable;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.Pair;

public class MongoPlugin extends PolyPlugin {


    public static final String ADAPTER_NAME = "MongoDB";
    private long id;


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public MongoPlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void afterCatalogInit() {
        this.id = AdapterManager.addAdapterTemplate( MongoStore.class, ADAPTER_NAME, MongoStore::new );
    }


    @Override
    public void stop() {
        AdapterManager.removeAdapterTemplate( id );
    }


    @Slf4j
    @Extension
    @AdapterProperties(
            name = "MongoDB",
            description = "MongoDB is a document-oriented database system.",
            usedModes = { DeployMode.REMOTE, DeployMode.DOCKER },
            defaultMode = DeployMode.DOCKER)
    @AdapterSettingInteger(name = "port", defaultValue = 27017, appliesTo = DeploySetting.REMOTE)
    @AdapterSettingString(name = "host", defaultValue = "localhost", appliesTo = DeploySetting.REMOTE)
    @AdapterSettingInteger(name = "trxLifetimeLimit", defaultValue = 1209600) // two weeks
    public static class MongoStore extends DataStore<DocAdapterCatalog> {

        private final String DEFAULT_DATABASE = "public";

        @Delegate(excludes = Exclude.class)
        private final DocumentModifyDelegate delegate;

        private String host;
        private int port;
        private DockerContainer container;
        private transient MongoClient client;
        private final transient TransactionProvider transactionProvider;
        @Getter
        private transient MongoNamespace currentNamespace;

        @Getter
        private final List<PolyType> unsupportedTypes = ImmutableList.of();


        public MongoStore( final long adapterId, final String uniqueName, final Map<String, String> settings ) {
            super( adapterId, uniqueName, settings, true, new DocAdapterCatalog( adapterId ) );

            if ( deployMode == DeployMode.DOCKER ) {
                if ( settings.getOrDefault( "deploymentId", "" ).isEmpty() ) {
                    int instanceId = Integer.parseInt( settings.get( "instanceId" ) );
                    DockerInstance instance = DockerManager.getInstance().getInstanceById( instanceId )
                            .orElseThrow( () -> new GenericRuntimeException( "No docker instance with id " + instanceId ) );
                    try {
                        this.container = instance.newBuilder( "polypheny/mongo:latest", getUniqueName() )
                                .withCommand( Arrays.asList( "mongod", "--replSet", "poly" ) )
                                .createAndStart();
                    } catch ( IOException e ) {
                        throw new GenericRuntimeException( e );
                    }

                    if ( !container.waitTillStarted( this::testConnection, 20000 ) ) {
                        container.destroy();
                        throw new GenericRuntimeException( "Failed to start Mongo container" );
                    }

                    try {
                        int exitCode = container.execute( Arrays.asList( "mongo", "--eval", "rs.initiate()" ) );
                        if ( exitCode != 0 ) {
                            throw new IOException( "Command returned non-zero exit code" );
                        }
                    } catch ( IOException e ) {
                        container.destroy();
                        throw new GenericRuntimeException( " Command 'mongo --eval rs.initiate()' failed" );
                    }

                    this.deploymentId = container.getContainerId();
                    settings.put( "deploymentId", this.deploymentId );
                    updateSettings( settings );
                } else {
                    deploymentId = settings.get( "deploymentId" );
                    DockerManager.getInstance(); // Make sure docker instances are loaded.  Very hacky, but it works.
                    container = DockerContainer.getContainerByUUID( deploymentId )
                            .orElseThrow( () -> new GenericRuntimeException( "Could not find docker container with id " + deploymentId ) );
                    if ( !testConnection() ) {
                        throw new GenericRuntimeException( "Could not connect to container" );
                    }
                }

                resetDockerConnection();
            } else if ( deployMode == DeployMode.REMOTE ) {
                this.host = settings.get( "host" );
                this.port = Integer.parseInt( settings.get( "port" ) );

                MongoClientSettings mongoSettings = MongoClientSettings
                        .builder()
                        .applyToClusterSettings( builder ->
                                builder.hosts( Collections.singletonList( new ServerAddress( host, port ) ) )
                        )
                        .build();

                this.client = MongoClients.create( mongoSettings );
            } else {
                throw new GenericRuntimeException( "Not supported deploy mode: " + deployMode.name() );
            }

            addInformationPhysicalNames();
            enableInformationPage();

            this.transactionProvider = new TransactionProvider( this.client );
            MongoDatabase db = this.client.getDatabase( "admin" );
            Document configs = new Document( "setParameter", 1 );
            String trxLifetimeLimit = getSetting( settings, "trxLifetimeLimit" );
            configs.put( "transactionLifetimeLimitSeconds", Integer.parseInt( trxLifetimeLimit ) );
            configs.put( "cursorTimeoutMillis", 6 * 600000 );
            db.runCommand( configs );

            this.delegate = new DocumentModifyDelegate( this, adapterCatalog );
        }


        private String getSetting( Map<String, String> settings, String key ) {
            return settings.get( key );
        }


        @Override
        public void resetDockerConnection() {
            DockerContainer c = DockerContainer.getContainerByUUID( deploymentId )
                    .orElseThrow( () -> new GenericRuntimeException( "Could not find docker container with id " + deploymentId ) );

            HostAndPort hp = container.connectToContainer( 27017 );
            host = hp.host();
            port = hp.port();

            MongoClientSettings mongoSettings = MongoClientSettings
                    .builder()
                    .applyToClusterSettings( builder ->
                            builder.hosts( Collections.singletonList( new ServerAddress( host, port ) ) )
                    )
                    .build();

            this.client = MongoClients.create( mongoSettings );
            if ( transactionProvider != null ) {
                transactionProvider.setClient( client );
            }
        }


        @Override
        public void updateNamespace( String name, long id ) {
            String[] splits = name.split( "_" );
            String database = name;
            if ( splits.length >= 2 ) {
                database = splits[0] + "_" + splits[1];
            }
            database = database.toLowerCase( Locale.ROOT ) + "_" + id;
            currentNamespace = new MongoNamespace( id, database, this.client, transactionProvider, this );
        }


        @Override
        public void truncate( Context context, long allocId ) {
            commitAll();
            PhysicalEntity physical = adapterCatalog.fromAllocation( allocId, PhysicalEntity.class );
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            // DDL is auto-committed
            currentNamespace.database.getCollection( physical.name ).deleteMany( new Document() );
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
            DockerContainer.getContainerByUUID( deploymentId ).ifPresent( DockerContainer::destroy );

            removeInformationPage();
        }


        @Override
        protected void reloadSettings( List<String> updatedSettings ) {

        }


        @Override
        public List<PhysicalEntity> createCollection( Context context, LogicalCollection logical, AllocationCollection allocation ) {
            commitAll();
            String name = getPhysicalEntityName( allocation.id );

            if ( adapterCatalog.getNamespace( allocation.namespaceId ) == null ) {
                updateNamespace( DEFAULT_DATABASE, allocation.namespaceId );
                adapterCatalog.addNamespace( allocation.namespaceId, currentNamespace );
            }

            PhysicalCollection table = adapterCatalog.createCollection(
                    DEFAULT_DATABASE,
                    name,
                    logical,
                    allocation );

            this.currentNamespace.database.createCollection( name );

            MongoEntity physical = this.currentNamespace.createEntity( table, List.of() );

            this.adapterCatalog.addPhysical( allocation, physical );

            return List.of( physical );
        }


        @Override
        public void dropCollection( Context context, AllocationCollection allocation ) {
            commitAll();
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            PhysicalEntity collection = adapterCatalog.fromAllocation( allocation.id, PhysicalEntity.class );
            this.currentNamespace.database.getCollection( collection.name ).drop();

            adapterCatalog.removeAllocAndPhysical( allocation.id );
        }


        @Override
        public void renameLogicalColumn( long id, String newColumnName ) {
            Stream<Long> allocIds = adapterCatalog.fields.values().stream().filter( f -> f.id == id ).map( f -> f.allocId ).distinct();
            adapterCatalog.renameLogicalColumn( id, newColumnName );
            allocIds.forEach( allocId -> {
                MongoEntity entity = adapterCatalog.fromAllocation( allocId, MongoEntity.class );
                this.currentNamespace.createEntity( entity, adapterCatalog.getFields( allocId ) );
            } );
        }


        @Override
        public void dropTable( Context context, long allocId ) {
            commitAll();
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            PhysicalEntity physical = adapterCatalog.fromAllocation( allocId, PhysicalEntity.class );

            this.currentNamespace.database.getCollection( physical.name ).drop();
            adapterCatalog.removeAllocAndPhysical( allocId );
        }


        @Override
        public void addColumn( Context context, long allocId, LogicalColumn logicalColumn ) {
            commitAll();
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            MongoEntity physical = adapterCatalog.fromAllocation( allocId, MongoEntity.class );
            String physicalName = getPhysicalColumnName( logicalColumn );
            // updates all columns with this field if a default value is provided
            PhysicalColumn column = adapterCatalog.createColumn( physicalName, allocId, physical.fields.size() - 1, logicalColumn );

            List<PhysicalField> fields = new ArrayList<>( physical.fields );
            fields.add( column );

            physical.toBuilder().fields( fields ).build();

            Document field;
            if ( logicalColumn.defaultValue != null ) {
                LogicalDefaultValue defaultValue = logicalColumn.defaultValue;

                BsonValue value = BsonUtil.getAsBson( defaultValue.value, defaultValue.type, currentNamespace.getBucket() );

                field = new Document().append( getPhysicalColumnName( logicalColumn ), value );
            } else {
                field = new Document().append( getPhysicalColumnName( logicalColumn ), null );
            }
            Document update = new Document().append( "$set", field );

            // DDL is auto-commit
            this.currentNamespace.database.getCollection( physical.name ).updateMany( new Document(), update );

            refreshEntity( allocId );

        }


        private String getPhysicalColumnName( LogicalColumn catalogColumn ) {
            return getPhysicalColumnName( catalogColumn.id );
        }


        @Override
        public void dropColumn( Context context, long allocId, long columnId ) {
            commitAll();
            MongoEntity physical = adapterCatalog.fromAllocation( allocId, MongoEntity.class );
            PhysicalColumn column = adapterCatalog.getColumn( columnId, allocId );

            Document field = new Document().append( column.name, 1 );
            Document filter = new Document().append( "$unset", field );

            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            // DDL is auto-commit
            this.currentNamespace.database.getCollection( physical.name ).updateMany( new Document(), filter );
            adapterCatalog.dropColumn( allocId, columnId );

            physical = physical.toBuilder().fields( physical.fields.stream().filter( f -> f.id != columnId ).toList() ).build();
            adapterCatalog.replacePhysical( physical );
        }


        @Override
        public String addIndex( Context context, LogicalIndex index, AllocationTable allocation ) {
            commitAll();
            MongoEntity physical = adapterCatalog.fromAllocation( allocation.id, MongoEntity.class );
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            IndexTypes type = IndexTypes.valueOf( index.method.toUpperCase( Locale.ROOT ) );

            String physicalIndexName = getPhysicalIndexName( index );

            switch ( type ) {
                case SINGLE:
                    List<String> columns = index.key.getFieldNames();
                    if ( columns.size() > 1 ) {
                        throw new GenericRuntimeException( "A \"SINGLE INDEX\" can not have multiple columns." );
                    }
                    addCompositeIndex( index, columns, physical, physicalIndexName );
                    break;

                case DEFAULT:
                case COMPOUND:
                    addCompositeIndex( index, index.key.getFieldNames(), physical, physicalIndexName );
                    break;
/*
            case MULTIKEY:
                //array
            case GEOSPATIAL:
            case TEXT:
                // stemd and stop words removed
            case HASHED:
                throw new UnsupportedOperationException( "The MongoDB adapter does not yet support this type of index: " + type );*/
            }

            return physicalIndexName;
        }


        @Override
        public void dropIndex( Context context, LogicalIndex index, long allocId ) {
            commitAll();
            MongoEntity physical = adapterCatalog.fromAllocation( allocId, MongoEntity.class );
            context.getStatement().getTransaction().registerInvolvedAdapter( this );

            this.currentNamespace.database.getCollection( physical.name ).dropIndex( index.physicalName + "_" + allocId );

        }


        private String getPhysicalIndexName( LogicalIndex index ) {
            return "idx" + index.key.entityId + "_" + index.id + "_" + index.name;
        }


        private void addCompositeIndex( LogicalIndex index, List<String> columns, PhysicalEntity physical, String physicalIndexName ) {
            Document doc = new Document();

            Pair.zip( index.key.fieldIds, columns ).forEach( p -> doc.append( getPhysicalColumnName( p.left ), 1 ) );

            IndexOptions options = new IndexOptions();
            options.unique( index.unique );
            options.name( physicalIndexName );

            this.currentNamespace.database
                    .getCollection( physical.name ).listIndexes().forEach( i -> {
                        if ( i.get( "key" ).equals( doc ) ) {
                            this.currentNamespace.database.getCollection( physical.name ).dropIndex( i.getString( "name" ) );
                        }
                    } );

            this.currentNamespace.database
                    .getCollection( physical.name )
                    .createIndex( doc, options );
        }


        @Override
        public void updateColumnType( Context context, long allocId, LogicalColumn newCol ) {
            PhysicalColumn column = adapterCatalog.updateColumnType( allocId, newCol );
            MongoEntity physical = adapterCatalog.fromAllocation( allocId, MongoEntity.class );

            List<PhysicalField> fields = new ArrayList<>( physical.fields.stream().filter( f -> f.id != newCol.id ).toList() );
            fields.add( column );
            fields = fields.stream().map( f -> f.unwrap( PhysicalColumn.class ).orElseThrow() ).sorted( Comparator.comparingInt( f -> f.position ) ).map( f -> f.unwrapOrThrow( PhysicalField.class ) ).toList();

            BsonDocument filter = new BsonDocument();
            List<BsonDocument> updates = Collections.singletonList( new BsonDocument( "$set", new BsonDocument( physical.name, new BsonDocument( "$convert", new BsonDocument()
                    .append( "input", new BsonString( "$" + physical.name ) )
                    .append( "to", new BsonInt32( BsonUtil.getTypeNumber( newCol.type ) ) ) ) ) ) );

            this.currentNamespace.database.getCollection( physical.name ).updateMany( filter, updates );

            adapterCatalog.replacePhysical( currentNamespace.createEntity( physical, fields ) );
        }


        @Override
        public List<IndexMethodModel> getAvailableIndexMethods() {
            return Arrays.stream( IndexTypes.values() )
                    .map( IndexTypes::asMethod )
                    .toList();
        }


        @Override
        public IndexMethodModel getDefaultIndexMethod() {
            return IndexTypes.COMPOUND.asMethod();
        }


        @Override
        public List<FunctionalIndexInfo> getFunctionalIndexes( LogicalTable table ) {
            return ImmutableList.of();
        }


        public static String getPhysicalColumnName( long id ) {
            /*if ( name.startsWith( "_" ) ) {
                return name;
            }*/
            // we can simply use ids as our physical column names as MongoDB allows this
            return "col" + id;
        }


        public static String getPhysicalEntityName( long tableId ) {
            return "e-" + tableId;
        }


        private boolean testConnection() {
            MongoClient client = null;
            if ( container == null ) {
                return false;
            }

            HostAndPort hp = container.connectToContainer( 27017 );
            host = hp.host();
            port = hp.port();

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


        @Override
        public AlgNode getDocModify( long allocId, DocumentModify<?> modify, AlgBuilder builder ) {
            PhysicalCollection collection = adapterCatalog.fromAllocation( allocId, PhysicalCollection.class );
            if ( collection.unwrap( ModifiableTable.class ).isPresent() ) {
                return null;
            }
            return collection.unwrap( ModifiableTable.class ).get().toModificationTable(
                    modify.getCluster(),
                    modify.getTraitSet(),
                    collection,
                    modify.getInput(),
                    modify.getOperation(),
                    null,
                    null );
        }


        @Override
        public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
            commitAll();

            if ( this.currentNamespace == null ) {
                updateNamespace( DEFAULT_DATABASE, allocation.table.id );
                adapterCatalog.addNamespace( allocation.table.namespaceId, currentNamespace );
            }

            String physicalTableName = getPhysicalEntityName( allocation.table.id );
            PhysicalTable physical = adapterCatalog.createTable(
                    logical.getTable().getNamespaceName(),
                    physicalTableName,
                    allocation.columns.stream().collect( Collectors.toMap( c -> c.columnId, c -> getPhysicalColumnName( c.columnId ) ) ),
                    logical.table,
                    logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c ) ),
                    logical.pkIds,
                    allocation );

            this.adapterCatalog.addPhysical( allocation.table, this.currentNamespace.createEntity( physical, physical.columns ) );
            return refreshEntity( allocation.table.id );
        }


        public List<PhysicalEntity> refreshEntity( long allocId ) {
            PhysicalEntity physical = adapterCatalog.fromAllocation( allocId, PhysicalEntity.class );
            List<? extends PhysicalField> fields = adapterCatalog.getFields( allocId );
            adapterCatalog.replacePhysical( currentNamespace.createEntity( physical, fields ) );
            return List.of( physical );
        }


        private enum IndexTypes {
            DEFAULT,
            COMPOUND,
            SINGLE;
            /*MULTIKEY,
            GEOSPATIAL,
            TEXT,
            HASHED;*/


            public IndexMethodModel asMethod() {
                return new IndexMethodModel( name().toLowerCase( Locale.ROOT ), name() + " INDEX" );
            }
        }

    }


    @SuppressWarnings("unused")
    public interface Exclude {

        AlgNode getDocModify( long allocId, DocumentModify<?> modify, AlgBuilder builder );

        AlgNode getDocumentScan( long allocId, AlgBuilder builder );

        void refreshCollection( long allocId );

        void createCollection( Context context, LogicalCollection logical, AllocationCollection allocation );

        void dropCollection( Context context, AllocationCollection allocation );

        void dropColumn( Context context, long allocId, long columnId );

        void dropTable( Context context, long allocId );

        void updateColumnType( Context context, long allocId, LogicalColumn newCol );

        void addColumn( Context context, long allocId, LogicalColumn logicalColumn );

        void refreshTable( long allocId );

        void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocationWrapper );

        String addIndex( Context context, LogicalIndex logicalIndex, AllocationTable allocation );

        void dropIndex( Context context, LogicalIndex index, long allocId );

        public void renameLogicalColumn( long id, String newColumnName );

    }

}

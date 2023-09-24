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

package org.polypheny.db.adapter.mongodb;

import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.experimental.Delegate;
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
import org.polypheny.db.catalog.catalogs.DocStoreCatalog;
import org.polypheny.db.catalog.entity.CatalogDefaultValue;
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
import org.polypheny.db.type.PolyTypeFamily;
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
    public static class MongoStore extends DataStore<DocStoreCatalog> {

        private String DEFAULT_DATABASE;

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


        public MongoStore( long adapterId, String uniqueName, Map<String, String> settings ) {
            super( adapterId, uniqueName, settings, true, new DocStoreCatalog( adapterId ) );

            if ( deployMode == DeployMode.DOCKER ) {
                if ( settings.getOrDefault( "deploymentId", "" ).isEmpty() ) {
                    int instanceId = Integer.parseInt( settings.get( "instanceId" ) );
                    DockerInstance instance = DockerManager.getInstance().getInstanceById( instanceId )
                            .orElseThrow( () -> new RuntimeException( "No docker instance with id " + instanceId ) );
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
                            .orElseThrow( () -> new RuntimeException( "Could not find docker container with id " + deploymentId ) );
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
                throw new GenericRuntimeException( "Unknown deploy mode: " + deployMode.name() );
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

            this.delegate = new DocumentModifyDelegate( this, storeCatalog );
        }


        private String getSetting( Map<String, String> settings, String key ) {
            return settings.get( key );
        }


        @Override
        public void resetDockerConnection() {
            DockerContainer c = DockerContainer.getContainerByUUID( deploymentId )
                    .orElseThrow( () -> new RuntimeException( "Could not find docker container with id " + deploymentId ) );

            HostAndPort hp = container.connectToContainer( 27017 );
            host = hp.getHost();
            port = hp.getPort();

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
            currentNamespace = new MongoNamespace( id, database, this.client, transactionProvider, this );
        }


        @Override
        public void truncate( Context context, long allocId ) {
            commitAll();
            PhysicalTable physical = storeCatalog.fromAllocation( allocId, PhysicalTable.class );
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
        public void createCollection( Context context, LogicalCollection collection, AllocationCollection allocation ) {
            commitAll();
            String name = getPhysicalTableName( allocation.id, adapterId );
            /*if ( this.currentNamespace == null ) {
                createNewSchema( null, catalogCollection.getNamespaceName(), catalogCollection.namespaceId );
            }*/
            if ( storeCatalog.getNamespace( allocation.namespaceId ) == null ) {
                updateNamespace( DEFAULT_DATABASE, allocation.namespaceId );
                storeCatalog.addNamespace( allocation.namespaceId, currentNamespace );
            }

            //String physicalCollectionName = getPhysicalTableName( catalogCollection.id, adapterId );
            this.currentNamespace.database.createCollection( name );

        }


        @Override
        public void dropCollection( Context context, AllocationCollection allocation ) {
            commitAll();
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            PhysicalCollection collection = storeCatalog.fromAllocation( allocation.id, PhysicalCollection.class );
            this.currentNamespace.database.getCollection( collection.name ).drop();

            storeCatalog.removePhysical( allocation.id );
        }


        @Override
        public void dropTable( Context context, long allocId ) {
            commitAll();
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            PhysicalTable physical = storeCatalog.fromAllocation( allocId, PhysicalTable.class );

            this.currentNamespace.database.getCollection( physical.name ).drop();
            storeCatalog.removePhysical( allocId );
        }


        @Override
        public void addColumn( Context context, long allocId, LogicalColumn logicalColumn ) {
            commitAll();
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            PhysicalTable physical = storeCatalog.fromAllocation( allocId, PhysicalTable.class );
            String physicalName = getPhysicalColumnName( logicalColumn );
            // updates all columns with this field if a default value is provided
            PhysicalColumn column = storeCatalog.addColumn( physicalName, allocId, physical.columns.size() - 1, logicalColumn );

            Document field;
            if ( logicalColumn.defaultValue != null ) {
                CatalogDefaultValue defaultValue = logicalColumn.defaultValue;
                BsonValue value;
                if ( logicalColumn.type.getFamily() == PolyTypeFamily.CHARACTER ) {
                    value = new BsonString( defaultValue.value );
                } else if ( PolyType.INT_TYPES.contains( logicalColumn.type ) ) {
                    value = new BsonInt32( Integer.parseInt( defaultValue.value ) );
                } else if ( PolyType.FRACTIONAL_TYPES.contains( logicalColumn.type ) ) {
                    value = new BsonDouble( Double.parseDouble( defaultValue.value ) );
                } else if ( logicalColumn.type.getFamily() == PolyTypeFamily.BOOLEAN ) {
                    value = new BsonBoolean( Boolean.parseBoolean( defaultValue.value ) );
                } else if ( logicalColumn.type.getFamily() == PolyTypeFamily.DATE ) {
                    try {
                        value = new BsonInt64( new SimpleDateFormat( "yyyy-MM-dd" ).parse( defaultValue.value ).getTime() );
                    } catch ( ParseException e ) {
                        throw new GenericRuntimeException( e );
                    }
                } else if ( logicalColumn.type.getFamily() == PolyTypeFamily.TIME ) {
                    value = new BsonInt32( (int) Time.valueOf( defaultValue.value ).getTime() );
                } else if ( logicalColumn.type.getFamily() == PolyTypeFamily.TIMESTAMP ) {
                    value = new BsonInt64( Timestamp.valueOf( defaultValue.value ).getTime() );
                } else if ( logicalColumn.type.getFamily() == PolyTypeFamily.BINARY ) {
                    value = new BsonBinary( ByteString.parseBase64( defaultValue.value ) );
                } else {
                    value = new BsonString( defaultValue.value );
                }
                if ( logicalColumn.collectionsType == PolyType.ARRAY ) {
                    throw new GenericRuntimeException( "Default values are not supported for array types" );
                }

                field = new Document().append( getPhysicalColumnName( logicalColumn ), value );
            } else {
                field = new Document().append( getPhysicalColumnName( logicalColumn ), null );
            }
            Document update = new Document().append( "$set", field );

            // DDL is auto-commit
            this.currentNamespace.database.getCollection( physical.name ).updateMany( new Document(), update );

        }


        private String getPhysicalColumnName( LogicalColumn catalogColumn ) {
            return getPhysicalColumnName( catalogColumn.id );
        }


        @Override
        public void dropColumn( Context context, long allocId, long columnId ) {
            commitAll();
            PhysicalTable physical = storeCatalog.fromAllocation( allocId, PhysicalTable.class );
            PhysicalColumn column = storeCatalog.getColumn( columnId, allocId );

            Document field = new Document().append( column.name, 1 );
            Document filter = new Document().append( "$unset", field );

            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            // DDL is auto-commit
            this.currentNamespace.database.getCollection( physical.name ).updateMany( new Document(), filter );
            storeCatalog.dropColumn( allocId, columnId );
        }


        @Override
        public String addIndex( Context context, LogicalIndex logicalIndex, AllocationTable allocation ) {
            commitAll();
            PhysicalTable physical = storeCatalog.fromAllocation( allocation.id, PhysicalTable.class );
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            IndexTypes type = IndexTypes.valueOf( logicalIndex.method.toUpperCase( Locale.ROOT ) );

            String physicalIndexName = getPhysicalIndexName( logicalIndex );

            switch ( type ) {
                case SINGLE:
                    List<String> columns = logicalIndex.key.getColumnNames();
                    if ( columns.size() > 1 ) {
                        throw new RuntimeException( "A \"SINGLE INDEX\" can not have multiple columns." );
                    }
                    addCompositeIndex( logicalIndex, columns, physical, physicalIndexName );
                    break;

                case DEFAULT:
                case COMPOUND:
                    addCompositeIndex( logicalIndex, logicalIndex.key.getColumnNames(), physical, physicalIndexName );
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


        private String getPhysicalIndexName( LogicalIndex catalogIndex ) {
            return "idx" + catalogIndex.key.tableId + "_" + catalogIndex.id;
        }


        private void addCompositeIndex( LogicalIndex index, List<String> columns, PhysicalEntity physical, String physicalIndexName ) {
            Document doc = new Document();

            Pair.zip( index.key.columnIds, columns ).forEach( p -> doc.append( getPhysicalColumnName( p.left ), 1 ) );

            IndexOptions options = new IndexOptions();
            options.unique( index.unique );
            options.name( physicalIndexName );

            this.currentNamespace.database
                    .getCollection( physical.name )
                    .createIndex( doc, options );
        }


        @Override
        public void updateColumnType( Context context, long allocId, LogicalColumn newCol ) {
            PhysicalColumn column = storeCatalog.updateColumnType( allocId, newCol );
            PhysicalTable physical = storeCatalog.fromAllocation( allocId, PhysicalTable.class );

            BsonDocument filter = new BsonDocument();
            List<BsonDocument> updates = Collections.singletonList( new BsonDocument( "$set", new BsonDocument( physical.name, new BsonDocument( "$convert", new BsonDocument()
                    .append( "input", new BsonString( "$" + physical.name ) )
                    .append( "to", new BsonInt32( BsonUtil.getTypeNumber( newCol.type ) ) ) ) ) ) );

            this.currentNamespace.database.getCollection( physical.name ).updateMany( filter, updates );
        }


        @Override
        public List<IndexMethodModel> getAvailableIndexMethods() {
            return Arrays.stream( IndexTypes.values() )
                    .map( IndexTypes::asMethod )
                    .collect( Collectors.toList() );
        }


        @Override
        public IndexMethodModel getDefaultIndexMethod() {
            return IndexTypes.COMPOUND.asMethod();
        }


        @Override
        public List<FunctionalIndexInfo> getFunctionalIndexes( LogicalTable catalogTable ) {
            return ImmutableList.of();
        }


        public static String getPhysicalColumnName( long id ) {
            /*if ( name.startsWith( "_" ) ) {
                return name;
            }*/
            // we can simply use ids as our physical column names as MongoDB allows this
            return "col" + id;
        }


        public static String getPhysicalTableName( long tableId, long partitionId ) {
            String physicalTableName = "tab-" + tableId;
            if ( partitionId >= 0 ) {
                physicalTableName += "_part" + partitionId;
            }
            return physicalTableName;
        }


        private boolean testConnection() {
            MongoClient client = null;
            if ( container == null ) {
                return false;
            }

            HostAndPort hp = container.connectToContainer( 27017 );
            host = hp.getHost();
            port = hp.getPort();

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
            PhysicalCollection collection = storeCatalog.fromAllocation( allocId, PhysicalCollection.class );
            if ( collection.unwrap( ModifiableTable.class ) == null ) {
                return null;
            }
            return collection.unwrap( ModifiableTable.class ).toModificationAlg(
                    modify.getCluster(),
                    modify.getTraitSet(),
                    collection,
                    modify.getInput(),
                    modify.getOperation(),
                    null,
                    null );
        }


        @Override
        public void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
            commitAll();

            if ( this.currentNamespace == null ) {
                updateNamespace( DEFAULT_DATABASE, allocation.table.id );
                storeCatalog.addNamespace( allocation.table.namespaceId, currentNamespace );
            }

            String physicalTableName = getPhysicalTableName( logical.table.id, allocation.table.id );
            PhysicalTable physical = storeCatalog.createTable(
                    logical.getTable().getNamespaceName(),
                    physicalTableName,
                    allocation.columns.stream().collect( Collectors.toMap( c -> c.columnId, c -> getPhysicalColumnName( c.columnId ) ) ),
                    logical.table,
                    logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c ) ),
                    allocation );

            this.storeCatalog.addPhysical( allocation.table, this.currentNamespace.createEntity( physical, physical.columns ) );
        }


        @Override
        public void refreshTable( long allocId ) {
            PhysicalEntity physical = storeCatalog.fromAllocation( allocId, PhysicalEntity.class );
            List<? extends PhysicalField> fields = storeCatalog.getFields( allocId );
            storeCatalog.replacePhysical( currentNamespace.createEntity( physical, fields ) );
        }


        @Override
        public void refreshCollection( long allocId ) {
            PhysicalEntity physical = storeCatalog.fromAllocation( allocId, PhysicalEntity.class );
            List<? extends PhysicalField> fields = storeCatalog.getFields( allocId );
            storeCatalog.replacePhysical( this.currentNamespace.createEntity( physical, fields ) );
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

    }

}

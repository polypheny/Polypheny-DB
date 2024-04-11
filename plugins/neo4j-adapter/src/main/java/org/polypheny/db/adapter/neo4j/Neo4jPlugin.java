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

package org.polypheny.db.adapter.neo4j;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.Neo4jException;
import org.pf4j.Extension;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DataStore.IndexMethodModel;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.GraphModifyDelegate;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.neo4j.types.NestedSingleType;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.catalog.catalogs.GraphAdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalDefaultValue;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalField;
import org.polypheny.db.catalog.entity.physical.PhysicalGraph;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.docker.DockerContainer;
import org.polypheny.db.docker.DockerContainer.HostAndPort;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.PasswordGenerator;


public class Neo4jPlugin extends PolyPlugin {


    public static final String ADAPTER_NAME = "Neo4j";
    private long id;


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public Neo4jPlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void afterCatalogInit() {
        this.id = AdapterManager.addAdapterTemplate( Neo4jStore.class, ADAPTER_NAME, Neo4jStore::new );
    }


    @Override
    public void stop() {
        AdapterManager.removeAdapterTemplate( id );
    }


    public static String getPhysicalEntityName( long namespaceId, long allocId ) {
        return String.format( "n_%d_entity_%d_", namespaceId, allocId );
    }


    public static String getPhysicalNamespaceName( long id ) {
        return String.format( "namespace_%d", id );
    }


    public static String getPhysicalFieldName( long id ) {
        return String.format( "field_%d", id );
    }


    private static String getPhysicalGraphName( long id ) {
        return String.format( "graph_%d", id );
    }


    private static String getMappingLabel( long id ) {
        return String.format( "___n_%d___", id );
    }


    @Slf4j
    @AdapterProperties(
            name = "Neo4j",
            description = "Neo4j is a graph-model based database system. It stores data in a graph structure which consists of nodes and edges.",
            usedModes = { DeployMode.DOCKER, DeployMode.REMOTE },
            defaultMode = DeployMode.DOCKER)
    @Extension
    public static class Neo4jStore extends DataStore<GraphAdapterCatalog> {

        private final String DEFAULT_DATABASE = "public";
        @Delegate(excludes = Exclude.class)
        private final GraphModifyDelegate delegate;

        private int port;
        private final String user;
        private final Session session;
        private final DockerContainer container;
        private Driver db;
        private final String pass;
        private final AuthToken auth;
        @Getter
        private NeoNamespace currentNamespace;

        @Getter
        private NeoGraph currentGraph;

        private final TransactionProvider transactionProvider;
        private String host;


        public Neo4jStore( final long adapterId, final String uniqueName, final Map<String, String> adapterSettings ) {
            super( adapterId, uniqueName, adapterSettings, true, new GraphAdapterCatalog( adapterId ) );

            this.user = "neo4j";
            if ( !settings.containsKey( "password" ) ) {
                this.pass = PasswordGenerator.generatePassword();
                settings.put( "password", this.pass );
                updateSettings( settings );
            } else {
                this.pass = settings.get( "password" );
            }
            this.auth = AuthTokens.basic( this.user, this.pass );

            if ( deployMode == DeployMode.DOCKER ) {

                if ( settings.getOrDefault( "deploymentId", "" ).isEmpty() ) {
                    int instanceId = Integer.parseInt( settings.get( "instanceId" ) );
                    DockerInstance instance = DockerManager.getInstance().getInstanceById( instanceId )
                            .orElseThrow( () -> new GenericRuntimeException( "No docker instance with id " + instanceId ) );
                    try {
                        this.container = instance.newBuilder( "polypheny/neo:latest", getUniqueName() )
                                .withEnvironmentVariable( "NEO4J_AUTH", String.format( "%s/%s", user, pass ) )
                                .createAndStart();
                    } catch ( IOException e ) {
                        throw new GenericRuntimeException( e );
                    }

                    if ( !container.waitTillStarted( this::testConnection, 100000 ) ) {
                        container.destroy();
                        throw new GenericRuntimeException( "Failed to create neo4j container" );
                    }
                    this.deploymentId = container.getContainerId();
                    settings.put( "deploymentId", deploymentId );
                    updateSettings( settings );
                } else {
                    deploymentId = settings.get( "deploymentId" );
                    DockerManager.getInstance(); // Make sure docker instances are loaded.  Very hacky, but it works
                    container = DockerContainer.getContainerByUUID( deploymentId )
                            .orElseThrow( () -> new GenericRuntimeException( "Could not find docker container with id " + deploymentId ) );
                    if ( !testConnection() ) {
                        throw new GenericRuntimeException( "Could not connect to container" );
                    }
                }
            } else if ( deployMode == DeployMode.REMOTE ) {
                this.host = settings.get( "host" );
                this.port = Integer.parseInt( settings.get( "port" ) );
                this.container = null;

            } else {
                throw new GenericRuntimeException( "Not supported deploy mode: " + deployMode.name() );
            }

            if ( this.db == null ) {
                try {
                    this.db = GraphDatabase.driver( new URI( String.format( "bolt://%s:%s", host, port ) ), auth );
                } catch ( URISyntaxException e ) {
                    throw new GenericRuntimeException( "Error while restoring the Neo4j database." );
                }
            }

            this.session = this.db.session();
            this.transactionProvider = new TransactionProvider( this.db );

            addInformationPhysicalNames();
            enableInformationPage();

            this.delegate = new GraphModifyDelegate( this, adapterCatalog );
        }


        /**
         * Test if a connection to the provided Neo4j database is possible.
         */
        private boolean testConnection() {
            if ( container == null ) {
                return false;
            }

            HostAndPort hp = container.connectToContainer( 7687 );
            this.host = hp.host();
            this.port = hp.port();

            try {
                this.db = GraphDatabase.driver( new URI( String.format( "bolt://%s:%s", host, port ) ), auth );
            } catch ( URISyntaxException e ) {
                throw new GenericRuntimeException( "Error while initiating the neo4j adapter." );
            }
            if ( this.db == null ) {
                return false;
            }

            try {
                this.db.verifyConnectivity();
                return true;
            } catch ( Exception e ) {
                return false;
            }
        }


        public void executeDdlTrx( PolyXid xid, List<String> queries ) {
            Transaction trx = transactionProvider.getDdlTransaction();
            try {
                for ( String query : queries ) {
                    trx.run( query );
                }

                transactionProvider.commitDdlTransaction();
            } catch ( Neo4jException e ) {
                transactionProvider.rollbackDdlTransaction();
                throw new GenericRuntimeException( e );
            }
        }


        public void executeDdlTrx( PolyXid session, String query ) {
            executeDdlTrx( session, List.of( query ) );
        }


        @Override
        public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
            if ( this.currentNamespace == null ) {
                updateNamespace( DEFAULT_DATABASE, allocation.table.id );
                adapterCatalog.addNamespace( allocation.table.namespaceId, currentNamespace );
            }

            String physicalName = getPhysicalEntityName( logical.table.namespaceId, allocation.table.id );

            PhysicalTable physical = adapterCatalog.createTable(
                    logical.getTable().getNamespaceName(),
                    physicalName,
                    allocation.columns.stream().collect( Collectors.toMap( c -> c.columnId, c -> getPhysicalFieldName( c.columnId ) ) ),
                    logical.table,
                    logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c ) ),
                    logical.pkIds,
                    allocation );

            this.adapterCatalog.addPhysical( allocation.table, this.currentNamespace.createEntity( physical, physical.columns, currentNamespace ) );
            return refreshTable( allocation.table.id );
        }


        public List<PhysicalEntity> refreshTable( long allocId ) {
            PhysicalEntity physical = adapterCatalog.fromAllocation( allocId, PhysicalEntity.class );
            List<? extends PhysicalField> fields = adapterCatalog.getFields( allocId );
            adapterCatalog.replacePhysical( currentNamespace.createEntity( physical, fields, this.currentNamespace ) );
            return List.of( physical );
        }


        @Override
        public void dropTable( Context context, long allocId ) {
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            PhysicalEntity physical = adapterCatalog.fromAllocation( allocId, PhysicalEntity.class );

            executeDdlTrx(
                    context.getStatement().getTransaction().getXid(),
                    String.format( "MATCH (n:%s) DELETE n", physical.name ) );
        }


        @Override
        public void addColumn( Context context, long allocId, LogicalColumn logicalColumn ) {
            transactionProvider.commitAll();
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            NeoEntity physical = adapterCatalog.fromAllocation( allocId, NeoEntity.class );
            String physicalName = getPhysicalFieldName( logicalColumn.id );
            PhysicalColumn column = adapterCatalog.createColumn( physicalName, allocId, physical.getFields().size() - 1, logicalColumn );

            if ( logicalColumn.defaultValue != null ) {
                executeDdlTrx( context.getStatement().getTransaction().getXid(), String.format(
                        "MATCH (n:%s) SET n += {%s:%s}",
                        getPhysicalEntityName( logicalColumn.namespaceId, allocId ),
                        getPhysicalFieldName( logicalColumn.id ),
                        getDefaultAsNeo( logicalColumn.defaultValue, logicalColumn.type ) ) );
            }

            List<PhysicalField> fields = new ArrayList<>( physical.getFields() );
            fields.add( column );
            adapterCatalog.replacePhysical(
                    physical.toBuilder().fields( fields.stream()
                            .map( f -> f.unwrap( PhysicalColumn.class ).orElseThrow() )
                            .sorted( Comparator.comparingInt( a -> a.position ) ).toList() ).build() );
        }


        private Object getDefaultAsNeo( LogicalDefaultValue defaultValue, PolyType type ) {
            if ( defaultValue != null ) {
                Object value = NeoUtil.fixParameterValue( defaultValue.value, new NestedSingleType( type ), false );
                return String.format( "%s", value );
            }
            return null;
        }


        @Override
        public void dropColumn( Context context, long allocId, long columnId ) {
            NeoEntity physical = adapterCatalog.fromAllocation( allocId, NeoEntity.class );
            PhysicalColumn column = adapterCatalog.getField( columnId, allocId ).unwrap( PhysicalColumn.class ).orElseThrow();
            context.getStatement().getTransaction().registerInvolvedAdapter( this );

            adapterCatalog.fields.values().stream().filter( f -> f.id == columnId && f.allocId == allocId ).forEach( f -> {
                adapterCatalog.dropColumn( f.allocId, f.id );
            } );

            executeDdlTrx(
                    context.getStatement().getTransaction().getXid(),
                    String.format( "MATCH (n:%s) REMOVE n.%s", physical.name, column.name ) );

            adapterCatalog.replacePhysical(
                    physical.toBuilder().fields( physical.getFields().stream().filter( f -> f.id != columnId ).toList() ).build() );
        }


        @Override
        public String addIndex( Context context, LogicalIndex index, AllocationTable allocation ) {
            NeoEntity physical = adapterCatalog.fromAllocation( allocation.id, NeoEntity.class );
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            IndexTypes type = IndexTypes.valueOf( index.method.toUpperCase( Locale.ROOT ) );
            List<Long> columns = index.key.fieldIds;

            String physicalIndexName = "idx" + index.key.entityId + "_" + index.id;

            switch ( type ) {
                case DEFAULT:
                case COMPOSITE:
                    addCompositeIndex(
                            context.getStatement().getTransaction().getXid(),
                            index,
                            columns,
                            physical,
                            physicalIndexName );
                    break;
            }

            return physicalIndexName;
        }


        private void addCompositeIndex( PolyXid xid, LogicalIndex index, List<Long> columnIds, NeoEntity physical, String physicalIndexName ) {
            String fields = columnIds.stream()
                    .map( id -> String.format( "n.%s", getPhysicalFieldName( id ) ) )
                    .collect( Collectors.joining( ", " ) );

            executeDdlTrx( xid, String.format(
                    "CREATE INDEX %s IF NOT EXISTS FOR (n:%s) on (%s)",
                    physicalIndexName + "_" + physical.id + "_" + index.id,
                    getPhysicalEntityName( index.key.namespaceId, physical.id ),
                    fields ) );
        }


        @Override
        public void dropIndex( Context context, LogicalIndex index, long allocId ) {
            context.getStatement().getTransaction().registerInvolvedAdapter( this );

            executeDdlTrx(
                    context.getStatement().getTransaction().getXid(),
                    String.format( "DROP INDEX %s IF EXISTS", index.physicalName + "_" + allocId + "_" + index.id ) );

        }


        @Override
        public void updateColumnType( Context context, long allocId, LogicalColumn newCol ) {
            // empty on purpose, all string?
            NeoEntity entity = adapterCatalog.fromAllocation( allocId, NeoEntity.class );
            PhysicalColumn column = adapterCatalog.updateColumnType( allocId, newCol );

            adapterCatalog.replacePhysical(
                    entity.toBuilder().fields( entity.getFields().stream().map( f -> f.id == newCol.id ? column : f ).toList() ).build() );
        }


        @Override
        public List<IndexMethodModel> getAvailableIndexMethods() {
            return Arrays.stream( IndexTypes.values() ).map( IndexTypes::asMethod ).toList();
        }


        @Override
        public IndexMethodModel getDefaultIndexMethod() {
            return IndexTypes.COMPOSITE.asMethod();
        }


        @Override
        public List<FunctionalIndexInfo> getFunctionalIndexes( LogicalTable table ) {
            return ImmutableList.of();
        }


        @Override
        public void updateNamespace( String name, long id ) {
            this.currentNamespace = new NeoNamespace(
                    db,
                    transactionProvider,
                    this,
                    id );
        }


        @Override
        public void truncate( Context context, long allocId ) {
            transactionProvider.commitAll();
            context.getStatement().getTransaction().registerInvolvedAdapter( this );

            NeoEntity physical = adapterCatalog.fromAllocation( allocId, NeoEntity.class );
            executeDdlTrx(
                    context.getStatement().getTransaction().getXid(),
                    String.format( "MATCH (n:%s) DETACH DELETE n", physical.name ) );

        }


        @Override
        public List<PhysicalEntity> createGraph( Context context, LogicalGraph logical, AllocationGraph allocation ) {
            PhysicalGraph physical = adapterCatalog.createGraph(
                    getPhysicalGraphName( allocation.id ),
                    logical,
                    allocation );

            this.adapterCatalog.addPhysical( allocation, new NeoGraph( physical, List.of(), this.transactionProvider, this.db, getMappingLabel( physical.id ), this ) );
            return refreshGraph( allocation.id );
        }


        public List<PhysicalEntity> refreshGraph( long allocId ) {
            PhysicalGraph physical = adapterCatalog.fromAllocation( allocId, PhysicalGraph.class );
            adapterCatalog.replacePhysical( new NeoGraph( physical, List.of(), this.transactionProvider, this.db, getMappingLabel( physical.id ), this ) );
            return List.of( physical );
        }


        @Override
        public void dropGraph( Context context, AllocationGraph allocation ) {
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            PhysicalGraph physical = adapterCatalog.fromAllocation( allocation.id, PhysicalGraph.class );
            executeDdlTrx(
                    context.getStatement().getTransaction().getXid(),
                    String.format( "MATCH (n:%s) DETACH DELETE n", getMappingLabel( physical.id ) ) );
        }


        @Override
        public void renameLogicalColumn( long id, String newColumnName ) {
            Stream<Long> allocIds = adapterCatalog.fields.values().stream().filter( f -> f.id == id ).map( f -> f.allocId ).distinct();
            adapterCatalog.renameLogicalColumn( id, newColumnName );
            allocIds.forEach( allocId -> {
                NeoEntity entity = adapterCatalog.fromAllocation( allocId, NeoEntity.class );
                this.currentNamespace.createEntity( entity, adapterCatalog.getFields( allocId ), this.currentNamespace );
            } );
        }


        @Override
        public boolean prepare( PolyXid xid ) {
            return true;
        }


        @Override
        public void commit( PolyXid xid ) {
            this.transactionProvider.commit( xid );
        }


        @Override
        public void rollback( PolyXid xid ) {
            this.transactionProvider.rollback( xid );
        }


        @Override
        public void shutdown() {
            DockerContainer.getContainerByUUID( deploymentId ).ifPresent( DockerContainer::destroy );

            removeInformationPage();
        }


        @Override
        protected void reloadSettings( List<String> updatedSettings ) {

        }


    }


    public enum IndexTypes {
        DEFAULT,
        COMPOSITE;


        public IndexMethodModel asMethod() {
            return new IndexMethodModel( name().toLowerCase( Locale.ROOT ), name() + " INDEX" );
        }
    }


    @SuppressWarnings("unused")
    public interface Exclude {

        void refreshGraph( long allocId );

        void dropGraph( Context context, AllocationGraph allocation );

        void createGraph( Context context, LogicalGraph logical, AllocationGraph allocation );

        void dropColumn( Context context, long allocId, long columnId );

        void dropTable( Context context, long allocId );

        void updateColumnType( Context context, long allocId, LogicalColumn newCol );

        void addColumn( Context context, long allocId, LogicalColumn logicalColumn );

        void refreshTable( long allocId );

        void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocationWrapper );

        String addIndex( Context context, LogicalIndex logicalIndex, AllocationTable allocation );

        void dropIndex( Context context, LogicalIndex index, long allocId );

        void renameLogicalColumn( long id, String newColumnName );

    }

}

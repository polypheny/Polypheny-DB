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

package org.polypheny.db.adapter.neo4j;

import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.Expression;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.exceptions.UnknownNamespaceException;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.docker.DockerManager.ContainerBuilder;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;

@Slf4j
@AdapterProperties(
        name = "Neo4j",
        description = "Neo4j is a graph-model based database system. If stores data in a graph structure which consists of nodes and edges.",
        usedModes = { DeployMode.DOCKER },
        supportedSchemaTypes = { NamespaceType.GRAPH, NamespaceType.RELATIONAL })
@AdapterSettingInteger(name = "port", defaultValue = 7687)
public class Neo4jStore extends DataStore {

    @Getter
    private final List<PolyType> unsupportedTypes = ImmutableList.of();

    private final int port;
    private final String user;
    private final Session session;
    private Driver db;
    private final String pass;
    private final AuthToken auth;
    @Getter
    private NeoNamespace currentSchema;

    @Getter
    private NeoGraph currentGraph;

    private final TransactionProvider transactionProvider;


    public Neo4jStore( int adapterId, String uniqueName, Map<String, String> settings ) {
        super( adapterId, uniqueName, settings, Boolean.parseBoolean( settings.get( "persistent" ) ) );

        this.port = Integer.parseInt( settings.get( "port" ) );

        this.pass = "test";
        this.user = "neo4j";
        this.auth = AuthTokens.basic( "neo4j", this.pass );

        DockerManager.Container container = new ContainerBuilder( getAdapterId(), "neo4j:4.4-community", getUniqueName(), Integer.parseInt( settings.get( "instanceId" ) ) )
                .withMappedPort( 7687, port )
                .withEnvironmentVariable( String.format( "NEO4J_AUTH=%s/%s", user, pass ) )
                .withReadyTest( this::testConnection, 20000 )
                .build();

        DockerManager.getInstance().initialize( container ).start();

        if ( this.db == null ) {
            try {
                this.db = GraphDatabase.driver( new URI( "bolt://localhost:" + port ), auth );
            } catch ( URISyntaxException e ) {
                throw new RuntimeException( "Error while restoring the Neo4j database." );
            }
        }

        this.session = this.db.session();
        this.transactionProvider = new TransactionProvider( this.db );

        addInformationPhysicalNames();
        enableInformationPage();

    }


    private boolean testConnection() {
        try {
            this.db = GraphDatabase.driver( new URI( "bolt://localhost:" + port ), auth );
        } catch ( URISyntaxException e ) {
            throw new RuntimeException( "Error while initiating the neo4j adapter." );
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


    @Override
    public List<NamespaceType> getSupportedSchemaType() {
        return ImmutableList.of( NamespaceType.RELATIONAL, NamespaceType.GRAPH );
    }


    @Override
    public void createTable( Context context, CatalogEntity combinedTable, List<Long> partitionIds ) {
        Catalog catalog = Catalog.getInstance();

        if ( this.currentSchema == null ) {
            createNewSchema( null, Catalog.getInstance().getNamespace( combinedTable.namespaceId ).getName() );
        }

        for ( long partitionId : partitionIds ) {
            String physicalTableName = getPhysicalEntityName( combinedTable.namespaceId, combinedTable.id, partitionId );

            catalog.updatePartitionPlacementPhysicalNames(
                    getAdapterId(),
                    partitionId,
                    combinedTable.getNamespaceName(),
                    physicalTableName );

            for ( CatalogColumnPlacement placement : catalog.getColumnPlacementsOnAdapterPerTable( getAdapterId(), combinedTable.id ) ) {
                catalog.updateColumnPlacementPhysicalNames(
                        getAdapterId(),
                        placement.columnId,
                        combinedTable.getNamespaceName(),
                        physicalTableName,
                        true );
            }
        }

    }


    public void executeDdlTrx( PolyXid xid, List<String> queries ) {
        Transaction trx = transactionProvider.get( xid );
        for ( String query : queries ) {
            trx.run( query );
        }
    }


    public void executeDdlTrx( PolyXid session, String query ) {
        executeDdlTrx( session, List.of( query ) );
    }


    @Override
    public void dropTable( Context context, CatalogEntity combinedTable, List<Long> partitionIds ) {
        Catalog catalog = Catalog.getInstance();
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        List<CatalogPartitionPlacement> partitionPlacements = partitionIds.stream().map( id -> catalog.getPartitionPlacement( getAdapterId(), id ) ).collect( Collectors.toList() );

        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
            catalog.deletePartitionPlacement( getAdapterId(), partitionPlacement.partitionId );
            executeDdlTrx( context.getStatement().getTransaction().getXid(), String.format( "MATCH (n:%s) DELETE n", partitionPlacement.physicalTableName ) );
        }
    }


    @Override
    public void addColumn( Context context, CatalogEntity catalogEntity, CatalogColumn catalogColumn ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        Catalog catalog = Catalog.getInstance();
        List<CatalogPartitionPlacement> partitionPlacements = catalogEntity
                .partitionProperty
                .partitionIds
                .stream()
                .map( id -> catalog.getPartitionPlacement( getAdapterId(), id ) )
                .collect( Collectors.toList() );

        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
            if ( catalogColumn.defaultValue != null ) {
                executeDdlTrx( context.getStatement().getTransaction().getXid(), String.format(
                        "MATCH (n:%s) SET += {%s:'%s'}",
                        getPhysicalEntityName( catalogColumn.schemaId, catalogColumn.tableId, partitionPlacement.partitionId ),
                        getPhysicalFieldName( catalogColumn.id ),
                        catalogColumn.defaultValue ) );
            }

            // Add physical name to placement
            catalog.updateColumnPlacementPhysicalNames(
                    getAdapterId(),
                    catalogColumn.id,
                    currentSchema.physicalName,
                    getPhysicalFieldName( catalogColumn.id ),
                    false );
        }

    }


    @Override
    public void dropColumn( Context context, CatalogColumnPlacement columnPlacement ) {
        for ( CatalogPartitionPlacement partitionPlacement : catalog.getPartitionPlacementsByTableOnAdapter( columnPlacement.adapterId, columnPlacement.tableId ) ) {
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            executeDdlTrx( context.getStatement().getTransaction().getXid(), String.format( "MATCH (n:%s) REMOVE n.%s", partitionPlacement.physicalTableName, columnPlacement.physicalColumnName ) );
        }
    }


    @Override
    public void addIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds ) {
        Catalog catalog = Catalog.getInstance();
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        IndexTypes type = IndexTypes.valueOf( catalogIndex.method.toUpperCase( Locale.ROOT ) );
        List<Long> columns = catalogIndex.key.columnIds;
        for ( Long partitionId : partitionIds ) {
            switch ( type ) {
                case DEFAULT:
                case COMPOSITE:
                    addCompositeIndex( context.getStatement().getTransaction().getXid(), catalogIndex, columns, catalog.getPartitionPlacement( getAdapterId(), partitionId ) );
                    break;
            }
        }

        Catalog.getInstance().setIndexPhysicalName( catalogIndex.id, catalogIndex.name );
    }


    private void addCompositeIndex( PolyXid xid, CatalogIndex catalogIndex, List<Long> columnIds, CatalogPartitionPlacement partitionPlacement ) {
        String fields = columnIds.stream().map( id -> String.format( "n.%s", getPhysicalFieldName( id ) ) ).collect( Collectors.joining( ", " ) );
        executeDdlTrx( xid, String.format(
                "CREATE INDEX %s FOR (n:%s) on (%s)",
                catalogIndex.name,
                getPhysicalEntityName( catalogIndex.key.schemaId, catalogIndex.key.tableId, partitionPlacement.partitionId ),
                fields ) );
    }


    @Override
    public void dropIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        List<CatalogPartitionPlacement> partitionPlacements = partitionIds
                .stream()
                .map( id -> catalog.getPartitionPlacement( getAdapterId(), id ) )
                .collect( Collectors.toList() );

        for ( CatalogPartitionPlacement ignored : partitionPlacements ) {
            executeDdlTrx( context.getStatement().getTransaction().getXid(), String.format( "DROP INDEX %s", catalogIndex.physicalName ) );
        }
    }


    @Override
    public void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn, PolyType oldType ) {
        // empty on purpose, all string?
    }


    @Override
    public List<AvailableIndexMethod> getAvailableIndexMethods() {
        return Arrays.stream( IndexTypes.values() ).map( IndexTypes::asMethod ).collect( Collectors.toList() );
    }


    @Override
    public AvailableIndexMethod getDefaultIndexMethod() {
        return IndexTypes.COMPOSITE.asMethod();
    }


    @Override
    public List<FunctionalIndexInfo> getFunctionalIndexes( CatalogEntity catalogEntity ) {
        return ImmutableList.of();
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        final Expression expression = Schemas.subSchemaExpression( rootSchema, name, NeoNamespace.class );
        String namespaceName;
        String[] splits = name.split( "_" );
        if ( splits.length == 3 ) {
            namespaceName = splits[1];
        } else {
            throw new RuntimeException( "Error while generating new namespace" );
        }

        try {
            this.currentSchema = new NeoNamespace( db, expression, transactionProvider, this, Catalog.getInstance().getNamespace( Catalog.defaultDatabaseId, namespaceName ).id );
        } catch ( UnknownNamespaceException e ) {
            throw new RuntimeException( "Error while generating new namespace" );
        }
    }


    @Override
    public Table createTableSchema( CatalogEntity combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
        return this.currentSchema.createTable( combinedTable, columnPlacementsOnStore, partitionPlacement );
    }


    @Override
    public void truncate( Context context, CatalogEntity table ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        for ( CatalogPartitionPlacement partitionPlacement : catalog.getPartitionPlacementsByTableOnAdapter( getAdapterId(), table.id ) ) {
            executeDdlTrx( context.getStatement().getTransaction().getXid(), String.format( "MATCH (n:%s) DELETE n", partitionPlacement.physicalTableName ) );
        }
    }


    @Override
    public void createGraph( Context context, CatalogGraphDatabase graphDatabase ) {
        catalog.updateGraphPlacementPhysicalNames( graphDatabase.id, getAdapterId(), getPhysicalGraphName( graphDatabase.id ) );
    }


    @Override
    public void dropGraph( Context context, CatalogGraphPlacement graphPlacement ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        executeDdlTrx( context.getStatement().getTransaction().getXid(), String.format( "MATCH (n:%s)\nDETACH DELETE n", getMappingLabel( graphPlacement.graphId ) ) );
    }


    @Override
    public void createGraphNamespace( SchemaPlus rootSchema, String name, long id ) {
        this.currentGraph = new NeoGraph( name, this.transactionProvider, this.db, id, getMappingLabel( id ), this );
    }


    @Override
    public Schema getCurrentGraphNamespace() {
        return currentGraph;
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
        DockerInstance.getInstance().destroyAll( getAdapterId() );

        removeInformationPage();
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {

    }


    public static String getPhysicalEntityName( long namespaceId, long entityId, long partitionId ) {
        return String.format( "n_%d_entity_%d_%d", namespaceId, entityId, partitionId );
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


    public enum IndexTypes {
        DEFAULT,
        COMPOSITE;


        public AvailableIndexMethod asMethod() {
            return new AvailableIndexMethod( name().toLowerCase( Locale.ROOT ), name() + " INDEX" );
        }
    }

}

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

import static java.lang.String.format;

import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Expression;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.Neo4jException;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDefaultValue;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.docker.DockerManager;
import org.polypheny.db.docker.DockerManager.Container;
import org.polypheny.db.docker.DockerManager.ContainerBuilder;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;

@Slf4j
@AdapterProperties(
        name = "Neo4j",
        description = "Neo4j is a graph-model based database system. If stores data in a graph structure which consists of nodes and edges.",
        usedModes = { DeployMode.DOCKER },
        supportedNamespaceTypes = { NamespaceType.GRAPH, NamespaceType.RELATIONAL })
@AdapterSettingInteger(name = "port", defaultValue = 7687)
public class Neo4jStore extends DataStore {

    @Getter
    private final List<PolyType> unsupportedTypes = ImmutableList.of();

    private final int port;
    private final String user;
    private final Session session;
    private final Container container;
    private Driver db;
    private final String pass;
    private final AuthToken auth;
    @Getter
    private NeoNamespace currentSchema;

    @Getter
    private NeoGraph currentGraph;

    private final TransactionProvider transactionProvider;
    private String host;


    public Neo4jStore( int adapterId, String uniqueName, Map<String, String> settings ) {
        super( adapterId, uniqueName, settings, Boolean.parseBoolean( settings.get( "persistent" ) ) );

        this.port = Integer.parseInt( settings.get( "port" ) );

        this.pass = "test";
        this.user = "neo4j";
        this.auth = AuthTokens.basic( "neo4j", this.pass );

        DockerManager.Container container = new ContainerBuilder( getAdapterId(), "neo4j:4.4-community", getUniqueName(), Integer.parseInt( settings.get( "instanceId" ) ) )
                .withMappedPort( 7687, port )
                .withEnvironmentVariable( format( "NEO4J_AUTH=%s/%s", user, pass ) )
                .withReadyTest( this::testConnection, 50000 )
                .build();
        this.container = container;
        DockerManager.getInstance().initialize( container ).start();

        if ( this.db == null ) {
            try {
                this.db = GraphDatabase.driver( new URI( format( "bolt://%s:%s", host, port ) ), auth );
            } catch ( URISyntaxException e ) {
                throw new RuntimeException( "Error while restoring the Neo4j database." );
            }
        }

        this.session = this.db.session();
        this.transactionProvider = new TransactionProvider( this.db );

        addInformationPhysicalNames();
        enableInformationPage();
    }


    /**
     * Test if a connection to the provided Neo4j database is possible.
     */
    private boolean testConnection() {
        if ( container == null ) {
            return false;
        }
        container.updateIpAddress();
        this.host = container.getIpAddress();
        if ( this.host == null ) {
            return false;
        }

        try {
            this.db = GraphDatabase.driver( new URI( format( "bolt://%s:%s", host, port ) ), auth );
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
    public void createTable( Context context, CatalogTable combinedTable, List<Long> partitionIds ) {
        Catalog catalog = Catalog.getInstance();

        if ( this.currentSchema == null ) {
            createNewSchema( null, Catalog.getInstance().getSchema( combinedTable.namespaceId ).getName() );
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
        Transaction trx = transactionProvider.getDdlTransaction();
        try {
            for ( String query : queries ) {
                trx.run( query );
            }

            transactionProvider.commitDdlTransaction();
        } catch ( Neo4jException e ) {
            transactionProvider.rollbackDdlTransaction();
            throw new RuntimeException( e );
        }
    }


    public void executeDdlTrx( PolyXid session, String query ) {
        executeDdlTrx( session, List.of( query ) );
    }


    @Override
    public void dropTable( Context context, CatalogTable combinedTable, List<Long> partitionIds ) {
        Catalog catalog = Catalog.getInstance();
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        List<CatalogPartitionPlacement> partitionPlacements = partitionIds.stream()
                .map( id -> catalog.getPartitionPlacement( getAdapterId(), id ) )
                .collect( Collectors.toList() );

        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
            catalog.deletePartitionPlacement( getAdapterId(), partitionPlacement.partitionId );
            executeDdlTrx(
                    context.getStatement().getTransaction().getXid(),
                    format( "MATCH (n:%s) DELETE n", partitionPlacement.physicalTableName ) );
        }
    }


    @Override
    public void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn ) {
        transactionProvider.commitAll();
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        Catalog catalog = Catalog.getInstance();
        List<CatalogPartitionPlacement> partitionPlacements = catalogTable
                .partitionProperty
                .partitionIds
                .stream()
                .map( id -> catalog.getPartitionPlacement( getAdapterId(), id ) )
                .collect( Collectors.toList() );

        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
            if ( catalogColumn.defaultValue != null ) {
                executeDdlTrx( context.getStatement().getTransaction().getXid(), format(
                        "MATCH (n:%s) SET n += {%s:%s}",
                        getPhysicalEntityName( catalogColumn.schemaId, catalogColumn.tableId, partitionPlacement.partitionId ),
                        getPhysicalFieldName( catalogColumn.id ),
                        getDefaultAsNeo( catalogColumn.defaultValue, catalogColumn.type ) ) );
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


    private static Object getDefaultAsNeo( CatalogDefaultValue defaultValue, PolyType type ) {
        if ( defaultValue != null ) {
            Object value;
            if ( type.getFamily() == PolyTypeFamily.CHARACTER ) {
                value = defaultValue.value;
            } else if ( PolyType.INT_TYPES.contains( type ) ) {
                return Integer.parseInt( defaultValue.value );
            } else if ( PolyType.FRACTIONAL_TYPES.contains( type ) ) {
                return Double.parseDouble( defaultValue.value );
            } else if ( type.getFamily() == PolyTypeFamily.BOOLEAN ) {
                return Boolean.valueOf( defaultValue.value );
            } else if ( type.getFamily() == PolyTypeFamily.DATE ) {
                try {
                    return new SimpleDateFormat( "yyyy-MM-dd" ).parse( defaultValue.value ).getTime();
                } catch ( ParseException e ) {
                    throw new RuntimeException( e );
                }
            } else if ( type.getFamily() == PolyTypeFamily.TIME ) {
                return (int) Time.valueOf( defaultValue.value ).getTime();
            } else if ( type.getFamily() == PolyTypeFamily.TIMESTAMP ) {
                return Timestamp.valueOf( defaultValue.value ).getTime();
            } else if ( type.getFamily() == PolyTypeFamily.BINARY ) {
                return Arrays.toString( ByteString.parseBase64( defaultValue.value ) );
            } else {
                value = defaultValue.value;
            }
            if ( type == PolyType.ARRAY ) {
                throw new RuntimeException( "Default values are not supported for array types" );
            }

            return format( "'%s'", value );
        }
        return null;
    }


    @Override
    public void dropColumn( Context context, CatalogColumnPlacement columnPlacement ) {
        for ( CatalogPartitionPlacement partitionPlacement : catalog.getPartitionPlacementsByTableOnAdapter( columnPlacement.adapterId, columnPlacement.tableId ) ) {
            context.getStatement().getTransaction().registerInvolvedAdapter( this );
            executeDdlTrx(
                    context.getStatement().getTransaction().getXid(),
                    format( "MATCH (n:%s) REMOVE n.%s", partitionPlacement.physicalTableName, columnPlacement.physicalColumnName ) );
        }
    }


    @Override
    public void addIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds ) {
        Catalog catalog = Catalog.getInstance();
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        IndexTypes type = IndexTypes.valueOf( catalogIndex.method.toUpperCase( Locale.ROOT ) );
        List<Long> columns = catalogIndex.key.columnIds;

        List<CatalogPartitionPlacement> partitionPlacements = new ArrayList<>();
        partitionIds.forEach( id -> partitionPlacements.add( catalog.getPartitionPlacement( getAdapterId(), id ) ) );

        String physicalIndexName = "idx" + catalogIndex.key.tableId + "_" + catalogIndex.id;

        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
            switch ( type ) {
                case DEFAULT:
                case COMPOSITE:
                    addCompositeIndex(
                            context.getStatement().getTransaction().getXid(),
                            catalogIndex,
                            columns,
                            partitionPlacement,
                            physicalIndexName );
                    break;
            }
        }

        Catalog.getInstance().setIndexPhysicalName( catalogIndex.id, physicalIndexName );
    }


    private void addCompositeIndex( PolyXid xid, CatalogIndex catalogIndex, List<Long> columnIds, CatalogPartitionPlacement partitionPlacement, String physicalIndexName ) {
        String fields = columnIds.stream()
                .map( id -> format( "n.%s", getPhysicalFieldName( id ) ) )
                .collect( Collectors.joining( ", " ) );
        executeDdlTrx( xid, format(
                "CREATE INDEX %s FOR (n:%s) on (%s)",
                physicalIndexName + "_" + partitionPlacement.partitionId,
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

        for ( CatalogPartitionPlacement partitionPlacement : partitionPlacements ) {
            executeDdlTrx(
                    context.getStatement().getTransaction().getXid(),
                    format( "DROP INDEX %s", catalogIndex.physicalName + "_" + partitionPlacement.partitionId ) );
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
    public List<FunctionalIndexInfo> getFunctionalIndexes( CatalogTable catalogTable ) {
        return ImmutableList.of();
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        final Expression expression = Schemas.subSchemaExpression( rootSchema, name, NeoNamespace.class );
        String namespaceName;
        String[] splits = name.split( "_" );
        if ( splits.length >= 3 ) {
            namespaceName = splits[1];
        } else {
            throw new RuntimeException( "Error while generating new namespace" );
        }

        try {
            this.currentSchema = new NeoNamespace(
                    db,
                    expression,
                    transactionProvider,
                    this,
                    Catalog.getInstance().getSchema( Catalog.defaultDatabaseId, namespaceName ).id );
        } catch ( UnknownSchemaException e ) {
            throw new RuntimeException( "Error while generating new namespace" );
        }
    }


    @Override
    public Table createTableSchema( CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
        return this.currentSchema.createTable( combinedTable, columnPlacementsOnStore, partitionPlacement );
    }


    @Override
    public void truncate( Context context, CatalogTable table ) {
        transactionProvider.commitAll();
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        for ( CatalogPartitionPlacement partitionPlacement : catalog.getPartitionPlacementsByTableOnAdapter( getAdapterId(), table.id ) ) {
            executeDdlTrx(
                    context.getStatement().getTransaction().getXid(),
                    format( "MATCH (n:%s) DETACH DELETE n", partitionPlacement.physicalTableName ) );
        }
    }


    @Override
    public void createGraph( Context context, CatalogGraphDatabase graphDatabase ) {
        catalog.updateGraphPlacementPhysicalNames( graphDatabase.id, getAdapterId(), getPhysicalGraphName( graphDatabase.id ) );
    }


    @Override
    public void dropGraph( Context context, CatalogGraphPlacement graphPlacement ) {
        context.getStatement().getTransaction().registerInvolvedAdapter( this );
        executeDdlTrx(
                context.getStatement().getTransaction().getXid(),
                format( "MATCH (n:%s) DETACH DELETE n", getMappingLabel( graphPlacement.graphId ) ) );
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
        return format( "n_%d_entity_%d_%d", namespaceId, entityId, partitionId );
    }


    public static String getPhysicalNamespaceName( long id ) {
        return format( "namespace_%d", id );
    }


    public static String getPhysicalFieldName( long id ) {
        return format( "field_%d", id );
    }


    private static String getPhysicalGraphName( long id ) {
        return format( "graph_%d", id );
    }


    private static String getMappingLabel( long id ) {
        return format( "___n_%d___", id );
    }


    public enum IndexTypes {
        DEFAULT,
        COMPOSITE;


        public AvailableIndexMethod asMethod() {
            return new AvailableIndexMethod( name().toLowerCase( Locale.ROOT ), name() + " INDEX" );
        }
    }

}

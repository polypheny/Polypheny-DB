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

package org.polypheny.db.adapter;


import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.ExtensionPoint;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.AllocationColumn;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.util.LoggedIdBuilder;
import org.polypheny.db.catalog.util.StoreCatalog;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.type.PolyType;

@Slf4j
public abstract class DataStore extends Adapter implements ExtensionPoint {

    @Getter
    private final boolean persistent;

    protected final transient Catalog catalog = Catalog.getInstance();


    public DataStore( final long adapterId, final String uniqueName, final Map<String, String> settings, final boolean persistent ) {
        super( adapterId, uniqueName, settings );
        this.persistent = persistent;

        informationPage.setLabel( "Stores" );
    }


    public List<NamespaceType> getSupportedSchemaType() {
        log.info( "Using default NamespaceType support." );
        return ImmutableList.of( NamespaceType.RELATIONAL );
    }


    public abstract void createTable( StoreCatalog snapshot, Context context, AllocationTable alloc );

    public abstract void dropTable( Context context, LogicalTable combinedTable, AllocationTable table, List<? extends PhysicalEntity> physicals );

    public abstract void addColumn( Context context, AllocationTable catalogTable, LogicalColumn logicalColumn );

    public abstract void dropColumn( Context context, AllocationColumn columnPlacement );

    public abstract String addIndex( Context context, LogicalIndex logicalIndex, AllocationTable allocation );

    public abstract void dropIndex( Context context, LogicalIndex logicalIndex, List<Long> partitionIds );

    public abstract void updateColumnType( Context context, AllocationColumn columnPlacement, LogicalColumn logicalColumn, PolyType oldType );

    public abstract List<AvailableIndexMethod> getAvailableIndexMethods();

    public abstract AvailableIndexMethod getDefaultIndexMethod();

    public abstract List<FunctionalIndexInfo> getFunctionalIndexes( LogicalTable catalogTable );


    /**
     * Default method for creating a new graph on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the LPG data model natively.
     */
    public void createGraph( StoreCatalog catalog, Context context, AllocationGraph allocation ) {
        // overwrite this if the datastore supports graph
        createGraphSubstitution( catalog, context, allocation );
    }


    /**
     * Default method for dropping an existing graph on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the LPG data model natively.
     */
    public void dropGraph( Context context, CatalogGraphPlacement graphPlacement ) {
        // overwrite this if the datastore supports graph
        dropGraphSubstitution( context, graphPlacement );
    }


    /**
     * Substitution method, which is used to handle the {@link DataStore} required operations
     * as if the data model would be {@link NamespaceType#RELATIONAL}.
     */
    private void createGraphSubstitution( StoreCatalog catalog, Context context, AllocationGraph allocation ) {
        LoggedIdBuilder idBuilder = new LoggedIdBuilder();
        List<? extends PhysicalEntity> physicals = new ArrayList<>();

        LogicalTable nodes = new LogicalTable( idBuilder.getNewLogicalId() );
        AllocationTable aNodes = new AllocationTable( idBuilder.getNewAllocId() );
        storeCatalog.getPhysicals().addAll( createTable( context, nodes, aNodes ) );

        LogicalTable nodeProperty = new LogicalTable( idBuilder.getNewLogicalId() );
        AllocationTable aNodeProperty = new AllocationTable( idBuilder.getNewLogicalId() );
        storeCatalog.getPhysicals().addAll( createTable( context, nodeProperty, aNodeProperty ) );

        LogicalTable edges = new LogicalTable( idBuilder.getNewLogicalId() );
        AllocationTable aEdges = new AllocationTable( idBuilder.getNewLogicalId() );
        storeCatalog.getPhysicals().addAll( context, edges, aEdges ) );

        LogicalTable edgeProperty = new LogicalTable( idBuilder.getNewLogicalId() );
        AllocationTable aEdgeProperty = new AllocationTable( idBuilder.getNewLogicalId() );
        storeCatalog.getPhysicals().addAll( createTable( context, edgeProperty, aEdgeProperty ) );

        storeCatalog.getLogicals().add( nodes );
        storeCatalog.getLogicals().add( nodeProperty );
        storeCatalog.getLogicals().add( edges );
        storeCatalog.getLogicals().add( edgeProperty );

    }


    /**
     * Substitution method, which is used to handle the {@link DataStore} required operations
     * as if the data model would be {@link NamespaceType#RELATIONAL}.
     */
    private void dropGraphSubstitution( Context context, CatalogGraphPlacement graphPlacement ) {
        /*Catalog catalog = Catalog.getInstance();
        CatalogGraphMapping mapping = catalog.getGraphMapping( graphPlacement.graphId );

        LogicalTable nodes = catalog.getTable( mapping.nodesId );
        dropTable( context, nodes, nodes.partitionProperty.partitionIds );

        LogicalTable nodeProperty = catalog.getTable( mapping.nodesPropertyId );
        dropTable( context, nodeProperty, nodeProperty.partitionProperty.partitionIds );

        LogicalTable edges = catalog.getTable( mapping.edgesId );
        dropTable( context, edges, edges.partitionProperty.partitionIds );

        LogicalTable edgeProperty = catalog.getTable( mapping.edgesPropertyId );
        dropTable( context, edgeProperty, edgeProperty.partitionProperty.partitionIds );*/
        // todo dl
    }


    /**
     * Default method for creating a new collection on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the document data model natively.
     */
    public void createPhysicalCollection( StoreCatalog snapshot, Context context, AllocationCollection allocation ) {
        // overwrite this if the datastore supports document
        createCollectionSubstitution( snapshot, context, allocation );
    }


    /**
     * Substitution method, which is used to handle the {@link DataStore} required operations
     * as if the data model would be {@link NamespaceType#RELATIONAL}.
     */
    private void createCollectionSubstitution( StoreCatalog snapshot, Context context, AllocationCollection allocation ) {
        /*Catalog catalog = Catalog.getInstance();
        CatalogCollectionMapping mapping = catalog.getCollectionMapping( catalogCollection.id );

        LogicalTable collectionEntity = catalog.getTable( mapping.collectionId );
        createTable( prepareContext, collectionEntity, null );*/
        // todo dl
    }


    /**
     * Default method for dropping an existing collection on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the document data model natively.
     */
    public void dropCollection( StoreCatalog snapshot, Context prepareContext, AllocationCollection allocation ) {
        // overwrite this if the datastore supports document
        dropCollectionSubstitution( snapshot, prepareContext, allocation );
    }


    /**
     * Substitution method, which is used to handle the {@link DataStore} required operations
     * as if the data model would be {@link NamespaceType#RELATIONAL}.
     */
    private void dropCollectionSubstitution( StoreCatalog snapshot, Context context, AllocationCollection catalogCollection ) {
        /*Catalog catalog = Catalog.getInstance();
        CatalogCollectionMapping mapping = catalog.getCollectionMapping( catalogCollection.id );

        LogicalTable collectionEntity = catalog.getTable( mapping.collectionId );
        dropTable( prepareContext, collectionEntity, collectionEntity.partitionProperty.partitionIds );*/
        // todo dl
    }


    @AllArgsConstructor
    public static class AvailableIndexMethod {

        public final String name;
        public final String displayName;

    }


    @AllArgsConstructor
    public static class FunctionalIndexInfo {

        public final List<Long> columnIds;
        public final String methodDisplayName;


        public List<String> getColumnNames() {
            List<String> columnNames = new ArrayList<>( columnIds.size() );
            for ( long columnId : columnIds ) {
                // columnNames.add( Catalog.getInstance().getLogicalRel( columnNames ).getColumn( columnId ).name );
                // todo dl
            }
            return columnNames;
        }

    }


    public static JsonSerializer<DataStore> getSerializer() {
        //see https://futurestud.io/tutorials/gson-advanced-custom-serialization-part-1
        return ( src, typeOfSrc, context ) -> {
            JsonObject jsonStore = new JsonObject();
            jsonStore.addProperty( "adapterId", src.getAdapterId() );
            jsonStore.add( "adapterSettings", context.serialize( AbstractAdapterSetting.serializeSettings( src.getAvailableSettings( src.getClass() ), src.getCurrentSettings() ) ) );
            jsonStore.add( "currentSettings", context.serialize( src.getCurrentSettings() ) );
            jsonStore.addProperty( "adapterName", src.getAdapterName() );
            jsonStore.addProperty( "uniqueName", src.getUniqueName() );
            jsonStore.addProperty( "type", src.getAdapterType().name() );
            jsonStore.add( "persistent", context.serialize( src.isPersistent() ) );
            jsonStore.add( "availableIndexMethods", context.serialize( src.getAvailableIndexMethods() ) );
            return jsonStore;
        };
    }


    private AdapterType getAdapterType() {
        return AdapterType.STORE;
    }

}

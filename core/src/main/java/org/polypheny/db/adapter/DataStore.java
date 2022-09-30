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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogCollectionMapping;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.catalog.entity.CatalogGraphMapping;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.type.PolyType;

@Slf4j
public abstract class DataStore extends Adapter {

    @Getter
    private final boolean persistent;

    protected final transient Catalog catalog = Catalog.getInstance();


    public DataStore( final int adapterId, final String uniqueName, final Map<String, String> settings, final boolean persistent ) {
        super( adapterId, uniqueName, settings );
        this.persistent = persistent;

        informationPage.setLabel( "Stores" );
    }


    public List<NamespaceType> getSupportedSchemaType() {
        log.info( "Using default NamespaceType support." );
        return ImmutableList.of( NamespaceType.RELATIONAL );
    }


    public abstract void createTable( Context context, CatalogTable combinedTable, List<Long> partitionIds );

    public abstract void dropTable( Context context, CatalogTable combinedTable, List<Long> partitionIds );

    public abstract void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn );

    public abstract void dropColumn( Context context, CatalogColumnPlacement columnPlacement );

    public abstract void addIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds );

    public abstract void dropIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds );

    public abstract void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn, PolyType oldType );

    public abstract List<AvailableIndexMethod> getAvailableIndexMethods();

    public abstract AvailableIndexMethod getDefaultIndexMethod();

    public abstract List<FunctionalIndexInfo> getFunctionalIndexes( CatalogTable catalogTable );


    /**
     * Default method for creating a new graph on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the LPG data model natively.
     */
    public void createGraph( Context context, CatalogGraphDatabase graphDatabase ) {
        // overwrite this if the datastore supports graph
        createGraphSubstitution( context, graphDatabase );
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
    private void createGraphSubstitution( Context context, CatalogGraphDatabase graphDatabase ) {
        CatalogGraphMapping mapping = Catalog.getInstance().getGraphMapping( graphDatabase.id );

        CatalogTable nodes = Catalog.getInstance().getTable( mapping.nodesId );
        createTable( context, nodes, nodes.partitionProperty.partitionIds );

        CatalogTable nodeProperty = Catalog.getInstance().getTable( mapping.nodesPropertyId );
        createTable( context, nodeProperty, nodeProperty.partitionProperty.partitionIds );

        CatalogTable edges = Catalog.getInstance().getTable( mapping.edgesId );
        createTable( context, edges, edges.partitionProperty.partitionIds );

        CatalogTable edgeProperty = Catalog.getInstance().getTable( mapping.edgesPropertyId );
        createTable( context, edgeProperty, edgeProperty.partitionProperty.partitionIds );
    }


    /**
     * Substitution method, which is used to handle the {@link DataStore} required operations
     * as if the data model would be {@link NamespaceType#RELATIONAL}.
     */
    private void dropGraphSubstitution( Context context, CatalogGraphPlacement graphPlacement ) {
        Catalog catalog = Catalog.getInstance();
        CatalogGraphMapping mapping = catalog.getGraphMapping( graphPlacement.graphId );

        CatalogTable nodes = catalog.getTable( mapping.nodesId );
        dropTable( context, nodes, nodes.partitionProperty.partitionIds );

        CatalogTable nodeProperty = catalog.getTable( mapping.nodesPropertyId );
        dropTable( context, nodeProperty, nodeProperty.partitionProperty.partitionIds );

        CatalogTable edges = catalog.getTable( mapping.edgesId );
        dropTable( context, edges, edges.partitionProperty.partitionIds );

        CatalogTable edgeProperty = catalog.getTable( mapping.edgesPropertyId );
        dropTable( context, edgeProperty, edgeProperty.partitionProperty.partitionIds );
    }


    /**
     * Default method for creating a new collection on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the document data model natively.
     */
    public void createCollection( Context prepareContext, CatalogCollection catalogCollection, long adapterId ) {
        // overwrite this if the datastore supports document
        createCollectionSubstitution( prepareContext, catalogCollection );
    }


    /**
     * Substitution method, which is used to handle the {@link DataStore} required operations
     * as if the data model would be {@link NamespaceType#RELATIONAL}.
     */
    private void createCollectionSubstitution( Context prepareContext, CatalogCollection catalogCollection ) {
        Catalog catalog = Catalog.getInstance();
        CatalogCollectionMapping mapping = catalog.getCollectionMapping( catalogCollection.id );

        CatalogTable collectionEntity = catalog.getTable( mapping.collectionId );
        createTable( prepareContext, collectionEntity, collectionEntity.partitionProperty.partitionIds );
    }


    /**
     * Default method for dropping an existing collection on the {@link DataStore}.
     * It comes with a substitution methods called by default and should be overwritten if the inheriting {@link DataStore}
     * support the document data model natively.
     */
    public void dropCollection( Context prepareContext, CatalogCollection catalogCollection ) {
        // overwrite this if the datastore supports document
        dropCollectionSubstitution( prepareContext, catalogCollection );
    }


    /**
     * Substitution method, which is used to handle the {@link DataStore} required operations
     * as if the data model would be {@link NamespaceType#RELATIONAL}.
     */
    private void dropCollectionSubstitution( Context prepareContext, CatalogCollection catalogCollection ) {
        Catalog catalog = Catalog.getInstance();
        CatalogCollectionMapping mapping = catalog.getCollectionMapping( catalogCollection.id );

        CatalogTable collectionEntity = catalog.getTable( mapping.collectionId );
        dropTable( prepareContext, collectionEntity, collectionEntity.partitionProperty.partitionIds );
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
                columnNames.add( Catalog.getInstance().getColumn( columnId ).name );
            }
            return columnNames;
        }

    }


    public static JsonSerializer<DataStore> getSerializer() {
        //see https://futurestud.io/tutorials/gson-advanced-custom-serialization-part-1
        return ( src, typeOfSrc, context ) -> {
            JsonObject jsonStore = new JsonObject();
            jsonStore.addProperty( "adapterId", src.getAdapterId() );
            jsonStore.addProperty( "uniqueName", src.getUniqueName() );
            jsonStore.add( "adapterSettings", context.serialize( AbstractAdapterSetting.serializeSettings( src.getAvailableSettings(), src.getCurrentSettings() ) ) );
            jsonStore.add( "currentSettings", context.serialize( src.getCurrentSettings() ) );
            jsonStore.addProperty( "adapterName", src.getAdapterName() );
            jsonStore.addProperty( "type", src.getClass().getCanonicalName() );
            jsonStore.add( "persistent", context.serialize( src.isPersistent() ) );
            jsonStore.add( "availableIndexMethods", context.serialize( src.getAvailableIndexMethods() ) );
            return jsonStore;
        };
    }

}

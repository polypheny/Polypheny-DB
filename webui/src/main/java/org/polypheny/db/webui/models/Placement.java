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

package org.polypheny.db.webui.models;


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Value;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.logistic.PlacementType;


/**
 * A model for the UI, modelling the placements of a table
 */
public class Placement {

    Throwable exception;
    public List<Store> stores = new ArrayList<>();
    boolean isPartitioned;
    List<String> partitionNames;
    String tableType;


    public Placement( final boolean isPartitioned, final List<String> partitionNames, final EntityType entityType ) {
        this.isPartitioned = isPartitioned;
        this.partitionNames = partitionNames;
        this.tableType = entityType.name();
    }


    public Placement( final Throwable exception ) {
        this.exception = exception;
    }


    public Placement addAdapter( final RelationalStore s ) {
        if ( s.columnPlacements.size() > 0 ) {
            this.stores.add( s );
        }
        return this;
    }


    public Placement addAdapter( final GraphStore s ) {
        this.stores.add( s );

        return this;
    }


    public Placement addAdapter( final DocumentStore s ) {
        this.stores.add( s );

        return this;
    }


    public static abstract class Store {

        public final String uniqueName;
        private final String adapterName;


        protected Store( String uniqueName, String adapterName ) {
            this.uniqueName = uniqueName;
            this.adapterName = adapterName;
        }

    }


    @SuppressWarnings({ "unused", "FieldCanBeLocal" })
    public static class RelationalStore extends Store {

        private final List<ColumnPlacement> columnPlacements;
        private final List<Long> partitionKeys;
        private final long numPartitions;
        private final PartitionType partitionType;


        public RelationalStore(
                String uniqueName,
                String adapterName,
                List<CatalogColumnPlacement> columnPlacements,
                final List<Long> partitionKeys,
                final long numPartitions,
                final PartitionType partitionType ) {
            super( uniqueName, adapterName );
            this.columnPlacements = columnPlacements.stream().map( ColumnPlacement::new ).collect( Collectors.toList() );
            this.partitionKeys = partitionKeys;
            this.numPartitions = numPartitions;
            this.partitionType = partitionType;
        }

    }


    @SuppressWarnings({ "unused", "FieldCanBeLocal" })
    public static class GraphStore extends Store {


        private final List<GraphPlacement> placements;
        private final boolean isNative;


        public GraphStore( String uniqueName, String adapterName, List<CatalogGraphPlacement> graphPlacements, boolean isNative ) {
            super( uniqueName, adapterName );
            this.placements = graphPlacements.stream().map( p -> new GraphPlacement( p.graphId, p.adapterId ) ).collect( Collectors.toList() );
            this.isNative = isNative;
        }

    }


    @SuppressWarnings({ "unused", "FieldCanBeLocal" })
    public static class DocumentStore extends Store {


        private final List<CollectionPlacement> placements;
        private final boolean isNative;


        public DocumentStore( String uniqueName, String adapterName, List<CatalogCollectionPlacement> collectionPlacements, boolean isNative ) {
            super( uniqueName, adapterName );
            this.placements = collectionPlacements.stream().map( p -> new CollectionPlacement( p.collectionId, p.adapterId ) ).collect( Collectors.toList() );
            this.isNative = isNative;
        }

    }


    @Value
    private static class CollectionPlacement {

        long collectionId;
        long adapterId;


        public CollectionPlacement( long collectionId, long adapterId ) {
            this.collectionId = collectionId;
            this.adapterId = adapterId;
        }

    }


    @Value
    private static class GraphPlacement {

        long graphId;
        long adapterId;


        public GraphPlacement( long graphId, long adapterId ) {
            this.graphId = graphId;
            this.adapterId = adapterId;
        }

    }


    @SuppressWarnings({ "unused", "FieldCanBeLocal" })
    private static class ColumnPlacement {

        private final long tableId;
        private final String tableName;
        private final long columnId;
        private final String columnName;
        private final int storeId;
        private final PlacementType placementType;


        public ColumnPlacement( CatalogColumnPlacement catalogColumnPlacement ) {
            this.tableId = catalogColumnPlacement.tableId;
            this.tableName = catalogColumnPlacement.getLogicalTableName();
            this.columnId = catalogColumnPlacement.columnId;
            this.columnName = catalogColumnPlacement.getLogicalColumnName();
            this.storeId = (int) catalogColumnPlacement.adapterId;
            this.placementType = catalogColumnPlacement.placementType;
        }

    }

}

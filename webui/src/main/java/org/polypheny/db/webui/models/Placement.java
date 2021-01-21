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

package org.polypheny.db.webui.models;


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.catalog.Catalog.PartitionType;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;


/**
 * A model for the UI, modelling the placements of a table
 */
public class Placement {

    Throwable exception;
    public List<Store> stores = new ArrayList<>();
    boolean isPartitioned;
    List<String> partitionNames;


    public Placement( final boolean isPartitioned, final List<String> partitionNames ) {
        this.isPartitioned = isPartitioned;
        this.partitionNames = partitionNames;
    }


    public Placement( final Throwable exception ) {
        this.exception = exception;
    }


    public Placement addStore( final Store s ) {
        if ( s.columnPlacements.size() > 0 ) {
            this.stores.add( s );
        }
        return this;
    }


    public static class Store {

        public final String uniqueName;
        private final String adapterName;
        private final boolean dataReadOnly;
        private final boolean schemaReadOnly;
        private final List<ColumnPlacement> columnPlacements;
        private final List<Long> partitionKeys;
        private final long numPartitions;
        private final PartitionType partitionType;


        public Store(
                final org.polypheny.db.adapter.Store store,
                final List<CatalogColumnPlacement> columnPlacements,
                final List<Long> partitionKeys,
                final long numPartitions,
                final PartitionType partitionType ) {
            this.uniqueName = store.getUniqueName();
            this.adapterName = store.getAdapterName();
            this.dataReadOnly = store.isDataReadOnly();
            this.schemaReadOnly = store.isSchemaReadOnly();
            this.columnPlacements = columnPlacements.stream().map( ColumnPlacement::new ).collect( Collectors.toList() );
            this.partitionKeys = partitionKeys;
            this.numPartitions = numPartitions;
            this.partitionType = partitionType;
        }

    }


    private static class ColumnPlacement {

        private final long tableId;
        private final String tableName;
        private final long columnId;
        private final String columnName;
        private final int storeId;
        private final String storeUniqueName;
        private final PlacementType placementType;
        private final String physicalSchemaName;
        private final String physicalTableName;
        private final String physicalColumnName;


        public ColumnPlacement( CatalogColumnPlacement catalogColumnPlacement ) {
            this.tableId = catalogColumnPlacement.tableId;
            this.tableName = catalogColumnPlacement.getLogicalTableName();
            this.columnId = catalogColumnPlacement.columnId;
            this.columnName = catalogColumnPlacement.getLogicalColumnName();
            this.storeId = catalogColumnPlacement.storeId;
            this.storeUniqueName = catalogColumnPlacement.storeUniqueName;
            this.placementType = catalogColumnPlacement.placementType;
            this.physicalSchemaName = catalogColumnPlacement.physicalSchemaName;
            this.physicalTableName = catalogColumnPlacement.physicalTableName;
            this.physicalColumnName = catalogColumnPlacement.physicalColumnName;
        }

    }

}

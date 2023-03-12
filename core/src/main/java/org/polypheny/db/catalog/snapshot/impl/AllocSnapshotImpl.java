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

package org.polypheny.db.catalog.snapshot.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.db.catalog.catalogs.AllocationCatalog;
import org.polypheny.db.catalog.catalogs.AllocationDocumentCatalog;
import org.polypheny.db.catalog.catalogs.AllocationGraphCatalog;
import org.polypheny.db.catalog.catalogs.AllocationRelationalCatalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogCollectionMapping;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogGraphPlacement;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPartitionGroup;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.logistic.DataPlacementRole;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.AllocSnapshot;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.util.Pair;

public class AllocSnapshotImpl implements AllocSnapshot {

    private final ImmutableMap<Long, AllocationTable> tables;
    private final ImmutableMap<Long, AllocationCollection> collections;
    private final ImmutableMap<Long, AllocationGraph> graphs;

    private final ImmutableMap<Pair<Long, Long>, CatalogColumnPlacement> adapterColumnPlacement;

    private final ImmutableMap<Long, AllocationEntity> allocs;
    private final ImmutableMap<Long, List<AllocationEntity>> allocsOnAdapters;
    private final ImmutableMap<Long, List<CatalogColumnPlacement>> columPlacements;
    private final ImmutableMap<Pair<Long, Long>, List<CatalogColumnPlacement>> adapterLogicalTablePlacements;


    public AllocSnapshotImpl( Map<Long, AllocationCatalog> allocationCatalogs ) {
        this.tables = buildTables( allocationCatalogs
                .values()
                .stream()
                .filter( a -> a.getNamespace().namespaceType == NamespaceType.RELATIONAL )
                .map( c -> (AllocationRelationalCatalog) c )
                .collect( Collectors.toList() ) );
        this.collections = buildCollections( allocationCatalogs
                .values()
                .stream()
                .filter( a -> a.getNamespace().namespaceType == NamespaceType.DOCUMENT )
                .map( c -> (AllocationDocumentCatalog) c )
                .collect( Collectors.toList() ) );
        this.graphs = buildGraphs( allocationCatalogs
                .values()
                .stream()
                .filter( a -> a.getNamespace().namespaceType == NamespaceType.GRAPH )
                .map( c -> (AllocationGraphCatalog) c )
                .collect( Collectors.toList() ) );

        this.allocs = mergeAllocs();
        this.allocsOnAdapters = buildAllocsOnAdapters();
        this.adapterColumnPlacement = buildAdapterColumnPlacement();
        this.columPlacements = buildColumnPlacements();
        this.adapterLogicalTablePlacements = buildAdapterLogicalTablePlacements();
    }


    private ImmutableMap<Pair<Long, Long>, List<CatalogColumnPlacement>> buildAdapterLogicalTablePlacements() {
        Map<Pair<Long, Long>, List<CatalogColumnPlacement>> map = new HashMap<>();
        this.tables.forEach( ( k, v ) -> {
            v.placements.forEach( p -> {
                if ( !map.containsKey( Pair.of( p.adapterId, p.tableId ) ) ) {
                    map.put( Pair.of( p.adapterId, p.tableId ), new ArrayList<>() );
                }
                map.get( Pair.of( p.adapterId, p.tableId ) ).add( p );
            } );
        } );

        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, List<CatalogColumnPlacement>> buildColumnPlacements() {
        Map<Long, List<CatalogColumnPlacement>> map = new HashMap<>();
        this.tables.forEach( ( k, v ) -> {
            v.placements.forEach( p -> {
                if ( !map.containsKey( p.columnId ) ) {
                    map.put( p.columnId, new ArrayList<>() );
                }
                map.get( p.columnId ).add( p );
            } );
        } );

        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Pair<Long, Long>, CatalogColumnPlacement> buildAdapterColumnPlacement() {
        Map<Pair<Long, Long>, CatalogColumnPlacement> map = new HashMap<>();
        this.tables.forEach( ( k, v ) -> v.placements.forEach( p -> map.put( Pair.of( v.adapterId, p.columnId ), p ) ) );
        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, AllocationEntity> mergeAllocs() {
        Map<Long, AllocationEntity> allocs = new HashMap<>();
        allocs.putAll( this.tables );
        allocs.putAll( this.collections );
        allocs.putAll( this.graphs );

        return ImmutableMap.copyOf( allocs );
    }


    private ImmutableMap<Long, List<AllocationEntity>> buildAllocsOnAdapters() {
        Map<Long, List<AllocationEntity>> allocs = new HashMap<>();
        this.allocs.forEach( ( k, v ) -> {
            if ( !allocs.containsKey( v.adapterId ) ) {
                allocs.put( v.adapterId, new ArrayList<>() );
            }
            allocs.get( v.adapterId ).add( v );
        } );
        return ImmutableMap.copyOf( allocs );

    }


    private ImmutableMap<Long, AllocationGraph> buildGraphs( List<AllocationGraphCatalog> catalogs ) {
        Map<Long, AllocationGraph> graphs = new HashMap<>();
        catalogs.forEach( c -> graphs.putAll( c.getGraphs() ) );

        return ImmutableMap.copyOf( graphs );
    }


    private ImmutableMap<Long, AllocationCollection> buildCollections( List<AllocationDocumentCatalog> catalogs ) {
        Map<Long, AllocationCollection> collections = new HashMap<>();
        catalogs.forEach( c -> collections.putAll( c.getCollections() ) );

        return ImmutableMap.copyOf( collections );
    }


    private ImmutableMap<Long, AllocationTable> buildTables( List<AllocationRelationalCatalog> catalogs ) {
        Map<Long, AllocationTable> tables = new HashMap<>();
        catalogs.forEach( c -> tables.putAll( c.getTables() ) );

        return ImmutableMap.copyOf( tables );
    }


    @Override
    public List<AllocationEntity> getAllocationsOnAdapter( long id ) {
        return allocsOnAdapters.get( id );
    }


    @Override
    public AllocationEntity getAllocEntity( long id ) {
        return allocs.get( id );
    }


    @Override
    public CatalogColumnPlacement getColumnPlacement( long adapterId, long columnId ) {
        return adapterColumnPlacement.get( Pair.of( adapterId, columnId ) );
    }


    @Override
    public boolean checkIfExistsColumnPlacement( long adapterId, long columnId ) {
        return adapterColumnPlacement.containsKey( Pair.of( adapterId, columnId ) );
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacements( long columnId ) {
        return columPlacements.get( columnId );
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapterPerTable( long adapterId, long tableId ) {
        return adapterLogicalTablePlacements.get( Pair.of( adapterId, tableId ) );
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapter( long adapterId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsByColumn( long columnId ) {
        return null;
    }


    @Override
    public ImmutableMap<Long, ImmutableList<Long>> getColumnPlacementsByAdapter( long tableId ) {
        return null;
    }


    @Override
    public long getPartitionGroupByPartition( long partitionId ) {
        return 0;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapterAndSchema( long adapterId, long schemaId ) {
        return null;
    }


    @Override
    public CatalogPartitionGroup getPartitionGroup( long partitionGroupId ) {
        return null;
    }


    @Override
    public CatalogPartition getPartition( long partitionId ) {
        return null;
    }


    @Override
    public List<CatalogPartition> getPartitionsByTable( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionGroup> getPartitionGroups( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionGroup> getPartitionGroups( Pattern schemaNamePattern, Pattern tableNamePattern ) {
        return null;
    }


    @Override
    public List<CatalogPartition> getPartitions( long partitionGroupId ) {
        return null;
    }


    @Override
    public List<CatalogPartition> getPartitions( Pattern schemaNamePattern, Pattern tableNamePattern ) {
        return null;
    }


    @Override
    public List<String> getPartitionGroupNames( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsByPartitionGroup( long tableId, long partitionGroupId, long columnId ) {
        return null;
    }


    @Override
    public List<CatalogAdapter> getAdaptersByPartitionGroup( long tableId, long partitionGroupId ) {
        return null;
    }


    @Override
    public List<Long> getPartitionGroupsOnDataPlacement( long adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<Long> getPartitionsOnDataPlacement( long adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<Long> getPartitionGroupsIndexOnDataPlacement( long adapterId, long tableId ) {
        return null;
    }


    @Override
    public CatalogDataPlacement getDataPlacement( long adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getDataPlacements( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getAllFullDataPlacements( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getAllColumnFullDataPlacements( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getAllPartitionFullDataPlacements( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogDataPlacement> getDataPlacementsByRole( long tableId, DataPlacementRole role ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByRole( long tableId, DataPlacementRole role ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByIdAndRole( long tableId, long partitionId, DataPlacementRole role ) {
        return null;
    }


    @Override
    public CatalogPartitionPlacement getPartitionPlacement( long adapterId, long partitionId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByAdapter( long adapterId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacementsByTableOnAdapter( long adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getAllPartitionPlacementsByTable( long tableId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getPartitionPlacements( long partitionId ) {
        return null;
    }


    @Override
    public boolean isHorizontalPartitioned( long id ) {
        return false;
    }


    @Override
    public boolean isVerticalPartitioned( long id ) {
        return false;
    }


    @Override
    public boolean checkIfExistsPartitionPlacement( long adapterId, long partitionId ) {
        return false;
    }


    @Override
    public List<AllocationTable> getAllocationsFromLogical( long logicalId ) {
        return null;
    }


    @Override
    public boolean isPartitioned( long id ) {
        return false;
    }


    @Override
    public CatalogGraphPlacement getGraphPlacement( long graphId, long adapterId ) {
        return null;
    }


    @Override
    public List<CatalogGraphPlacement> getGraphPlacements( long adapterId ) {
        return null;
    }


    @Override
    public CatalogCollectionPlacement getCollectionPlacement( long id, long placementId ) {
        return null;
    }


    @Override
    public CatalogCollectionMapping getCollectionMapping( long id ) {
        return null;
    }


    @Override
    public List<CatalogCollectionPlacement> getCollectionPlacementsByAdapter( long id ) {
        return null;
    }


    @Override
    public List<CatalogCollectionPlacement> getCollectionPlacements( long collectionId ) {
        return null;
    }


    @Override
    public PartitionProperty getPartitionProperty( long id ) {
        return null;
    }


    @Override
    public boolean adapterHasPlacement( long adapterId, long id ) {
        return false;
    }

}

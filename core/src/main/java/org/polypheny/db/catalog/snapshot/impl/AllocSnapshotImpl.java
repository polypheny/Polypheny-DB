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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.catalogs.AllocationCatalog;
import org.polypheny.db.catalog.catalogs.AllocationDocumentCatalog;
import org.polypheny.db.catalog.catalogs.AllocationGraphCatalog;
import org.polypheny.db.catalog.catalogs.AllocationRelationalCatalog;
import org.polypheny.db.catalog.entity.AllocationColumn;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogCollectionMapping;
import org.polypheny.db.catalog.entity.CatalogCollectionPlacement;
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
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.AllocSnapshot;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.util.Pair;

@Value
@Slf4j
public class AllocSnapshotImpl implements AllocSnapshot {

    ImmutableMap<Long, AllocationTable> tables;
    ImmutableMap<Pair<Long, Long>, AllocationColumn> columns;
    ImmutableMap<Long, AllocationCollection> collections;
    ImmutableMap<Long, AllocationGraph> graphs;

    ImmutableMap<Pair<Long, Long>, AllocationColumn> adapterColumnPlacement;

    ImmutableMap<Long, AllocationEntity> allocs;
    ImmutableMap<Long, List<AllocationEntity>> allocsOnAdapters;
    ImmutableMap<Long, List<AllocationColumn>> logicalColumnToAlloc;

    ImmutableMap<Long, TreeSet<AllocationColumn>> allocColumns;
    ImmutableMap<Pair<Long, Long>, List<AllocationColumn>> adapterLogicalTablePlacements;
    ImmutableMap<Pair<Long, Long>, AllocationEntity> adapterLogicalTableAlloc;
    ImmutableMap<Long, List<AllocationEntity>> logicalAllocs;
    ImmutableMap<Long, Map<Long, List<Long>>> tableAdapterColumns;


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
        this.columns = buildColumns( allocationCatalogs.values()
                .stream()
                .filter( a -> a.getNamespace().namespaceType == NamespaceType.RELATIONAL )
                .map( c -> (AllocationRelationalCatalog) c )
                .map( AllocationRelationalCatalog::getAllocColumns )
                .flatMap( c -> c.values().stream() )
                .collect( Collectors.toList() ) );

        this.allocs = mergeAllocs();
        this.allocsOnAdapters = buildAllocsOnAdapters();
        this.adapterColumnPlacement = buildAdapterColumnPlacement();
        this.logicalColumnToAlloc = buildColumnPlacements();
        this.adapterLogicalTablePlacements = buildAdapterLogicalTablePlacements();
        this.adapterLogicalTableAlloc = buildAdapterLogicalTableAlloc();
        this.allocColumns = buildAllocColumns();
        this.logicalAllocs = buildLogicalAllocs();

        this.tableAdapterColumns = buildTableAdapterColumns();
    }


    private ImmutableMap<Pair<Long, Long>, AllocationColumn> buildColumns( List<AllocationColumn> columns ) {
        return ImmutableMap.copyOf( columns.stream().collect( Collectors.toMap( c -> Pair.of( c.columnId, c.adapterId ), c -> c ) ) );

    }


    private ImmutableMap<Long, Map<Long, List<Long>>> buildTableAdapterColumns() {
        Map<Long, Map<Long, List<Long>>> map = new HashMap<>();
        for ( AllocationColumn column : this.columns.values() ) {
            if ( !map.containsKey( column.tableId ) ) {
                map.put( column.tableId, new HashMap<>() );
            }
            if ( !map.get( column.tableId ).containsKey( column.adapterId ) ) {
                map.get( column.tableId ).put( column.adapterId, new ArrayList<>() );
            }
            map.get( column.tableId ).get( column.adapterId ).add( column.columnId );
        }

        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, List<AllocationEntity>> buildLogicalAllocs() {
        Map<Long, List<AllocationEntity>> map = new HashMap<>();
        allocs.forEach( ( k, v ) -> {
            if ( !map.containsKey( v.logicalId ) ) {
                map.put( v.logicalId, new ArrayList<>() );
            }
            map.get( v.logicalId ).add( v );
        } );
        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, TreeSet<AllocationColumn>> buildAllocColumns() {
        Map<Long, TreeSet<AllocationColumn>> map = new HashMap<>();
        for ( AllocationColumn value : columns.values() ) {
            if ( !map.containsKey( value.tableId ) ) {
                map.put( value.tableId, new TreeSet<>( Comparator.comparingLong( c -> c.position ) ) );
            }
            map.get( value.tableId ).add( value );
        }

        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Pair<Long, Long>, AllocationEntity> buildAdapterLogicalTableAlloc() {
        Map<Pair<Long, Long>, AllocationEntity> map = new HashMap<>();
        this.allocs.forEach( ( k, v ) -> map.put( Pair.of( v.adapterId, v.logicalId ), v ) );
        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Pair<Long, Long>, List<AllocationColumn>> buildAdapterLogicalTablePlacements() {
        Map<Pair<Long, Long>, List<AllocationColumn>> map = new HashMap<>();

        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, List<AllocationColumn>> buildColumnPlacements() {
        Map<Long, List<AllocationColumn>> map = new HashMap<>();
        for ( AllocationColumn column : columns.values() ) {
            if ( !map.containsKey( column.columnId ) ) {
                map.put( column.columnId, new ArrayList<>() );
            }
            map.get( column.columnId ).add( column );
        }

        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Pair<Long, Long>, AllocationColumn> buildAdapterColumnPlacement() {
        Map<Pair<Long, Long>, AllocationColumn> map = new HashMap<>();
        //this.tables.forEach( ( k, v ) -> v.placements.forEach( p -> map.put( Pair.of( v.adapterId, p.columnId ), p ) ) );
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
    public AllocationEntity getAllocation( long id ) {
        return allocs.get( id );
    }


    @Override
    public AllocationColumn getColumn( long adapterId, long columnId ) {
        return adapterColumnPlacement.get( Pair.of( adapterId, columnId ) );
    }


    @Override
    public boolean checkIfExistsColumnPlacement( long adapterId, long columnId ) {
        return adapterColumnPlacement.containsKey( Pair.of( adapterId, columnId ) );
    }


    @Override
    public List<AllocationColumn> getColumnFromLogical( long columnId ) {
        return logicalColumnToAlloc.get( columnId );
    }


    @Override
    public List<AllocationColumn> getColumnPlacementsOnAdapterPerTable( long adapterId, long tableId ) {
        return adapterLogicalTablePlacements.get( Pair.of( adapterId, tableId ) );
    }


    @Override
    public List<AllocationColumn> getColumnPlacementsOnAdapter( long adapterId ) {
        return null;
    }


    @Override
    public List<AllocationColumn> getColumnPlacementsByColumn( long columnId ) {
        return null;
    }


    @Override
    public Map<Long, List<Long>> getColumnPlacementsByAdapter( long tableId ) {
        return tableAdapterColumns.get( tableId );
    }


    @Override
    public long getPartitionGroupByPartition( long partitionId ) {
        return 0;
    }


    @Override
    public List<AllocationColumn> getColumnPlacementsOnAdapterAndSchema( long adapterId, long schemaId ) {
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
    public List<AllocationColumn> getColumnPlacementsByPartitionGroup( long tableId, long partitionGroupId, long columnId ) {
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
    public List<AllocationEntity> getFromLogical( long logicalId ) {
        return logicalAllocs.get( logicalId );
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
        log.warn( "replace me" );
        return new PartitionProperty( PartitionType.NONE, false, List.of(), List.of(), -1, -1, -1, false );
    }


    @Override
    public boolean adapterHasPlacement( long adapterId, long logicalId ) {
        return adapterLogicalTableAlloc.containsKey( Pair.of( adapterId, logicalId ) );
    }


    @Override
    public AllocationEntity getAllocation( long adapterId, long entityId ) {
        return adapterLogicalTableAlloc.get( Pair.of( adapterId, entityId ) );
    }


    @Override
    public List<AllocationColumn> getColumns( long allocId ) {
        return List.copyOf( allocColumns.get( allocId ) );
    }

}

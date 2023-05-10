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
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.catalogs.AllocationCatalog;
import org.polypheny.db.catalog.catalogs.AllocationDocumentCatalog;
import org.polypheny.db.catalog.catalogs.AllocationGraphCatalog;
import org.polypheny.db.catalog.catalogs.AllocationRelationalCatalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.PartitionType;
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
    ImmutableMap<Long, Map<Long, List<Long>>> logicalTableAdapterColumns;


    public AllocSnapshotImpl( Map<Long, AllocationCatalog> allocationCatalogs, Map<Long, CatalogAdapter> adapters ) {
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
        this.allocsOnAdapters = buildAllocsOnAdapters( adapters );
        this.adapterColumnPlacement = buildAdapterColumnPlacement();
        this.logicalColumnToAlloc = buildColumnPlacements();
        this.adapterLogicalTablePlacements = buildAdapterLogicalTablePlacements();
        this.adapterLogicalTableAlloc = buildAdapterLogicalTableAlloc();
        this.allocColumns = buildAllocColumns();
        this.logicalAllocs = buildLogicalAllocs();

        this.logicalTableAdapterColumns = buildTableAdapterColumns();
    }


    private ImmutableMap<Pair<Long, Long>, AllocationColumn> buildColumns( List<AllocationColumn> columns ) {
        return ImmutableMap.copyOf( columns.stream().collect( Collectors.toMap( c -> Pair.of( c.columnId, c.adapterId ), c -> c ) ) );

    }


    private ImmutableMap<Long, Map<Long, List<Long>>> buildTableAdapterColumns() {
        Map<Long, Map<Long, List<Long>>> map = new HashMap<>();
        for ( AllocationColumn column : this.columns.values() ) {
            AllocationTable table = tables.get( column.tableId );
            if ( !map.containsKey( table.logicalId ) ) {
                map.put( table.logicalId, new HashMap<>() );
            }
            if ( !map.get( table.logicalId ).containsKey( column.adapterId ) ) {
                map.get( table.logicalId ).put( column.adapterId, new ArrayList<>() );
            }
            map.get( table.logicalId ).get( column.adapterId ).add( column.columnId );
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


    private ImmutableMap<Long, List<AllocationEntity>> buildAllocsOnAdapters( Map<Long, CatalogAdapter> adapters ) {
        Map<Long, List<AllocationEntity>> allocs = new HashMap<>();

        for ( CatalogAdapter adapter : adapters.values() ) {
            allocs.put( adapter.id, new ArrayList<>() );
        }
        this.allocs.forEach( ( k, v ) -> allocs.get( v.adapterId ).add( v ) );
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
    public @NonNull Optional<List<AllocationEntity>> getEntitiesOnAdapter( long id ) {
        return Optional.ofNullable( allocsOnAdapters.get( id ) );
    }


    @Override
    public @NonNull Optional<AllocationEntity> getEntity( long id ) {
        return Optional.ofNullable( allocs.get( id ) );
    }


    @Override
    public @NonNull Optional<AllocationColumn> getColumn( long adapterId, long columnId ) {
        return Optional.ofNullable( adapterColumnPlacement.get( Pair.of( adapterId, columnId ) ) );
    }


    @Override
    public @NonNull Optional<List<AllocationColumn>> getColumnFromLogical( long columnId ) {
        return Optional.ofNullable( logicalColumnToAlloc.get( columnId ) );
    }


    @Override
    public @NonNull List<AllocationColumn> getColumnPlacementsOnAdapterPerTable( long adapterId, long tableId ) {
        return Optional.ofNullable( adapterLogicalTablePlacements.get( Pair.of( adapterId, tableId ) ) ).orElse( List.of() );
    }


    @Override
    public @NonNull Map<Long, List<Long>> getColumnPlacementsByAdapter( long tableId ) {
        return Optional.ofNullable( logicalTableAdapterColumns.get( tableId ) ).orElse( Map.of() );
    }


    @Override
    public List<CatalogPartition> getPartitions( long partitionGroupId ) {
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
    public List<CatalogPartitionPlacement> getPartitionPlacementsByTableOnAdapter( long adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<CatalogPartitionPlacement> getAllPartitionPlacementsByTable( long tableId ) {
        return null;
    }


    @Override
    public @NonNull List<AllocationEntity> getFromLogical( long logicalId ) {
        return Optional.ofNullable( logicalAllocs.get( logicalId ) ).orElse( List.of() );
    }


    @Override
    public PartitionProperty getPartitionProperty( long id ) {
        log.warn( "replace me" );
        return new PartitionProperty( PartitionType.NONE, false, List.of(), List.of(), -1, -1, -1, false );
    }


    @Override
    public @NotNull Optional<AllocationEntity> getEntity( long adapterId, long entityId ) {
        return Optional.ofNullable( adapterLogicalTableAlloc.get( Pair.of( adapterId, entityId ) ) );
    }


    @Override
    public @NonNull List<AllocationColumn> getColumns( long allocId ) {
        return Optional.ofNullable( List.copyOf( allocColumns.get( allocId ) ) ).orElse( List.of() );
    }

}

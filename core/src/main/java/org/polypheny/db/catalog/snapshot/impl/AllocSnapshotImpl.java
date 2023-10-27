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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.catalogs.AllocationCatalog;
import org.polypheny.db.catalog.catalogs.AllocationDocumentCatalog;
import org.polypheny.db.catalog.catalogs.AllocationGraphCatalog;
import org.polypheny.db.catalog.catalogs.AllocationRelationalCatalog;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationPartitionGroup;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.logistic.NamespaceType;
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

    ImmutableMap<Long, AllocationEntity> allocs;
    ImmutableMap<Long, AllocationPartition> partitions;
    ImmutableMap<Long, AllocationPartitionGroup> groups;
    ImmutableMap<Long, AllocationPlacement> placements;
    ImmutableMap<Long, List<AllocationEntity>> allocsOnAdapters;
    ImmutableMap<Long, List<AllocationColumn>> logicalColumnToAlloc;

    ImmutableMap<Long, List<AllocationColumn>> placementColumns;
    ImmutableMap<Pair<Long, Long>, List<AllocationColumn>> adapterLogicalTablePlacements;
    ImmutableMap<Pair<Long, Long>, AllocationEntity> adapterLogicalTableAlloc;
    ImmutableMap<Long, List<AllocationEntity>> logicalAllocs;
    ImmutableMap<Long, Map<Long, List<Long>>> logicalTablePlacementColumns;

    ImmutableMap<Long, PartitionProperty> properties;
    ImmutableMap<Long, List<AllocationPartition>> logicalToPartitions;
    ImmutableMap<Long, List<AllocationPartitionGroup>> logicalToGroups;
    ImmutableMap<Long, List<AllocationPlacement>> logicalToPlacements;
    ImmutableMap<Pair<Long, Long>, AllocationEntity> placementPartitionToAlloc;
    ImmutableMap<Pair<Long, Long>, AllocationPlacement> adapterLogicalToPlacement;
    ImmutableMap<Long, List<AllocationEntity>> placementToPartitions;
    ImmutableMap<Long, List<AllocationPlacement>> placementsOfColumn;

    ImmutableMap<Pair<Long, String>, AllocationPartition> entityPartitionNameToPartition;


    public AllocSnapshotImpl( Map<Long, AllocationCatalog> allocationCatalogs, Map<Long, LogicalAdapter> adapters ) {
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

        this.columns = buildPlacementColumns( allocationCatalogs.values()
                .stream()
                .filter( a -> a.getNamespace().namespaceType == NamespaceType.RELATIONAL )
                .map( c -> (AllocationRelationalCatalog) c )
                .map( AllocationRelationalCatalog::getColumns )
                .flatMap( c -> c.values().stream() )
                .collect( Collectors.toList() ) );

        this.groups = buildPartitionGroups( allocationCatalogs );
        this.partitions = buildPartitions( allocationCatalogs );
        this.placements = buildPlacements( allocationCatalogs );

        this.allocs = mergeAllocs();
        this.allocsOnAdapters = buildAllocsOnAdapters( adapters );
        this.logicalColumnToAlloc = buildColumnPlacements();
        this.adapterLogicalTablePlacements = buildAdapterLogicalTablePlacements();
        this.adapterLogicalTableAlloc = buildAdapterLogicalTableAlloc();
        this.placementColumns = buildPlacementColumns();
        this.logicalAllocs = buildLogicalAllocs();

        this.logicalTablePlacementColumns = buildTableAdapterColumns();

        this.properties = ImmutableMap.copyOf( allocationCatalogs.values()
                .stream()
                .filter( a -> a.getNamespace().namespaceType == NamespaceType.RELATIONAL )
                .map( c -> (AllocationRelationalCatalog) c )
                .map( AllocationRelationalCatalog::getProperties )
                .flatMap( c -> c.values().stream() )
                .collect( Collectors.toMap( c -> c.entityId, c -> c ) ) );

        this.logicalToPartitions = buildLogicalToPartitions();
        this.logicalToGroups = buildLogicalGroups();
        this.logicalToPlacements = buildLogicalToPlacements();

        this.placementPartitionToAlloc = buildPlacementPartitionToAlloc();
        this.adapterLogicalToPlacement = buildAdapterLogicalToPlacement();
        this.placementToPartitions = buildPlacementToPartitions();
        this.placementsOfColumn = buildPlacementsOfColumn();

        this.entityPartitionNameToPartition = buildEntityPartitionNameToPartition();

    }


    private ImmutableMap<Pair<Long, String>, AllocationPartition> buildEntityPartitionNameToPartition() {
        Map<Pair<Long, String>, AllocationPartition> map = new HashMap<>();
        for ( Entry<Long, AllocationPartition> entry : partitions.entrySet() ) {
            Pair<Long, String> key = Pair.of( entry.getValue().logicalEntityId, entry.getValue().name );
            if ( !map.containsKey( key ) ) {
                map.put( key, entry.getValue() );
            }
        }

        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, List<AllocationPlacement>> buildPlacementsOfColumn() {
        Map<Long, List<AllocationPlacement>> map = new HashMap<>();
        for ( AllocationColumn value : this.columns.values() ) {
            if ( !map.containsKey( value.columnId ) ) {
                map.put( value.columnId, new ArrayList<>() );
            }
            map.get( value.columnId ).add( placements.get( value.placementId ) );
        }
        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, List<AllocationEntity>> buildPlacementToPartitions() {
        Map<Long, List<AllocationEntity>> map = new HashMap<>();
        for ( AllocationEntity value : this.allocs.values() ) {
            if ( !map.containsKey( value.placementId ) ) {
                map.put( value.placementId, new ArrayList<>() );
            }
            map.get( value.placementId ).add( value );
        }
        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Pair<Long, Long>, AllocationPlacement> buildAdapterLogicalToPlacement() {
        Map<Pair<Long, Long>, AllocationPlacement> map = new HashMap<>();
        for ( AllocationPlacement value : this.placements.values() ) {
            Pair<Long, Long> key = Pair.of( value.adapterId, value.logicalEntityId );
            map.put( key, value );
        }
        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Pair<Long, Long>, AllocationEntity> buildPlacementPartitionToAlloc() {
        Map<Pair<Long, Long>, AllocationEntity> map = new HashMap<>();
        for ( AllocationEntity value : this.allocs.values() ) {
            Pair<Long, Long> key = Pair.of( value.placementId, value.partitionId );
            map.put( key, value );
        }
        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, List<AllocationPlacement>> buildLogicalToPlacements() {
        Map<Long, List<AllocationPlacement>> map = new HashMap<>();
        for ( AllocationPlacement value : this.placements.values() ) {
            if ( !map.containsKey( value.logicalEntityId ) ) {
                map.put( value.logicalEntityId, new ArrayList<>() );
            }
            map.get( value.logicalEntityId ).add( value );
        }
        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, AllocationPlacement> buildPlacements( Map<Long, AllocationCatalog> allocationCatalogs ) {
        return ImmutableMap.copyOf( allocationCatalogs.values()
                .stream()
                .map( AllocationCatalog::getPlacements )
                .flatMap( c -> c.values().stream() )
                .collect( Collectors.toMap( c -> c.id, c -> c ) ) );
    }


    private ImmutableMap<Long, AllocationPartition> buildPartitions( Map<Long, AllocationCatalog> allocationCatalogs ) {
        return ImmutableMap.copyOf( allocationCatalogs.values()
                .stream()
                .map( AllocationCatalog::getPartitions )
                .flatMap( c -> c.values().stream() )
                .collect( Collectors.toMap( c -> c.id, c -> c ) ) );
    }


    private ImmutableMap<Long, AllocationPartitionGroup> buildPartitionGroups( Map<Long, AllocationCatalog> allocationCatalogs ) {
        return ImmutableMap.copyOf( allocationCatalogs.values()
                .stream()
                .filter( a -> a.getNamespace().namespaceType == NamespaceType.RELATIONAL )
                .map( c -> (AllocationRelationalCatalog) c )
                .map( AllocationRelationalCatalog::getPartitionGroups )
                .flatMap( c -> c.values().stream() )
                .collect( Collectors.toMap( c -> c.id, c -> c ) ) );

    }


    private ImmutableMap<Long, List<AllocationPartitionGroup>> buildLogicalGroups() {
        Map<Long, List<AllocationPartitionGroup>> map = new HashMap<>();
        for ( AllocationPartitionGroup value : this.groups.values() ) {
            if ( !map.containsKey( value.logicalEntityId ) ) {
                map.put( value.logicalEntityId, new ArrayList<>() );
            }
            map.get( value.logicalEntityId ).add( value );
        }
        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Long, List<AllocationPartition>> buildLogicalToPartitions() {
        Map<Long, List<AllocationPartition>> map = new HashMap<>();
        for ( AllocationPartition value : this.partitions.values() ) {
            if ( !map.containsKey( value.logicalEntityId ) ) {
                map.put( value.logicalEntityId, new ArrayList<>() );
            }
            map.get( value.logicalEntityId ).add( value );
        }
        return ImmutableMap.copyOf( map );
    }


    private ImmutableMap<Pair<Long, Long>, AllocationColumn> buildPlacementColumns( List<AllocationColumn> columns ) {
        return ImmutableMap.copyOf( columns.stream().collect( Collectors.toMap( c -> Pair.of( c.placementId, c.columnId ), c -> c ) ) );

    }


    private ImmutableMap<Long, Map<Long, List<Long>>> buildTableAdapterColumns() {
        Map<Long, Map<Long, List<Long>>> map = new HashMap<>();
        for ( AllocationColumn column : this.columns.values() ) {
            if ( !map.containsKey( column.logicalTableId ) ) {
                map.put( column.logicalTableId, new HashMap<>() );
            }
            if ( !map.get( column.logicalTableId ).containsKey( column.adapterId ) ) {
                map.get( column.logicalTableId ).put( column.adapterId, new ArrayList<>() );
            }
            map.get( column.logicalTableId ).get( column.adapterId ).add( column.columnId );
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


    private ImmutableMap<Long, List<AllocationColumn>> buildPlacementColumns() {
        Map<Long, TreeSet<AllocationColumn>> map = new HashMap<>();
        for ( AllocationColumn value : columns.values() ) {
            if ( !map.containsKey( value.placementId ) ) {
                map.put( value.placementId, new TreeSet<>( ( a, b ) -> AllocSnapshotImpl.comparatorOrId( () -> a.position - b.position, a, b ) ) );
            }
            map.get( value.placementId ).add( value );
        }

        return ImmutableMap.copyOf( map.entrySet().stream().collect( Collectors.toMap( Entry::getKey, e -> new ArrayList<>( e.getValue() ) ) ) );
    }


    private static int comparatorOrId( Supplier<Integer> comparator, AllocationColumn a, AllocationColumn b ) {
        int diff = comparator.get();
        return diff != 0 ? diff : (int) (b.columnId - a.columnId);
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


    private ImmutableMap<Long, AllocationEntity> mergeAllocs() {
        Map<Long, AllocationEntity> allocs = new HashMap<>();
        allocs.putAll( this.tables );
        allocs.putAll( this.collections );
        allocs.putAll( this.graphs );

        return ImmutableMap.copyOf( allocs );
    }


    private ImmutableMap<Long, List<AllocationEntity>> buildAllocsOnAdapters( Map<Long, LogicalAdapter> adapters ) {
        Map<Long, List<AllocationEntity>> allocs = new HashMap<>();

        for ( LogicalAdapter adapter : adapters.values() ) {
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
    public @NotNull List<AllocationColumn> getColumns() {
        return columns.values().asList();
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
    public @NonNull Optional<AllocationColumn> getColumn( long placementId, long columnId ) {
        return Optional.ofNullable( columns.get( Pair.of( placementId, columnId ) ) );
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
    public @NonNull Map<Long, List<Long>> getColumnPlacementsByAdapters( long tableId ) {
        return Optional.ofNullable( logicalTablePlacementColumns.get( tableId ) ).orElse( Map.of() );
    }


    @Override
    public List<AllocationPartition> getPartitions( long partitionGroupId ) {
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
    public List<LogicalAdapter> getAdaptersByPartitionGroup( long tableId, long partitionGroupId ) {
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
    public @NotNull Optional<AllocationPlacement> getPlacement( long adapterId, long logicalTableId ) {
        return Optional.ofNullable( adapterLogicalToPlacement.get( Pair.of( adapterId, logicalTableId ) ) );
    }


    @Override
    @Deprecated
    public List<CatalogDataPlacement> getDataPlacements( long tableId ) {
        return null;
    }


    @Override
    public List<AllocationPartition> getPartitionPlacementsByTableOnAdapter( long adapterId, long tableId ) {
        return null;
    }


    @Override
    public List<AllocationPartition> getAllPartitionPlacementsByTable( long tableId ) {
        return null;
    }


    @Override
    public @NonNull List<AllocationEntity> getFromLogical( long logicalId ) {
        return Optional.ofNullable( logicalAllocs.get( logicalId ) ).orElse( List.of() );
    }


    @Override
    public @NotNull Optional<PartitionProperty> getPartitionProperty( long id ) {
        return Optional.ofNullable( properties.get( id ) );
    }


    @Override
    public @NotNull Optional<AllocationEntity> getEntity( long adapterId, long entityId ) {
        return Optional.ofNullable( adapterLogicalTableAlloc.get( Pair.of( adapterId, entityId ) ) );
    }


    @Override
    public @NonNull List<AllocationColumn> getColumns( long placementId ) {
        return Optional.ofNullable( placementColumns.get( placementId ) ).orElse( List.of() );
    }


    @Override
    public @NotNull List<AllocationPartitionGroup> getPartitionGroupsFromLogical( long logicalId ) {
        return Optional.ofNullable( logicalToGroups.get( logicalId ) ).orElse( List.of() );
    }


    @Override
    public @NotNull List<AllocationPartition> getPartitionsFromLogical( long logicalId ) {
        return Optional.ofNullable( logicalToPartitions.get( logicalId ) ).orElse( List.of() );
    }


    @Override
    public @NotNull List<AllocationPlacement> getPlacementsFromLogical( long logicalId ) {
        return Optional.ofNullable( logicalToPlacements.get( logicalId ) ).orElse( List.of() );
    }


    @Override
    public @NotNull Optional<AllocationEntity> getAlloc( long placementId, long partitionId ) {
        return Optional.ofNullable( placementPartitionToAlloc.get( Pair.of( placementId, partitionId ) ) );
    }


    @Override
    public @NotNull List<AllocationEntity> getAllocsOfPlacement( long placementId ) {
        return Optional.ofNullable( placementToPartitions.get( placementId ) ).orElse( List.of() );
    }


    @Override
    public @NotNull List<AllocationPlacement> getPlacementsOfColumn( long logicalId ) {
        return Optional.ofNullable( placementsOfColumn.get( logicalId ) ).orElse( List.of() );
    }


    @Override
    public @NotNull Optional<AllocationPartition> getPartition( long partitionId ) {
        return Optional.ofNullable( partitions.get( partitionId ) );
    }


    @Override
    public Optional<AllocationPartition> getPartitionFromName( long logicalId, String name ) {
        return Optional.ofNullable( entityPartitionNameToPartition.get( Pair.of( logicalId, name ) ) );
    }


    @Override
    public @NotNull List<AllocationEntity> getAllocations() {
        return allocs.values().asList();
    }


    @Override
    public @NotNull List<AllocationPlacement> getPlacements() {
        return placements.values().asList();
    }


    @Override
    public @NotNull List<AllocationPartition> getPartitions() {
        return partitions.values().asList();
    }


}

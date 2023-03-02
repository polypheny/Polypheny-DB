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

package org.polypheny.db.catalog.allocation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.ConnectedMap;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.Serializable;
import org.polypheny.db.catalog.catalogs.AllocationRelationalCatalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogDataPlacement;
import org.polypheny.db.catalog.entity.CatalogPartition;
import org.polypheny.db.catalog.entity.CatalogPartitionGroup;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.logistic.DataPlacementRole;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.util.Pair;

@Slf4j
public class PolyAllocRelCatalog implements AllocationRelationalCatalog, Serializable {


    private final IdBuilder idBuilder = IdBuilder.getInstance();

    @Getter
    public BinarySerializer<PolyAllocRelCatalog> serializer = Serializable.builder.get().build( PolyAllocRelCatalog.class );

    @Serialize
    public final ConnectedMap<Long, AllocationTable> allocations;

    private final ConcurrentHashMap<Pair<Long, Long>, Long> adapterLogicalToAllocId;
    private final ConcurrentHashMap<Pair<Long, Long>, AllocationTable> adapterLogicalColumnToAlloc;
    private final ConcurrentHashMap<Long, List<CatalogColumnPlacement>> logicalColumnToPlacements;
    private final ConcurrentHashMap<Pair<Long, Long>, List<AllocationTable>> adapterLogicalTableToAllocs;

    private final ConcurrentHashMap<Long, List<AllocationTable>> adapterToAllocs;

    private final ConcurrentHashMap<Long, List<AllocationTable>> logicalTableToAllocs;


    public PolyAllocRelCatalog(
            @Deserialize("allocations") Map<Long, AllocationTable> allocations ) {
        this.allocations = new ConnectedMap<>( allocations );
        this.adapterLogicalToAllocId = new ConcurrentHashMap<>();
        this.allocations.addRowConnection( this.adapterLogicalToAllocId, ( k, v ) -> Pair.of( v.adapterId, v.logical.id ), ( k, v ) -> k );
        this.adapterLogicalColumnToAlloc = new ConcurrentHashMap<>();
        this.allocations.addRowConnection( this.adapterLogicalColumnToAlloc, ( k, v ) -> Pair.of( v.adapterId, v.logical.id ), ( k, v ) -> v );
        ////
        this.logicalColumnToPlacements = new ConcurrentHashMap<>();
        this.allocations.addConnection( a -> {
            logicalColumnToPlacements.clear();
            a.forEach( ( k, v ) -> v.placements.forEach( p -> {
                if ( logicalColumnToPlacements.containsKey( p.columnId ) ) {
                    logicalColumnToPlacements.get( p.columnId ).add( p );
                } else {
                    logicalColumnToPlacements.put( p.columnId, new ArrayList<>( List.of( p ) ) );
                }
            } ) );
        } );

        ////
        this.adapterLogicalTableToAllocs = new ConcurrentHashMap<>();
        this.allocations.addConnection( a -> a.forEach( ( k, v ) -> {
            if ( adapterLogicalTableToAllocs.containsKey( Pair.of( v.adapterId, v.logical.id ) ) ) {
                adapterLogicalTableToAllocs.get( Pair.of( v.adapterId, v.logical.id ) ).add( v );
            } else {
                adapterLogicalTableToAllocs.put( Pair.of( v.adapterId, v.logical.id ), new ArrayList<>( List.of( v ) ) );
            }
        } ) );

        ////
        this.adapterToAllocs = new ConcurrentHashMap<>();
        this.allocations.addConnection( a -> {
            adapterToAllocs.clear();
            for ( AllocationTable value : a.values() ) {
                if ( adapterToAllocs.containsKey( value.adapterId ) ) {
                    adapterToAllocs.get( value.adapterId ).add( value );
                } else {
                    adapterToAllocs.put( value.adapterId, new ArrayList<>( List.of( value ) ) );
                }
            }
        } );

        ////
        this.logicalTableToAllocs = new ConcurrentHashMap<>();
        this.allocations.addConnection( a -> {
            logicalTableToAllocs.clear();
            for ( AllocationTable table : a.values() ) {
                if ( logicalTableToAllocs.containsKey( table.logical.id ) ) {
                    logicalTableToAllocs.get( table.logical.id ).add( table );
                } else {
                    logicalTableToAllocs.put( table.logical.id, new ArrayList<>( List.of( table ) ) );
                }
            }
        } );
    }


    @Override
    public PolyAllocRelCatalog copy() {
        return deserialize( serialize(), PolyAllocRelCatalog.class );
    }


    // move to Snapshot
    @Override
    public List<AllocationTable> getAllocationsOnAdapter( long id ) {
        return null;
    }


    @Nullable
    private Long getAllocId( long adapterId, long tableId ) {
        Long allocId = adapterLogicalToAllocId.get( Pair.of( adapterId, tableId ) );
        if ( allocId == null ) {
            log.warn( "AllocationEntity does not yet exist." );
            return null;
        }
        return allocId;
    }


    @Override
    public void addColumnPlacement( long adapterId, long columnId, PlacementType placementType, String physicalSchemaName, String physicalTableName, String physicalColumnName ) {
        allocations.put( adapterLogicalToAllocId.get( Pair.of( adapterId, columnId ) ), adapterLogicalColumnToAlloc.get( Pair.of( adapterId, columnId ) ).withAddedColumn( columnId, placementType, physicalSchemaName, physicalTableName, physicalColumnName ) );
    }


    // might replace above one with this
    private void addColumnPlacementAlloc( long allocTableId, long columnId, PlacementType placementType, String physicalSchemaName, String physicalTableName, String physicalColumnName ) {
        allocations.put( allocTableId, allocations.get( allocTableId ).withAddedColumn( columnId, placementType, physicalSchemaName, physicalTableName, physicalColumnName ) );
    }


    @Override
    public void deleteColumnPlacement( long adapterId, long columnId, boolean columnOnly ) {
        allocations.put( adapterLogicalToAllocId.get( Pair.of( adapterId, columnId ) ), adapterLogicalColumnToAlloc.get( Pair.of( adapterId, columnId ) ).withRemovedColumn( columnId ) );
    }


    // might replace above one with this
    private void deleteColumnPlacementAlloc( long allocTableId, long columnId, boolean columnOnly ) {
        allocations.put( allocTableId, allocations.get( allocTableId ).withRemovedColumn( columnId ) );
    }


    @Override
    public CatalogColumnPlacement getColumnPlacement( long adapterId, long columnId ) {
        return allocations.get( adapterLogicalToAllocId.get( Pair.of( adapterId, columnId ) ) ).placements.stream().filter( p -> p.columnId == columnId ).findFirst().orElse( null );
    }


    @Override
    public boolean checkIfExistsColumnPlacement( long adapterId, long columnId ) {
        return allocations.get( adapterLogicalToAllocId.get( Pair.of( adapterId, columnId ) ) ).placements.stream().anyMatch( p -> p.columnId == columnId );
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacements( long columnId ) {
        return logicalColumnToPlacements.get( columnId );
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapterPerTable( long adapterId, long tableId ) {
        return adapterLogicalTableToAllocs.get( Pair.of( adapterId, tableId ) ).stream().flatMap( a -> a.placements.stream() ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsOnAdapter( long adapterId ) {
        return adapterToAllocs.get( adapterId ).stream().flatMap( a -> a.placements.stream() ).collect( Collectors.toList() );
    }


    @Override
    public List<CatalogColumnPlacement> getColumnPlacementsByColumn( long columnId ) {
        return null;
    }


    @Override
    public ImmutableMap<Integer, ImmutableList<Long>> getColumnPlacementsByAdapter( long tableId ) {
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
    public void updateColumnPlacementType( long adapterId, long columnId, PlacementType placementType ) {

    }


    @Override
    public void updateColumnPlacementPhysicalPosition( long adapterId, long columnId, long position ) {

    }


    @Override
    public void updateColumnPlacementPhysicalPosition( long adapterId, long columnId ) {

    }


    @Override
    public void updateColumnPlacementPhysicalNames( long adapterId, long columnId, String physicalSchemaName, String physicalColumnName, boolean updatePhysicalColumnPosition ) {

    }


    @Override
    public long addPartitionGroup( long tableId, String partitionGroupName, long schemaId, PartitionType partitionType, long numberOfInternalPartitions, List<String> effectivePartitionGroupQualifier, boolean isUnbound ) throws GenericCatalogException {
        return 0;
    }


    @Override
    public void deletePartitionGroup( long tableId, long schemaId, long partitionGroupId ) {

    }


    @Override
    public CatalogPartitionGroup getPartitionGroup( long partitionGroupId ) {
        return null;
    }


    @Override
    public long addPartition( long tableId, long schemaId, long partitionGroupId, List<String> effectivePartitionGroupQualifier, boolean isUnbound ) throws GenericCatalogException {
        return 0;
    }


    @Override
    public void deletePartition( long tableId, long schemaId, long partitionId ) {

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
    public void partitionTable( long tableId, PartitionType partitionType, long partitionColumnId, int numPartitionGroups, List<Long> partitionGroupIds, PartitionProperty partitionProperty ) {

    }


    @Override
    public void mergeTable( long tableId ) {

    }


    @Override
    public void updateTablePartitionProperties( long tableId, PartitionProperty partitionProperty ) {

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
    public void updatePartitionGroup( long partitionGroupId, List<Long> partitionIds ) {

    }


    @Override
    public void addPartitionToGroup( long partitionGroupId, Long partitionId ) {

    }


    @Override
    public void removePartitionFromGroup( long partitionGroupId, Long partitionId ) {

    }


    @Override
    public void updatePartition( long partitionId, Long partitionGroupId ) {

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
    public List<Long> getPartitionGroupsIndexOnDataPlacement( int adapterId, long tableId ) {
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
    public boolean validateDataPlacementsConstraints( long tableId, long adapterId, List<Long> columnIdsToBeRemoved, List<Long> partitionsIdsToBeRemoved ) {
        return false;
    }


    @Override
    public void addPartitionPlacement( long namespaceId, long adapterId, long tableId, long partitionId, PlacementType placementType, String physicalSchemaName, String physicalTableName, DataPlacementRole role ) {

    }


    @Override
    public void addDataPlacement( long adapterId, long tableId ) {

    }


    @Override
    public CatalogDataPlacement addDataPlacementIfNotExists( long adapterId, long tableId ) {
        return null;
    }


    @Override
    public void modifyDataPlacement( long adapterId, long tableId, CatalogDataPlacement catalogDataPlacement ) {

    }


    @Override
    public void removeDataPlacement( long adapterId, long tableId ) {

    }


    @Override
    public void addSingleDataPlacementToTable( long adapterId, long tableId ) {

    }


    @Override
    public void removeSingleDataPlacementFromTable( long adapterId, long tableId ) {

    }


    @Override
    public void updateDataPlacementsOnTable( long tableId, List<Integer> newDataPlacements ) {

    }


    @Override
    public void addColumnsToDataPlacement( long adapterId, long tableId, List<Long> columnIds ) {

    }


    @Override
    public void removeColumnsFromDataPlacement( long adapterId, long tableId, List<Long> columnIds ) {

    }


    @Override
    public void addPartitionsToDataPlacement( long adapterId, long tableId, List<Long> partitionIds ) {

    }


    @Override
    public void removePartitionsFromDataPlacement( long adapterId, long tableId, List<Long> partitionIds ) {

    }


    @Override
    public void updateDataPlacement( long adapterId, long tableId, List<Long> columnIds, List<Long> partitionIds ) {

    }


    @Override
    public void deletePartitionPlacement( long adapterId, long partitionId ) {

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
    public void addTableToPeriodicProcessing( long tableId ) {

    }


    @Override
    public void removeTableFromPeriodicProcessing( long tableId ) {

    }


    @Override
    public boolean checkIfExistsPartitionPlacement( long adapterId, long partitionId ) {
        return false;
    }


    @Override
    public List<AllocationTable> getAllocationsFromLogical( long logicalId ) {
        return logicalTableToAllocs.get( logicalId );
    }


}

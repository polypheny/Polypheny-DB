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

package org.polypheny.db.catalog.impl.allocation;

import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.AllocationRelationalCatalog;
import org.polypheny.db.catalog.entity.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationPartitionGroup;
import org.polypheny.db.catalog.entity.allocation.AllocationPartitionOld;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.DataPlacementRole;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.util.Pair;

@Slf4j
@Value
public class PolyAllocRelCatalog implements AllocationRelationalCatalog, PolySerializable {


    IdBuilder idBuilder = IdBuilder.getInstance();

    @Getter
    @Serialize
    public LogicalNamespace namespace;

    @Getter
    public BinarySerializer<PolyAllocRelCatalog> serializer = PolySerializable.builder.get().build( PolyAllocRelCatalog.class );

    @Serialize
    @Getter
    public ConcurrentHashMap<Long, AllocationTable> tables;

    @Serialize
    @Getter
    public ConcurrentHashMap<Pair<Long, Long>, AllocationColumn> columns;

    @Serialize
    @Getter
    public ConcurrentHashMap<Long, PartitionProperty> properties;

    @Serialize
    @Getter
    public ConcurrentHashMap<Long, AllocationPartitionGroup> partitionGroups;

    @Serialize
    @Getter
    public ConcurrentHashMap<Long, AllocationPartition> partitions;

    @Serialize
    @Getter
    public ConcurrentHashMap<Pair<Long, Long>, AllocationPartitionOld> allocationPartitions;


    public PolyAllocRelCatalog( LogicalNamespace namespace ) {
        this(
                namespace,
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>(),
                new HashMap<>() );
    }


    public PolyAllocRelCatalog(
            @Deserialize("namespace") LogicalNamespace namespace,
            @Deserialize("tables") Map<Long, AllocationTable> tables,
            @Deserialize("columns") Map<Pair<Long, Long>, AllocationColumn> columns,
            @Deserialize("partitionGroups") Map<Long, AllocationPartitionGroup> partitionGroups,
            @Deserialize("partitions") Map<Long, AllocationPartition> partitions,
            @Deserialize("properties") Map<Long, PartitionProperty> properties,
            @Deserialize("allocationPartitions") Map<Pair<Long, Long>, AllocationPartitionOld> allocationPartitions ) {
        this.namespace = namespace;
        this.tables = new ConcurrentHashMap<>( tables );
        this.columns = new ConcurrentHashMap<>( columns );
        this.partitionGroups = new ConcurrentHashMap<>( partitionGroups );
        this.partitions = new ConcurrentHashMap<>( partitions );
        this.properties = new ConcurrentHashMap<>( properties );
        this.allocationPartitions = new ConcurrentHashMap<>( allocationPartitions );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyAllocRelCatalog.class );
    }

    // move to Snapshot


    @Override
    public AllocationColumn addColumn( long partitionId, long allocationTableId, long logicalTableId, long columnId, PlacementType placementType, int position ) {
        AllocationColumn column = new AllocationColumn(
                namespace.id,
                partitionId,
                allocationTableId,
                logicalTableId,
                columnId,
                placementType,
                position,
                tables.get( allocationTableId ).adapterId );

        columns.put( Pair.of( partitionId, columnId ), column );
        return column;
    }


    @Override
    public void deleteColumn( long allocationId, long columnId ) {
        columns.remove( Pair.of( allocationId, columnId ) );
    }


    @Override
    public void updateColumnPlacementType( long adapterId, long columnId, PlacementType placementType ) {

    }


    @Override
    public AllocationPartitionGroup addPartitionGroup( long tableId, String partitionGroupName, long namespaceId, PartitionType partitionType, long numberOfInternalPartitions, List<String> effectivePartitionGroupQualifier, boolean isUnbound ) {
        long id = idBuilder.getNewGroupId();
        if ( log.isDebugEnabled() ) {
            log.debug( "Creating partitionGroup of type '{}' with id '{}'", partitionType, id );
        }

        AllocationPartitionGroup partitionGroup = new AllocationPartitionGroup(
                id,
                partitionGroupName,
                tableId,
                namespaceId,
                0,
                null,
                isUnbound );

        partitionGroups.put( id, partitionGroup );
        return partitionGroup;
    }


    @Override
    public void deletePartitionGroup( long tableId, long schemaId, long partitionGroupId ) {
        /*if ( log.isDebugEnabled() ) {
            log.debug( "Deleting partitionGroup with id '{}'", partitionGroupId );
        }

        partitionGroups.remove( partitionGroupId );*/
        return;
    }


    @Override
    public AllocationPartition addPartition( long tableId, long schemaId, long partitionGroupId, List<String> effectivePartitionGroupQualifier, boolean isUnbound ) {
        long id = idBuilder.getNewAllocId();
        if ( log.isDebugEnabled() ) {
            log.debug( "Creating partition with id '{}'", id );
        }

        AllocationPartition partition = new AllocationPartition(
                id,
                tableId,
                schemaId,
                effectivePartitionGroupQualifier,
                isUnbound,
                partitionGroupId );

        partitions.put( id, partition );
        return partition;
    }


    @Override
    public void deletePartition( long tableId, long schemaId, long partitionId ) {

    }


    @Override
    public void addPartitionProperty( long tableId, PartitionProperty partitionProperty ) {
        properties.put( tableId, partitionProperty );
    }


    @Override
    public void partitionTable( long tableId, PartitionProperty partitionProperty ) {
        properties.put( partitionProperty.entityId, partitionProperty );

        /*if ( partitionProperty.reliesOnPeriodicChecks ) {
            addTableToPeriodicProcessing( tableId );
        }*/
    }


    @Override
    public void mergeTable( long tableId ) {

    }


    @Override
    public void updatePartition( long partitionId, Long partitionGroupId ) {

    }


    @Override
    public boolean validateDataPlacementsConstraints( long tableId, long adapterId, List<Long> columnIdsToBeRemoved, List<Long> partitionsIdsToBeRemoved ) {
        return false;
    }


    @Override
    public AllocationPartitionOld addPartitionPlacement( long namespaceId, long adapterId, long tableId, PlacementType placementType, DataPlacementRole role ) {
        long id = idBuilder.getNewPartitionId();
        AllocationPartitionOld partition = new AllocationPartitionOld(
                id,
                namespaceId,
                tableId,
                adapterId,
                placementType,
                role );

        allocationPartitions.put( Pair.of( adapterId, id ), partition );
        return partition;
    }


    @Override
    public AllocationTable addAllocation( long adapterId, long partitionId, long logicalId ) {
        long id = idBuilder.getNewAllocId();
        AllocationTable table = new AllocationTable( id, partitionId, logicalId, namespace.id, adapterId );
        tables.put( id, table );
        return table;
    }


    @Override
    public void deleteAllocation( long allocId ) {
        tables.remove( allocId );

    }


    @Override
    public void updateDataPlacement( long adapterId, long tableId, List<Long> columnIds, List<Long> partitionIds ) {

    }


    @Override
    public void deletePartitionPlacement( long adapterId, long partitionId ) {

    }


}

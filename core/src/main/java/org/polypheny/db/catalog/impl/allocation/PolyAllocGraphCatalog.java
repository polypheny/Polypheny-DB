/*
 * Copyright 2019-2024 The Polypheny Project
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
import java.beans.PropertyChangeSupport;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Value;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.catalogs.AllocationGraphCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationPartition;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.logistic.DataPlacementRole;
import org.polypheny.db.catalog.logistic.PartitionType;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.type.PolySerializable;

@Value
public class PolyAllocGraphCatalog implements PolySerializable, AllocationGraphCatalog {

    IdBuilder idBuilder = IdBuilder.getInstance();

    @Getter
    @Serialize
    public LogicalNamespace namespace;
    @Getter
    public BinarySerializer<PolyAllocGraphCatalog> serializer = PolySerializable.buildSerializer( PolyAllocGraphCatalog.class );

    @Getter
    @Serialize
    public ConcurrentHashMap<Long, AllocationGraph> graphs;

    @Getter
    @Serialize
    public ConcurrentHashMap<Long, AllocationPlacement> placements;

    @Getter
    @Serialize
    public ConcurrentHashMap<Long, AllocationPartition> partitions;


    public PolyAllocGraphCatalog( LogicalNamespace namespace ) {
        this( namespace, new HashMap<>(), new HashMap<>(), new HashMap<>() );
    }


    public PolyAllocGraphCatalog(
            @Deserialize("namespace") LogicalNamespace namespace,
            @Deserialize("graphs") Map<Long, AllocationGraph> graphs,
            @Deserialize("placements") Map<Long, AllocationPlacement> placements,
            @Deserialize("partitions") Map<Long, AllocationPartition> partitions ) {
        this.namespace = namespace;
        this.graphs = new ConcurrentHashMap<>( graphs );
        this.placements = new ConcurrentHashMap<>( placements );
        this.partitions = new ConcurrentHashMap<>( partitions );
        listeners.addPropertyChangeListener( Catalog.getInstance().getChangeListener() );
    }


    PropertyChangeSupport listeners = new PropertyChangeSupport( this );


    public void change() {
        listeners.firePropertyChange( "change", null, null );
    }


    @Override
    public AllocationGraph addAllocation( LogicalGraph graph, long placementId, long partitionId, long adapterId ) {
        long id = idBuilder.getNewAllocId();
        AllocationGraph allocation = new AllocationGraph( id, placementId, partitionId, graph.id, namespace.id, adapterId );
        graphs.put( id, allocation );
        change();
        return allocation;
    }


    @Override
    public void deleteAllocation( long id ) {
        graphs.remove( id );
        change();
    }


    @Override
    public AllocationPlacement addPlacement( LogicalGraph graph, long adapterId ) {
        long id = idBuilder.getNewPlacementId();
        AllocationPlacement placement = new AllocationPlacement( id, graph.id, namespace.id, adapterId );
        placements.put( id, placement );
        change();
        return placement;
    }


    @Override
    public void removePlacement( long id ) {
        placements.remove( id );
        change();
    }


    @Override
    public AllocationPartition addPartition( LogicalGraph graph, PartitionType partitionType, String name ) {
        long id = idBuilder.getNewPartitionId();
        AllocationPartition partition = new AllocationPartition( id, namespace.id, graph.id, -1, PlacementType.MANUAL, name, DataPlacementRole.UP_TO_DATE, false, null, partitionType );
        partitions.put( id, partition );
        change();
        return partition;
    }


    @Override
    public void removePartition( long id ) {
        partitions.remove( id );
        change();
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyAllocGraphCatalog.class );
    }


}

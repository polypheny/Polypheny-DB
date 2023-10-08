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

package org.polypheny.db.catalog.catalogs;

import com.google.common.collect.ImmutableList;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalField;
import org.polypheny.db.catalog.entity.physical.PhysicalGraph;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.util.Pair;

@EqualsAndHashCode(callSuper = true)
@Value
public class GraphStoreCatalog extends StoreCatalog {

    @Getter
    public BinarySerializer<GraphStoreCatalog> serializer = PolySerializable.buildSerializer( GraphStoreCatalog.class );

    @Serialize
    public ConcurrentMap<Pair<Long, Long>, PhysicalField> fields; // allocId, columnId


    public GraphStoreCatalog( long adapterId ) {
        this( adapterId, Map.of(), Map.of(), Map.of(), Map.of() );
    }


    public GraphStoreCatalog(
            @Deserialize("adapterId") long adapterId,
            @Deserialize("physicals") Map<Long, PhysicalEntity> physicals,
            @Deserialize("allocations") Map<Long, AllocationEntity> allocations,
            @Deserialize("fields") Map<Pair<Long, Long>, PhysicalColumn> fields,
            @Deserialize("allocToPhysicals") Map<Long, Set<Long>> allocToPhysicals ) {
        super( adapterId, Map.of(), physicals, allocations, allocToPhysicals );
        this.fields = new ConcurrentHashMap<>( fields );
    }


    @Override
    public void renameLogicalField( long id, String newFieldName ) {
        List<PhysicalField> updates = new ArrayList<>();
        for ( PhysicalField field : fields.values() ) {
            if ( field.id == id ) {
                updates.add( field.unwrap( PhysicalColumn.class ).toBuilder().logicalName( newFieldName ).build() );
            }
        }
        updates.forEach( u -> fields.put( Pair.of( u.allocId, u.id ), u ) );
    }


    public PhysicalTable createTable(
            String namespaceName,
            String tableName,
            Map<Long, String> columnNames,
            LogicalTable logical,
            Map<Long, LogicalColumn> logicalColumns,
            AllocationTableWrapper wrapper ) {
        AllocationTable allocation = wrapper.table;
        List<AllocationColumn> columns = wrapper.columns;
        List<PhysicalColumn> pColumns = columns.stream().map( c -> new PhysicalColumn( columnNames.get( c.columnId ), logical.id, allocation.id, allocation.adapterId, c.position, logicalColumns.get( c.columnId ) ) ).collect( Collectors.toList() );
        long physicalId = IdBuilder.getInstance().getNewPhysicalId();
        PhysicalTable table = new PhysicalTable( physicalId, allocation.id, logical.id, tableName, pColumns, logical.namespaceId, namespaceName, allocation.adapterId );
        pColumns.forEach( this::addColumn );
        addPhysical( allocation, table );
        return table;
    }


    public <E extends PhysicalEntity> E fromAllocation( long id, Class<E> clazz ) {
        return getPhysicalsFromAllocs( id ).get( 0 ).unwrap( clazz );
    }


    public PhysicalGraph createGraph(
            String name,
            LogicalGraph logical,
            AllocationGraph allocation ) {
        long physicalId = IdBuilder.getInstance().getNewPhysicalId();
        PhysicalGraph physical = new PhysicalGraph( physicalId, allocation.id, logical.id, name, adapterId );
        addPhysical( allocation, physical );
        return physical;
    }


    public List<? extends PhysicalField> getFields( long allocId ) {
        return fields.values().stream().filter( f -> f.allocId == allocId ).collect( Collectors.toList() );
    }


    public PhysicalColumn createColumn(
            String name,
            long allocId,
            int position,
            LogicalColumn logicalColumn ) {
        PhysicalColumn column = new PhysicalColumn( name, logicalColumn.tableId, allocId, adapterId, position, logicalColumn );
        PhysicalTable table = fromAllocation( allocId, PhysicalTable.class );
        List<PhysicalColumn> columns = new ArrayList<>( table.columns );
        columns.add( position - 1, column );
        addColumn( column );
        addPhysical( getAlloc( table.allocationId ), table.toBuilder().columns( ImmutableList.copyOf( columns ) ).build() );
        return column;
    }


    public void addColumn( PhysicalColumn column ) {
        fields.put( Pair.of( column.allocId, column.id ), column );
    }


    public PhysicalField getField( long fieldId, long allocId ) {
        return fields.get( Pair.of( allocId, fieldId ) );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), GraphStoreCatalog.class );
    }

}

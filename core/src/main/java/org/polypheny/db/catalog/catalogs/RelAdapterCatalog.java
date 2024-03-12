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

package org.polypheny.db.catalog.catalogs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.activej.serializer.annotations.Deserialize;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalField;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.Pair;

@Getter
@EqualsAndHashCode(callSuper = true)
@Value
@Slf4j
@NonFinal
public class RelAdapterCatalog extends AdapterCatalog {


    public RelAdapterCatalog( long adapterId ) {
        this( adapterId, Map.of(), Map.of(), Map.of(), Map.of() );
    }


    public RelAdapterCatalog(
            @Deserialize("adapterId") long adapterId,
            @Deserialize("physicals") Map<Long, PhysicalEntity> physicals,
            @Deserialize("allocations") Map<Long, AllocationEntity> allocations,
            @Deserialize("allocToPhysicals") Map<Long, SortedSet<Long>> allocToPhysicals,
            @Deserialize("fields") Map<Pair<Long, Long>, PhysicalField> fields ) {
        super( adapterId, Map.of(), physicals, allocations, allocToPhysicals, fields );
    }


    @Override
    public void renameLogicalColumn( long id, String newFieldName ) {
        List<PhysicalColumn> updates = new ArrayList<>();
        for ( PhysicalField field : fields.values() ) {
            if ( field.id == id ) {
                updates.add( field.unwrap( PhysicalColumn.class ).orElseThrow().toBuilder().logicalName( newFieldName ).build() );
            }
        }
        for ( PhysicalColumn u : updates ) {
            PhysicalTable table = fromAllocation( u.allocId );
            List<PhysicalColumn> newColumns = table.columns.stream().filter( c -> c.id != id ).collect( Collectors.toList() );
            newColumns.add( u );
            physicals.put( table.id, table.toBuilder().columns( ImmutableList.copyOf( newColumns ) ).build() );
            fields.put( Pair.of( u.allocId, u.id ), u );
        }
    }


    public void addColumn( PhysicalColumn column ) {
        fields.put( Pair.of( column.allocId, column.id ), column );
    }


    public PhysicalTable getTable( long id ) {
        return getPhysical( id ).unwrap( PhysicalTable.class ).orElseThrow();
    }


    public PhysicalColumn getColumn( long id, long allocId ) {
        return fields.get( Pair.of( allocId, id ) ).unwrap( PhysicalColumn.class ).orElseThrow();
    }


    public PhysicalTable createTable( String namespaceName, String tableName, Map<Long, String> columnNames, LogicalTable logical, Map<Long, LogicalColumn> lColumns, List<Long> pkIds, AllocationTableWrapper wrapper ) {
        AllocationTable allocation = wrapper.table;
        List<AllocationColumn> columns = wrapper.columns;
        List<PhysicalColumn> pColumns = Streams.mapWithIndex( columns.stream(), ( c, i ) -> new PhysicalColumn( columnNames.get( c.columnId ), logical.id, allocation.id, allocation.adapterId, (int) i, lColumns.get( c.columnId ) ) ).collect( Collectors.toList() );
        PhysicalTable table = new PhysicalTable( IdBuilder.getInstance().getNewPhysicalId(), allocation.id, allocation.logicalId, tableName, pColumns, logical.namespaceId, namespaceName, pkIds, allocation.adapterId );
        pColumns.forEach( this::addColumn );
        addPhysical( allocation, table );
        return table;
    }


    public PhysicalColumn addColumn( String name, long allocId, int position, LogicalColumn lColumn ) {
        PhysicalColumn column = new PhysicalColumn( name, lColumn.tableId, allocId, adapterId, position, lColumn );
        PhysicalTable table = fromAllocation( allocId );
        List<PhysicalColumn> columns = new ArrayList<>( table.columns );
        columns.add( position, column );
        addColumn( column );
        addPhysical( getAlloc( table.allocationId ), table.toBuilder().columns( ImmutableList.copyOf( columns ) ).build() );
        return column;
    }


    public PhysicalColumn updateColumnType( long allocId, LogicalColumn newCol ) {
        PhysicalColumn old = getColumn( newCol.id, allocId );
        PhysicalColumn column = new PhysicalColumn( old.name, newCol.tableId, allocId, old.adapterId, old.position, newCol );
        PhysicalTable table = fromAllocation( allocId );
        addColumn( column );
        List<PhysicalColumn> pColumn = new ArrayList<>( table.columns ).stream().map( c -> c.id == column.id ? column : c ).toList();
        addPhysical( getAlloc( table.allocationId ), table.toBuilder().columns( ImmutableList.copyOf( pColumn ) ).build() );

        return column;
    }


    public PhysicalTable fromAllocation( long id ) {
        List<PhysicalEntity> allocs = getPhysicalsFromAllocs( id );
        if ( allocs == null || allocs.isEmpty() ) {
            throw new GenericRuntimeException( "No physical table found for allocation with id %s", id );
        }
        return allocs.get( 0 ).unwrap( PhysicalTable.class ).orElseThrow();
    }


    public void dropColumn( long allocId, long columnId ) {
        PhysicalColumn column = fields.get( Pair.of( allocId, columnId ) ).unwrap( PhysicalColumn.class ).orElseThrow();
        PhysicalTable table = fromAllocation( allocId );
        List<PhysicalColumn> pColumns = new ArrayList<>( table.columns );
        pColumns.remove( column );
        addPhysical( getAlloc( allocId ), table.toBuilder().columns( ImmutableList.copyOf( pColumns ) ).build() );
        fields.remove( Pair.of( allocId, columnId ) );
    }


    public List<PhysicalColumn> getColumns( long allocId ) {
        return fields.values().stream().map( p -> p.unwrap( PhysicalColumn.class ) ).filter( Optional::isPresent ).map( Optional::get ).filter( c -> c.allocId == allocId ).collect( Collectors.toList() );
    }



}

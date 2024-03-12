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

import io.activej.serializer.annotations.Deserialize;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalField;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.util.Pair;

@Getter
@EqualsAndHashCode(callSuper = true)
@Value
public class DocAdapterCatalog extends AdapterCatalog {


    public DocAdapterCatalog( long adapterId ) {
        this( adapterId, Map.of(), Map.of(), Map.of(), Map.of() );
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
            fields.put( Pair.of( u.allocId, u.id ), u );
        }
    }


    public DocAdapterCatalog(
            @Deserialize("adapterId") long adapterId,
            @Deserialize("physicals") Map<Long, PhysicalEntity> physicals,
            @Deserialize("allocations") Map<Long, AllocationEntity> allocations,
            @Deserialize("fields") Map<Pair<Long, Long>, PhysicalField> fields,
            @Deserialize("allocToPhysicals") Map<Long, SortedSet<Long>> allocToPhysicals ) {
        super( adapterId, Map.of(), physicals, allocations, allocToPhysicals, fields );
    }


    public <T extends PhysicalEntity> T fromAllocation( long id, Class<T> clazz ) {
        return getPhysicalsFromAllocs( id ).get( 0 ).unwrap( clazz ).orElseThrow();
    }


    public PhysicalColumn createColumn(
            String name,
            long allocId,
            int position,
            LogicalColumn logicalColumn ) {
        PhysicalColumn column = new PhysicalColumn( name, logicalColumn.tableId, allocId, adapterId, position, logicalColumn );
        addColumn( column );
        return column;
    }


    public void addColumn( PhysicalColumn column ) {
        fields.put( Pair.of( column.allocId, column.id ), column );
    }


    public PhysicalColumn getColumn( long columnId, long allocId ) {
        return fields.get( Pair.of( allocId, columnId ) ).unwrap( PhysicalColumn.class ).orElseThrow();
    }


    public List<? extends PhysicalField> getFields( long allocId ) {
        return fields.values().stream().filter( f -> f.allocId == allocId ).toList();
    }


    public void dropColumn( long allocId, long columnId ) {
        fields.remove( Pair.of( allocId, columnId ) );
    }


    public PhysicalColumn updateColumnType( long allocId, LogicalColumn newCol ) {
        PhysicalColumn old = getColumn( newCol.id, allocId );
        PhysicalColumn column = new PhysicalColumn( old.name, newCol.tableId, allocId, old.adapterId, old.position, newCol );
        addColumn( column );
        return column;
    }

    public PhysicalTable createTable(
            String namespaceName,
            String tableName,
            Map<Long, String> columnNames,
            LogicalTable logical,
            Map<Long, LogicalColumn> logicalColumns,
            List<Long> pkIds,
            AllocationTableWrapper wrapper ) {
        AllocationTable allocation = wrapper.table;
        List<AllocationColumn> columns = wrapper.columns;
        List<PhysicalColumn> pColumns = columns.stream().map( c -> new PhysicalColumn( columnNames.get( c.columnId ), logical.id, allocation.id, allocation.adapterId, c.position, logicalColumns.get( c.columnId ) ) ).collect( Collectors.toList() );
        long physicalId = IdBuilder.getInstance().getNewPhysicalId();
        PhysicalTable table = new PhysicalTable( physicalId, allocation.id, allocation.logicalId, tableName, pColumns, logical.namespaceId, namespaceName, pkIds, allocation.adapterId );
        pColumns.forEach( this::addColumn );
        addPhysical( allocation, table );
        return table;
    }


    public PhysicalCollection createCollection( String namespaceName, String name, LogicalCollection logical, AllocationCollection allocation ) {
        long physicalId = IdBuilder.getInstance().getNewPhysicalId();
        PhysicalCollection collection = new PhysicalCollection( physicalId, allocation.id, logical.id, logical.namespaceId, name, namespaceName, adapterId );
        addPhysical( allocation, collection );
        return collection;
    }


}

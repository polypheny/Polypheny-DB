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
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.entity.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.schema.Namespace;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;

@EqualsAndHashCode(callSuper = true)
@Value
@Slf4j
public class RelStoreCatalog extends StoreCatalog {

    public RelStoreCatalog( long adapterId ) {
        this( adapterId, new HashMap<>(), new HashMap<>(), new HashMap<>() );
    }


    @Override
    public AlgNode getRelScan( long allocId, Statement statement ) {
        return AlgBuilder.create( statement ).scan( tables.get( allocId ) ).build();
    }


    @Override
    public AlgNode getGraphScan( long allocId, Statement statement ) {
        log.warn( "todo" );
        return null;
    }


    @Override
    public AlgNode getDocumentScan( long allocId, Statement statement ) {
        log.warn( "todo" );
        return null;
    }


    public RelStoreCatalog(
            @Deserialize("adapterId") long adapterId,
            @Deserialize("namespaces") Map<Long, Namespace> namespaces,
            @Deserialize("tables") Map<Long, PhysicalTable> tables,
            @Deserialize("columns") Map<Long, PhysicalColumn> columns ) {
        super( adapterId );
        this.namespaces = new ConcurrentHashMap<>( namespaces );
        this.tables = new ConcurrentHashMap<>( tables );
        this.columns = new ConcurrentHashMap<>( columns );
    }


    @Serialize
    ConcurrentMap<Long, Namespace> namespaces;
    @Serialize
    ConcurrentMap<Long, PhysicalTable> tables;
    @Serialize
    ConcurrentMap<Long, PhysicalColumn> columns;


    public void addTable( PhysicalTable table ) {
        tables.put( table.id, table );
    }


    public void addColumns( List<PhysicalColumn> columns ) {
        this.columns.putAll( columns.stream().collect( Collectors.toMap( c -> c.id, c -> c ) ) );
    }


    public PhysicalTable getTable( long id ) {
        return tables.get( id );
    }


    public PhysicalColumn getColumn( long id ) {
        return columns.get( id );
    }


    public Namespace getNamespace( long id ) {
        return namespaces.get( id );
    }


    public void addNamespace( long id, Namespace namespace ) {
        namespaces.put( id, namespace );
    }


    public PhysicalTable createTable( String namespaceName, String tableName, Map<Long, String> columnNames, LogicalTable logical, Map<Long, LogicalColumn> lColumns, AllocationTable allocation, List<AllocationColumn> columns ) {
        List<PhysicalColumn> pColumns = columns.stream().map( c -> new PhysicalColumn( columnNames.get( c.columnId ), logical.id, allocation.adapterId, c.position, lColumns.get( c.columnId ) ) ).collect( Collectors.toList() );
        PhysicalTable table = new PhysicalTable( allocation.id, allocation.id, tableName, pColumns, logical.namespaceId, namespaceName, allocation.adapterId );
        addTable( table );
        return table;
    }


    public PhysicalColumn addColumn( String name, long tableId, long adapterId, int position, LogicalColumn lColumn ) {
        PhysicalColumn column = new PhysicalColumn( name, tableId, adapterId, position, lColumn );
        PhysicalTable table = getTable( tableId );
        List<PhysicalColumn> pColumn = new ArrayList<>( table.columns );
        pColumn.add( column );
        tables.put( tableId, table.toBuilder().columns( ImmutableList.copyOf( pColumn ) ).build() );
        return column;
    }


    public PhysicalColumn updateColumnType( long tableId, LogicalColumn newCol ) {
        PhysicalColumn old = getColumn( newCol.id );
        PhysicalColumn column = new PhysicalColumn( old.name, tableId, adapterId, old.position, newCol );
        PhysicalTable table = getTable( tableId );
        List<PhysicalColumn> pColumn = new ArrayList<>( table.columns );
        pColumn.remove( old );
        pColumn.add( column );
        tables.put( tableId, table.toBuilder().columns( ImmutableList.copyOf( pColumn ) ).build() );
        return column;
    }

}

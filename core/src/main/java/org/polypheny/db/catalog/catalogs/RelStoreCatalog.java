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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.catalog.entity.AllocationColumn;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.schema.Namespace;

@EqualsAndHashCode(callSuper = true)
@Value
public class RelStoreCatalog extends StoreCatalog {


    public RelStoreCatalog( long adapterId ) {
        super( adapterId );
    }


    Map<Long, Namespace> namespaces = new HashMap<>();
    Map<Long, PhysicalTable> tables = new HashMap<>();
    Map<Long, PhysicalColumn> columns = new HashMap<>();


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


    public PhysicalTable createTable( String namespaceName, String tableName, List<String> columnNames, LogicalTable logical, List<LogicalColumn> lColumns, AllocationTable allocation, List<AllocationColumn> columns ) {

    }

}

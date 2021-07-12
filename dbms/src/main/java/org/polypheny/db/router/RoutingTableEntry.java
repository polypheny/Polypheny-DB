/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.router;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.val;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;

@Builder
@Getter
public class RoutingTableEntry implements Comparable<RoutingTableEntry>{
    String id;
    Set<Integer> adapterIds;
    HashMap<Integer, Integer> tableToAdapterIdMapping; // tableId -> adapterId


    protected Map<Integer, List<CatalogColumnPlacement>> getColumnPlacements( HashMap<CatalogTable, Set<CatalogColumn>> usedCatalogColumnsPerTable ){
        HashMap<Integer, List<CatalogColumnPlacement>> result = new HashMap<>();

        for(val entry : usedCatalogColumnsPerTable.entrySet()){
            val table = entry.getKey();
            val columns = entry.getValue();
            val adapterId = tableToAdapterIdMapping.get( (int) table.id );

            val placements = columns.stream()
                    .map( col -> Catalog.getInstance().getColumnPlacement( adapterId, col.id ) )
                    .collect( Collectors.toList());
            result.put(  (int) table.id , placements );
        }

        return result;
    }


    @Override
    public int compareTo( RoutingTableEntry other ) {
        /*if(id.equals( other.id ) &&
                adapterIds.containsAll( other.adapterIds ) && tableToAdapterIdMapping.equals( other.tableToAdapterIdMapping )){*/
        if(this.hashCode() == other.hashCode()){
            return 0;
        }
        return -1;
    }


    @Override
    public int hashCode() {
        return this.id.hashCode() + this.tableToAdapterIdMapping.hashCode() + this.adapterIds.hashCode();
    }


    @Override
    public boolean equals( Object obj ) {
        val other = (RoutingTableEntry)obj;

        if(other == null)
            return false;
        else
            return this.hashCode() == other.hashCode();
    }

}
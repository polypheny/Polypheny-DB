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

package org.polypheny.db.catalog.entity;


import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;


@EqualsAndHashCode
public class CatalogKey implements CatalogEntity, Comparable<CatalogKey> {

    public final long id;
    public final long tableId;
    public final long schemaId;
    public final long databaseId;
    public final ImmutableList<Long> columnIds;
    public final EnforcementTime enforcementTime;


    public CatalogKey(
            final long id,
            final long tableId,
            final long schemaId,
            final long databaseId,
            final List<Long> columnIds, EnforcementTime enforcementTime ) {
        this.id = id;
        this.tableId = tableId;
        this.schemaId = schemaId;
        this.databaseId = databaseId;
        this.columnIds = ImmutableList.copyOf( columnIds );
        this.enforcementTime = enforcementTime;
    }


    public CatalogKey(
            final long id,
            final long tableId,
            final long schemaId,
            final long databaseId ) {
        this( id, tableId, schemaId, databaseId, null, EnforcementTime.ON_COMMIT );
    }


    @SneakyThrows
    public String getDatabaseName() {
        return Catalog.getInstance().getDatabase( databaseId ).name;
    }


    @SneakyThrows
    public String getSchemaName() {
        return Catalog.getInstance().getSchema( schemaId ).name;
    }


    @SneakyThrows
    public String getTableName() {
        return Catalog.getInstance().getTable( tableId ).name;
    }


    @SneakyThrows
    public List<String> getColumnNames() {
        Catalog catalog = Catalog.getInstance();
        List<String> columnNames = new LinkedList<>();
        for ( long columnId : columnIds ) {
            columnNames.add( catalog.getColumn( columnId ).name );
        }
        return columnNames;
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{ id, tableId, getTableName(), schemaId, getSchemaName(), databaseId, getDatabaseName(), null, null };
    }


    @Override
    public int compareTo( CatalogKey o ) {
        if ( o != null ) {
            return (int) (this.id - o.id);
        }
        return -1;
    }


    public enum EnforcementTime {
        ON_COMMIT,
        ON_QUERY
    }

}

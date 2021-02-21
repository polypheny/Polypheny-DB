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
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.rel.RelNode;


@EqualsAndHashCode
public class CatalogTable implements CatalogEntity, Comparable<CatalogTable> {

    private static final long serialVersionUID = 5426944084650275437L;

    public final long id;
    public final String name;
    public final ImmutableList<Long> columnIds;
    public final long schemaId;
    public final long databaseId;
    public final int ownerId;
    public final String ownerName;
    public final TableType tableType;
    public final RelNode definition;
    public final Long primaryKey;
    public final ImmutableMap<Integer, ImmutableList<Long>> placementsByAdapter;
    public final boolean modifiable;


    public CatalogTable(
            final long id,
            @NonNull final String name,
            final ImmutableList<Long> columnIds,
            final long schemaId,
            final long databaseId,
            final int ownerId,
            @NonNull final String ownerName,
            @NonNull final TableType type,
            final RelNode definition,
            final Long primaryKey,
            @NonNull final ImmutableMap<Integer, ImmutableList<Long>> placementsByAdapter, boolean modifiable ) {
        this.id = id;
        this.name = name;
        this.columnIds = columnIds;
        this.schemaId = schemaId;
        this.databaseId = databaseId;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.tableType = type;
        this.definition = definition;
        this.primaryKey = primaryKey;
        this.placementsByAdapter = placementsByAdapter;
        this.modifiable = modifiable;

        if ( type == TableType.TABLE && !modifiable ) {
            throw new RuntimeException( "Tables of table type TABLE must be modifiable!" );
        }
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
    public List<String> getColumnNames() {
        Catalog catalog = Catalog.getInstance();
        List<String> columnNames = new LinkedList<>();
        for ( long columnId : columnIds ) {
            columnNames.add( catalog.getColumn( columnId ).name );
        }
        return columnNames;
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{
                getDatabaseName(),
                getSchemaName(),
                name,
                tableType.name(),
                "",
                null,
                null,
                null,
                null,
                null,
                ownerName,
                definition };
    }


    @Override
    public int compareTo( CatalogTable o ) {
        if ( o != null ) {
            int comp = (int) (this.databaseId - o.databaseId);
            if ( comp == 0 ) {
                comp = (int) (this.schemaId - o.schemaId);
                if ( comp == 0 ) {
                    return (int) (this.id - o.id);
                } else {
                    return comp;
                }

            } else {
                return comp;
            }
        }
        return -1;
    }


    public CatalogView generateView() {
        return new CatalogView( id, name, columnIds, schemaId, databaseId, ownerId, ownerName, tableType, definition, primaryKey, placementsByAdapter, modifiable);
    }


    @RequiredArgsConstructor
    public static class PrimitiveCatalogTable {

        public final String tableCat;
        public final String tableSchem;
        public final String tableName;
        public final String tableType;
        public final String remarks;
        public final String typeCat;
        public final String typeSchem;
        public final String typeName;
        public final String selfReferencingColName;
        public final String refGeneration;
        public final String owner;
        public final String definition;
    }

}

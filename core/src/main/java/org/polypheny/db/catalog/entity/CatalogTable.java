/*
 * Copyright 2019-2020 The Polypheny Project
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;


/**
 *
 */
@EqualsAndHashCode
public final class CatalogTable implements CatalogEntity, Comparable<CatalogTable> {

    private static final long serialVersionUID = 5426944084650275437L;

    public final long id;
    public final String name;
    public final ImmutableList<Long> columnIds;
    public final ImmutableList<String> columnNames;
    public final long schemaId;
    public final long databaseId;
    public final int ownerId;
    public final String ownerName;
    public final TableType tableType;
    public final String definition;
    public final Long primaryKey;
    public final ImmutableMap<Integer, ImmutableList<Long>> placementsByStore;


    public CatalogTable(
            final long id,
            @NonNull final String name,
            final ImmutableList<Long> columnIds,
            final ImmutableList<String> columnNames,
            final long schemaId,
            final long databaseId,
            final int ownerId,
            @NonNull final String ownerName,
            @NonNull final TableType type,
            final String definition,
            final Long primaryKey,
            @NonNull final ImmutableMap<Integer, ImmutableList<Long>> placementsByStore ) {
        this.id = id;
        this.name = name;
        this.columnIds = columnIds;
        this.columnNames = columnNames;
        this.schemaId = schemaId;
        this.databaseId = databaseId;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.tableType = type;
        this.definition = definition;
        this.primaryKey = primaryKey;
        this.placementsByStore = placementsByStore;
    }


    @SneakyThrows
    public String getDatabaseName() {
        return Catalog.getInstance().getDatabase( databaseId ).name;
    }


    @SneakyThrows
    public String getSchemaName() {
        return Catalog.getInstance().getSchema( schemaId ).name;
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


    public static CatalogTable rename( CatalogTable table, String name ) {
        return new CatalogTable( table.id, name, table.columnIds, table.columnNames, table.schemaId, table.databaseId, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, table.placementsByStore );
    }


    public static CatalogTable replaceOwner( CatalogTable table, int ownerId, String ownerName ) {
        return new CatalogTable( table.id, table.name, table.columnIds, table.columnNames, table.schemaId, table.databaseId, ownerId, ownerName, table.tableType, table.definition, table.primaryKey, table.placementsByStore );
    }


    public static CatalogTable replacePrimary( CatalogTable table, Long keyId ) {
        return new CatalogTable( table.id, table.name, table.columnIds, table.columnNames, table.schemaId, table.databaseId, table.ownerId, table.ownerName, table.tableType, table.definition, keyId, table.placementsByStore );
    }


    public static CatalogTable addColumn( CatalogTable table, long columnId, String columnName ) {
        List<Long> columnIds = new ArrayList<>( table.columnIds );
        columnIds.add( columnId );
        List<String> columnNames = new ArrayList<>( table.columnNames );
        columnNames.add( columnName );
        return new CatalogTable( table.id, table.name, ImmutableList.copyOf( columnIds ), ImmutableList.copyOf( columnNames ), table.schemaId, table.databaseId, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, table.placementsByStore );
    }


    public static CatalogTable removeColumn( CatalogTable table, long columnId, String columnName ) {
        List<Long> columnIds = new ArrayList<>( table.columnIds );
        columnIds.remove( columnId );
        List<String> columnNames = new ArrayList<>( table.columnNames );
        columnNames.remove( columnName );
        return new CatalogTable( table.id, table.name, ImmutableList.copyOf( columnIds ), ImmutableList.copyOf( columnNames ), table.schemaId, table.databaseId, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, table.placementsByStore );
    }


    public static CatalogTable replaceColumnName( CatalogTable table, long columnId, String columnName ) {
        int index = table.columnIds.indexOf( columnId );
        List<String> columnNames = new ArrayList<>( table.columnNames );
        columnNames.set( index, columnName );

        return new CatalogTable( table.id, table.name, table.columnIds, ImmutableList.copyOf( columnNames ), table.schemaId, table.databaseId, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, table.placementsByStore );
    }


    public static CatalogTable addColumnPlacement( CatalogTable table, long columnId, int storeId ) {
        Map<Integer, ImmutableList<Long>> placementsByStore = new HashMap<>( table.placementsByStore );
        if ( placementsByStore.containsKey( storeId ) ) {
            List<Long> placements = new ArrayList<>( placementsByStore.get( storeId ) );
            placements.add( columnId );
            placementsByStore.replace( storeId, ImmutableList.copyOf( placements ) );
        } else {
            placementsByStore.put( storeId, ImmutableList.of( columnId ) );
        }
        return new CatalogTable( table.id, table.name, table.columnIds, table.columnNames, table.schemaId, table.databaseId, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, ImmutableMap.copyOf( placementsByStore ) );
    }


    public static CatalogTable removeColumnPlacement( CatalogTable table, long columnId, int storeId ) {
        Map<Integer, ImmutableList<Long>> placementsByStore = new HashMap<>( table.placementsByStore );
        List<Long> placements = new ArrayList<>( placementsByStore.get( storeId ) );
        placements.remove( columnId );
        if ( placements.size() != 0 ) {
            placementsByStore.put( storeId, ImmutableList.copyOf( placements ) );
        } else {
            placementsByStore.remove( storeId );
        }

        return new CatalogTable( table.id, table.name, table.columnIds, table.columnNames, table.schemaId, table.databaseId, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, ImmutableMap.copyOf( placementsByStore ) );
    }

}

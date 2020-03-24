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


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;


/**
 *
 */
@EqualsAndHashCode
public final class CatalogTable implements CatalogEntity {

    private static final long serialVersionUID = 5426944084650275437L;

    public final long id;
    public final String name;
    public final ImmutableList<Long> columnIds;
    public final ImmutableList<String> columnNames;
    public final long schemaId;
    public final String schemaName;
    public final long databaseId;
    public final String databaseName;
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
            @NonNull final String schemaName,
            final long databaseId,
            @NonNull final String databaseName,
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
        this.schemaName = schemaName;
        this.databaseId = databaseId;
        this.databaseName = databaseName;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.tableType = type;
        this.definition = definition;
        this.primaryKey = primaryKey;
        this.placementsByStore = placementsByStore;
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{
                databaseName,
                schemaName,
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
        return new CatalogTable( table.id, name, table.columnIds, table.columnNames, table.schemaId, table.schemaName, table.databaseId, table.databaseName, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, table.placementsByStore );
    }


    public static CatalogTable replaceOwner( CatalogTable table, int ownerId ) {
        return new CatalogTable( table.id, table.name, table.columnIds, table.columnNames, table.schemaId, table.schemaName, table.databaseId, table.databaseName, ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, table.placementsByStore );
    }


    public static CatalogTable replacePrimary( CatalogTable table, Long keyId ) {
        return new CatalogTable( table.id, table.name, table.columnIds, table.columnNames, table.schemaId, table.schemaName, table.databaseId, table.databaseName, table.ownerId, table.ownerName, table.tableType, table.definition, keyId, table.placementsByStore );
    }


    public static CatalogTable addColumn( CatalogTable table, long columnId, String columnName ) {
        List<Long> columnIds = new ArrayList<>( table.columnIds );
        columnIds.add( columnId );
        List<String> columnNames = new ArrayList<>( table.columnNames );
        columnNames.add( columnName );
        return new CatalogTable( table.id, table.name, ImmutableList.copyOf( columnIds ), ImmutableList.copyOf( columnNames ), table.schemaId, table.schemaName, table.databaseId, table.databaseName, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, table.placementsByStore );
    }


    public static CatalogTable removeColumn( CatalogTable table, long columnId, String columnName ) {
        List<Long> columnIds = new ArrayList<>( table.columnIds );
        columnIds.remove( columnId );
        List<String> columnNames = new ArrayList<>( table.columnNames );
        columnNames.remove( columnName );
        return new CatalogTable( table.id, table.name, ImmutableList.copyOf( columnIds ), ImmutableList.copyOf( columnNames ), table.schemaId, table.schemaName, table.databaseId, table.databaseName, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, table.placementsByStore );
    }


    public static CatalogTable replaceColumnName( CatalogTable table, long columnId, String columnName ) {
        int index = table.columnIds.indexOf( columnId );
        List<String> columnNames = new ArrayList<>( table.columnNames );
        columnNames.set( index, columnName );

        return new CatalogTable( table.id, table.name, table.columnIds, ImmutableList.copyOf( columnNames ), table.schemaId, table.schemaName, table.databaseId, table.databaseName, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, table.placementsByStore );
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
        return new CatalogTable( table.id, table.name, table.columnIds, table.columnNames, table.schemaId, table.schemaName, table.databaseId, table.databaseName, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, ImmutableMap.copyOf( placementsByStore ) );
    }


    public static CatalogTable removeColumnPlacement( CatalogTable table, long columnId, int storeId ) {
        Map<Integer, ImmutableList<Long>> placementsByStore = new HashMap<>( table.placementsByStore );
        List<Long> placements = new ArrayList<>( placementsByStore.get( storeId ) );
        placements.remove( columnId );
        if( placements.size() != 0){
            placementsByStore.put( storeId, ImmutableList.copyOf( placements ) );
        }else {
            placementsByStore.remove( storeId );
        }


        return new CatalogTable( table.id, table.name, table.columnIds, table.columnNames, table.schemaId, table.schemaName, table.databaseId, table.databaseName, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, ImmutableMap.copyOf( placementsByStore ) );
    }

}

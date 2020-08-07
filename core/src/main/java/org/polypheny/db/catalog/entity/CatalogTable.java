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
import java.util.LinkedList;
import java.util.List;

import lombok.*;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.Catalog.PartitionType;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionException;


/**
 *
 */
@EqualsAndHashCode
public final class CatalogTable implements CatalogEntity, Comparable<CatalogTable> {

    private static final long serialVersionUID = 5426944084650275437L;

    public final long id;
    public final String name;
    public final ImmutableList<Long> columnIds;
    public final long schemaId;
    public final long databaseId;
    public final int ownerId;
    public final String ownerName;
    public final TableType tableType;
    public final String definition;
    public final Long primaryKey;
    public final ImmutableMap<Integer, ImmutableList<Long>> placementsByStore;

    //HENNLO
    @Getter
    public boolean isPartitioned = false;
    @Getter
    public Catalog.PartitionType partitionType = PartitionType.NONE;
    public ImmutableList<Long> partitionIds;
    public long partitionColumnId;
    public long numPartitions;

    public CatalogTable(
            final long id,
            @NonNull final String name,
            final ImmutableList<Long> columnIds,
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
        this.schemaId = schemaId;
        this.databaseId = databaseId;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.tableType = type;
        this.definition = definition;
        this.primaryKey = primaryKey;
        this.placementsByStore = placementsByStore;

    }

    // Hennlo
    // numPartitons can be empty and calculated based on the partition key
    // Only used when explicitly working with partitons to not alter existing call stack and logic
    public CatalogTable(
            final long id,
            @NonNull final String name,
            final ImmutableList<Long> columnIds,
            final long schemaId,
            final long databaseId,
            final int ownerId,
            @NonNull final String ownerName,
            @NonNull final TableType type,
            final String definition,
            final Long primaryKey,
            @NonNull final ImmutableMap<Integer, ImmutableList<Long>> placementsByStore,
            final long numPartitions,
            final PartitionType partitionType,
            final ImmutableList<Long> partitionIds,
            final long partitionColumnId) {
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
        this.placementsByStore = placementsByStore;

        //HENNLO added
        this.partitionType = partitionType;
        this.partitionIds = partitionIds;
        this.partitionColumnId = partitionColumnId;
        isPartitioned = true;

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

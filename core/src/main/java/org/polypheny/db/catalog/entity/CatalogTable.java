/*
 * Copyright 2019-2022 The Polypheny Project
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
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.partition.properties.PartitionProperty;


@EqualsAndHashCode
public class CatalogTable implements CatalogObject, Comparable<CatalogTable> {

    private static final long serialVersionUID = 4653390333258552102L;

    public final long id;
    public final String name;
    public final ImmutableList<Long> fieldIds;
    public final long namespaceId;
    public final long databaseId;
    public final int ownerId;
    public final EntityType entityType;
    public final Long primaryKey;
    public final boolean modifiable;

    public final PartitionProperty partitionProperty;

    public final ImmutableList<Integer> dataPlacements;

    @Getter
    public final ImmutableList<Long> connectedViews;


    public CatalogTable(
            final long id,
            @NonNull final String name,
            final ImmutableList<Long> fieldIds,
            final long namespaceId,
            final long databaseId,
            final int ownerId,
            @NonNull final Catalog.EntityType type,
            final Long primaryKey,
            @NonNull final ImmutableList<Integer> dataPlacements,
            boolean modifiable,
            PartitionProperty partitionProperty ) {
        this.id = id;
        this.name = name;
        this.fieldIds = fieldIds;
        this.namespaceId = namespaceId;
        this.databaseId = databaseId;
        this.ownerId = ownerId;
        this.entityType = type;
        this.primaryKey = primaryKey;
        this.modifiable = modifiable;

        this.partitionProperty = partitionProperty;
        this.connectedViews = ImmutableList.of();

        this.dataPlacements = ImmutableList.copyOf( dataPlacements );

        if ( type == EntityType.ENTITY && !modifiable ) {
            throw new RuntimeException( "Tables of table type TABLE must be modifiable!" );
        }
    }


    public CatalogTable(
            final long id,
            @NonNull final String name,
            final ImmutableList<Long> fieldIds,
            final long namespaceId,
            final long databaseId,
            final int ownerId,
            @NonNull final Catalog.EntityType type,
            final Long primaryKey,
            @NonNull final ImmutableList<Integer> dataPlacements,
            boolean modifiable,
            PartitionProperty partitionProperty,
            ImmutableList<Long> connectedViews ) {
        this.id = id;
        this.name = name;
        this.fieldIds = fieldIds;
        this.namespaceId = namespaceId;
        this.databaseId = databaseId;
        this.ownerId = ownerId;
        this.entityType = type;
        this.primaryKey = primaryKey;
        this.modifiable = modifiable;

        this.partitionProperty = partitionProperty;

        this.connectedViews = connectedViews;

        this.dataPlacements = ImmutableList.copyOf( dataPlacements );

        if ( type == EntityType.ENTITY && !modifiable ) {
            throw new RuntimeException( "Tables of table type TABLE must be modifiable!" );
        }
    }


    @SneakyThrows
    public String getDatabaseName() {
        return Catalog.getInstance().getDatabase( databaseId ).name;
    }


    @SneakyThrows
    public String getNamespaceName() {
        return Catalog.getInstance().getSchema( namespaceId ).name;
    }


    @SneakyThrows
    public NamespaceType getNamespaceType() {
        return Catalog.getInstance().getSchema( namespaceId ).namespaceType;
    }


    @SneakyThrows
    public String getOwnerName() {
        return Catalog.getInstance().getUser( ownerId ).name;
    }


    @SneakyThrows
    public List<String> getColumnNames() {
        Catalog catalog = Catalog.getInstance();
        List<String> fieldNames = new LinkedList<>();
        for ( long fieldId : fieldIds ) {
            fieldNames.add( catalog.getColumn( fieldId ).name );
        }
        return fieldNames;
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{
                getDatabaseName(),
                getNamespaceName(),
                name,
                entityType.name(),
                "",
                null,
                null,
                null,
                null,
                null,
                getOwnerName()

        };
    }


    @Override
    public int compareTo( CatalogTable o ) {
        if ( o != null ) {
            int comp = (int) (this.databaseId - o.databaseId);
            if ( comp == 0 ) {
                comp = (int) (this.namespaceId - o.namespaceId);
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


    static String getEnumNameOrNull( Enum<?> theEnum ) {
        if ( theEnum == null ) {
            return null;
        } else {
            return theEnum.name();
        }
    }


    public CatalogTable getRenamed( String newName ) {
        return new CatalogTable(
                id,
                newName,
                fieldIds,
                namespaceId,
                databaseId,
                ownerId,
                entityType,
                primaryKey,
                dataPlacements,
                modifiable,
                partitionProperty,
                connectedViews );
    }


    public CatalogTable getConnectedViews( ImmutableList<Long> newConnectedViews ) {
        return new CatalogTable(
                id,
                name,
                fieldIds,
                namespaceId,
                databaseId,
                ownerId,
                entityType,
                primaryKey,
                dataPlacements,
                modifiable,
                partitionProperty,
                newConnectedViews );
    }


    public CatalogTable getTableWithColumns( ImmutableList<Long> newColumnIds ) {
        return new CatalogTable(
                id,
                name,
                newColumnIds,
                namespaceId,
                databaseId,
                ownerId,
                entityType,
                primaryKey,
                dataPlacements,
                modifiable,
                partitionProperty,
                connectedViews );
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

    }

}

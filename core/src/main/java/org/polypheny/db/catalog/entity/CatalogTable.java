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


import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.polypheny.db.catalog.Catalog.TableType;


/**
 *
 */
@EqualsAndHashCode
public final class CatalogTable implements CatalogEntity {

    private static final long serialVersionUID = 5426944084650275437L;

    public final long id;
    public final String name;
    public final long schemaId;
    public final String schemaName;
    public final long databaseId;
    public final String databaseName;
    public final int ownerId;
    public final String ownerName;
    public final TableType tableType;
    public final String definition;
    public final Long primaryKey;


    public CatalogTable(
            final long id,
            @NonNull final String name,
            final long schemaId,
            @NonNull final String schemaName,
            final long databaseId,
            @NonNull final String databaseName,
            final int ownerId,
            @NonNull final String ownerName,
            @NonNull final TableType type,
            final String definition,
            final Long primaryKey ) {
        this.id = id;
        this.name = name;
        this.schemaId = schemaId;
        this.schemaName = schemaName;
        this.databaseId = databaseId;
        this.databaseName = databaseName;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.tableType = type;
        this.definition = definition;
        this.primaryKey = primaryKey;
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
        return new CatalogTable( table.id, name, table.schemaId, table.schemaName, table.databaseId, table.databaseName, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey );
    }


    public static CatalogTable replaceOwner( CatalogTable table, int ownerId ) {
        return new CatalogTable( table.id, table.name, table.schemaId, table.schemaName, table.databaseId, table.databaseName, ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey );
    }

    public static CatalogTable replacePrimary( CatalogTable table, Long keyId ) {
        return new CatalogTable( table.id, table.name, table.schemaId, table.schemaName, table.databaseId, table.databaseName, table.ownerId, table.ownerName, table.tableType, table.definition, keyId );
    }

}

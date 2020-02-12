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


import org.polypheny.db.PolySqlType;
import org.polypheny.db.catalog.Catalog.Collation;
import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/**
 *
 */
@EqualsAndHashCode
public final class CatalogColumn implements CatalogEntity {

    private static final long serialVersionUID = -6566756853822620430L;

    public final long id;
    public final String name;
    public final long tableId;
    public final String tableName;
    public final long schemaId;
    public final String schemaName;
    public final long databaseId;
    public final String databaseName;
    public final int position;
    public final PolySqlType type;
    public final Integer length; // JDBC length or precision depending on type
    public final Integer scale; // decimal digits
    public final boolean nullable;
    public final Collation collation;
    public final CatalogDefaultValue defaultValue;


    public CatalogColumn(
            final long id,
            @NonNull final String name,
            final long tableId,
            @NonNull final String tableName,
            final long schemaId,
            @NonNull final String schemaName,
            final long databaseId,
            @NonNull final String databaseName,
            final int position,
            @NonNull final PolySqlType type,
            final Integer length,
            final Integer scale,
            final boolean nullable,
            final Collation collation,
            CatalogDefaultValue defaultValue ) {
        this.id = id;
        this.name = name;
        this.tableId = tableId;
        this.tableName = tableName;
        this.schemaId = schemaId;
        this.schemaName = schemaName;
        this.databaseId = databaseId;
        this.databaseName = databaseName;
        this.position = position;
        this.type = type;
        this.length = length;
        this.scale = scale;
        this.nullable = nullable;
        this.collation = collation;
        this.defaultValue = defaultValue;
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{
                databaseName,
                schemaName,
                tableName,
                name,
                type.getJavaSqlTypesValue(),
                type.name(),
                length,
                null,
                scale,
                null,
                nullable ? 1 : 0,
                "",
                defaultValue == null ? null : defaultValue.value,
                null,
                null,
                null,
                position,
                nullable ? "YES" : "NO",
                CatalogEntity.getEnumNameOrNull( collation ) };
    }


    @RequiredArgsConstructor
    public static class PrimitiveCatalogColumn {

        public final String tableCat;
        public final String tableSchem;
        public final String tableName;
        public final String columnName;
        public final int dataType;
        public final String typeName;
        public final Integer columnSize; // precision or length
        public final Integer bufferLength; // always null
        public final Integer decimalDigits; // scale
        public final Integer numPrecRadix;
        public final int nullable;
        public final String remarks;
        public final String columnDef;
        public final Integer sqlDataType; // always null
        public final Integer sqlDatetimeSub; // always null
        public final Integer charOctetLength; // always null
        public final int ordinalPosition; // position
        public final String isNullable;

        public final String collation;
    }
}

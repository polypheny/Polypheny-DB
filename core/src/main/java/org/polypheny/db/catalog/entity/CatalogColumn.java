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
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.type.PolyType;


/**
 *
 */
@EqualsAndHashCode
public final class CatalogColumn implements CatalogEntity, Comparable<CatalogColumn> {

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
    public final PolyType type;
    public final PolyType collectionsType;
    public final Integer length; // JDBC length or precision depending on type
    public final Integer scale; // decimal digits
    public final Integer dimension;
    public final Integer cardinality;
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
            @NonNull final PolyType type,
            final PolyType collectionsType,
            final Integer length,
            final Integer scale,
            final Integer dimension,
            final Integer cardinality,
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
        this.collectionsType = collectionsType;
        this.length = length;
        this.scale = scale;
        this.dimension = dimension;
        this.cardinality = cardinality;
        this.nullable = nullable;
        this.collation = collation;
        this.defaultValue = defaultValue;
    }


    public RelDataType getRelDataType( final RelDataTypeFactory typeFactory ) {
        RelDataType elementType;
        if ( this.length != null && this.scale != null && this.type.allowsPrecScale( true, true ) ) {
            elementType = typeFactory.createPolyType( this.type, this.length, this.scale );
        } else if ( this.length != null && this.type.allowsPrecNoScale() ) {
            elementType = typeFactory.createPolyType( this.type, this.length );
        } else {
            assert this.type.allowsNoPrecNoScale();
            elementType = typeFactory.createPolyType( this.type );
        }
        if ( collectionsType == PolyType.ARRAY ) {
            return typeFactory.createArrayType( elementType, cardinality != null ? cardinality : -1, dimension != null ? dimension : -1 );
        } else {
            return elementType;
        }
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{
                databaseName,
                schemaName,
                tableName,
                name,
                type.getJdbcOrdinal(),
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


    @Override
    public int compareTo( CatalogColumn o ) {
        if ( o != null ) {
            int comp = (int) (this.databaseId - o.databaseId);
            if ( comp == 0 ) {
                comp = (int) (this.schemaId - o.schemaId);
                if ( comp == 0 ) {
                    comp = (int) (this.tableId - o.tableId);
                    if ( comp == 0 ) {
                        return (int) (this.id - o.id);
                    } else {
                        return comp;
                    }

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


    public static CatalogColumn replaceName( CatalogColumn column, String name ) {
        return new CatalogColumn( column.id, name, column.tableId, column.tableName, column.schemaId, column.schemaName, column.databaseId, column.databaseName, column.position, column.type, column.collectionsType, column.length, column.scale, column.dimension, column.cardinality, column.nullable, column.collation, column.defaultValue );
    }


    public static CatalogColumn replacePosition( CatalogColumn column, int position ) {
        return new CatalogColumn( column.id, column.name, column.tableId, column.tableName, column.schemaId, column.schemaName, column.databaseId, column.databaseName, position, column.type, column.collectionsType, column.length, column.scale, column.dimension, column.cardinality, column.nullable, column.collation, column.defaultValue );
    }


    public static CatalogColumn replaceColumnType( CatalogColumn column, PolyType type, Integer length, Integer scale, Integer dimension, Integer cardinality, Collation collation ) {
        return new CatalogColumn( column.id, column.name, column.tableId, column.tableName, column.schemaId, column.schemaName, column.databaseId, column.databaseName, column.position, type, column.collectionsType, length, scale, dimension, cardinality, column.nullable, collation, column.defaultValue );
    }


    public static CatalogColumn replaceNullable( CatalogColumn column, boolean nullable ) {
        return new CatalogColumn( column.id, column.name, column.tableId, column.tableName, column.schemaId, column.schemaName, column.databaseId, column.databaseName, column.position, column.type, column.collectionsType, column.length, column.scale, column.dimension, column.cardinality, nullable, column.collation, column.defaultValue );
    }


    public static CatalogColumn replaceCollation( CatalogColumn column, Collation collation ) {
        return new CatalogColumn( column.id, column.name, column.tableId, column.tableName, column.schemaId, column.schemaName, column.databaseId, column.databaseName, column.position, column.type, column.collectionsType, column.length, column.scale, column.dimension, column.cardinality, column.nullable, collation, column.defaultValue );
    }


    // TODO: check defaultValue call
    public static CatalogColumn replaceDefaultValue( CatalogColumn column, CatalogDefaultValue defaultValue ) {
        return new CatalogColumn( column.id, column.name, column.tableId, column.tableName, column.schemaId, column.schemaName, column.databaseId, column.databaseName, column.position, column.type, column.collectionsType, column.length, column.scale, column.dimension, column.cardinality, column.nullable, column.collation, defaultValue );
    }

}

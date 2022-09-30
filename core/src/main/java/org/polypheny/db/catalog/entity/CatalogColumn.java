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

import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.type.PolyType;


@EqualsAndHashCode
public final class CatalogColumn implements CatalogObject, Comparable<CatalogColumn> {

    private static final long serialVersionUID = -4792846455300897399L;

    public final long id;
    public final String name;
    public final long tableId;
    public final long schemaId;
    public final long databaseId;
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
    @EqualsAndHashCode.Exclude
    // lombok uses getter methods to compare objects
    // and this method depends on the catalog, which can lead to nullpointers -> doNotUseGetters
    public NamespaceType namespaceType;


    public CatalogColumn(
            final long id,
            @NonNull final String name,
            final long tableId,
            final long schemaId,
            final long databaseId,
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
        this.schemaId = schemaId;
        this.databaseId = databaseId;
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


    public AlgDataType getAlgDataType( final AlgDataTypeFactory typeFactory ) {
        AlgDataType elementType;
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
        } else if ( collectionsType == PolyType.MAP ) {
            return typeFactory.createMapType( typeFactory.createPolyType( PolyType.ANY ), elementType );
        } else {
            return elementType;
        }
    }


    public NamespaceType getNamespaceType() {
        if ( namespaceType == null ) {
            namespaceType = Catalog.getInstance().getSchema( schemaId ).namespaceType;
        }
        return namespaceType;
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


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{
                getDatabaseName(),
                getSchemaName(),
                getTableName(),
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
                CatalogObject.getEnumNameOrNull( collation ) };
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


}

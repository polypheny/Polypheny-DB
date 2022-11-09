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
import java.util.LinkedList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.IndexType;


@EqualsAndHashCode(callSuper = false)
public final class CatalogIndex implements Serializable {

    private static final long serialVersionUID = -318228681682792406L;

    public final long id;
    public final String name;
    public final String physicalName;
    public final boolean unique;
    public final IndexType type;
    public final Integer location;
    public final String method;
    public final String methodDisplayName;

    public final CatalogKey key;
    public final long keyId;


    public CatalogIndex(
            final long id,
            @NonNull final String name,
            final boolean unique,
            final String method,
            final String methodDisplayName,
            final IndexType type,
            final Integer location,
            final long keyId,
            final CatalogKey key,
            final String physicalName ) {
        this.id = id;
        this.name = name;
        this.unique = unique;
        this.method = method;
        this.methodDisplayName = methodDisplayName;
        this.type = type;
        this.location = location;
        this.keyId = keyId;
        this.key = key;
        this.physicalName = physicalName;
    }


    // Used for creating ResultSets
    public List<CatalogIndexColumn> getCatalogIndexColumns() {
        int i = 1;
        List<CatalogIndexColumn> list = new LinkedList<>();
        for ( String columnName : key.getColumnNames() ) {
            list.add( new CatalogIndexColumn( id, i++, columnName ) );
        }
        return list;
    }


    public Serializable[] getParameterArray( int ordinalPosition, String columnName ) {
        return new Serializable[]{
                key.getDatabaseName(),
                key.getSchemaName(),
                key.getTableName(),
                !unique,
                null,
                name,
                0,
                ordinalPosition,
                columnName,
                null,
                -1,
                null,
                null,
                location,
                type.getId() };
    }


    // Used for creating ResultSets
    @RequiredArgsConstructor
    public static class CatalogIndexColumn implements CatalogObject {

        private static final long serialVersionUID = -5596459769680478780L;

        private final long indexId;
        private final int ordinalPosition;
        @Getter
        private final String columnName;


        @Override
        public Serializable[] getParameterArray() {
            return Catalog.getInstance().getIndex( indexId ).getParameterArray( ordinalPosition, columnName );
        }


        @RequiredArgsConstructor
        public static class PrimitiveCatalogIndexColumn {

            public final String tableCat;
            public final String tableSchem;
            public final String tableName;
            public final boolean nonUnique;
            public final String indexQualifier;
            public final String indexName;
            public final int type;
            public final int ordinalPosition;
            public final String columnName;
            public final Integer ascOrDesc;
            public final int cardinality;
            public final String pages;
            public final String filterCondition;
            public final int location;
            public final int indexType;

        }

    }

}

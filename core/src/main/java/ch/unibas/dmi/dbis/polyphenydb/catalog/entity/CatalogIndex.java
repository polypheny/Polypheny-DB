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

package ch.unibas.dmi.dbis.polyphenydb.catalog.entity;


import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.IndexType;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/**
 *
 */
@EqualsAndHashCode(callSuper = false)
public final class CatalogIndex {

    public final long id;
    public final String name;
    public final boolean unique;
    public final IndexType type;
    public final Integer location;

    public CatalogKey key;
    public long keyId;


    public CatalogIndex(
            final long id,
            @NonNull final String name,
            final boolean unique,
            final IndexType type,
            final Integer location,
            final long keyId ) {
        this.id = id;
        this.name = name;
        this.unique = unique;
        this.type = type;
        this.location = location;
        this.keyId = keyId;
    }


    // Used for creating ResultSets
    public List<CatalogIndexColumn> getCatalogIndexColumns() {
        int i = 1;
        LinkedList<CatalogIndexColumn> list = new LinkedList<>();
        for ( String columnName : key.columnNames ) {
            list.add( new CatalogIndexColumn( i++, columnName ) );
        }
        return list;
    }


    // Used for creating ResultSets
    @RequiredArgsConstructor
    public class CatalogIndexColumn implements CatalogEntity {

        private static final long serialVersionUID = -5596459769680478780L;

        private final int ordinalPosition;
        private final String columnName;


        @Override
        public Serializable[] getParameterArray() {
            return new Serializable[]{
                    key.databaseName,
                    key.schemaName,
                    key.tableName,
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


        @RequiredArgsConstructor
        public class PrimitiveCatalogIndexColumn {

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

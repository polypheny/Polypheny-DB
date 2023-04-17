/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.catalog.entity.logical;


import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogObject;
import org.polypheny.db.catalog.logistic.IndexType;


@EqualsAndHashCode(callSuper = false)
@Value
@SuperBuilder(toBuilder = true)
public class LogicalIndex implements Serializable {

    private static final long serialVersionUID = -318228681682792406L;

    @Serialize
    public long id;
    @Serialize
    public String name;
    @Serialize
    public String physicalName;
    @Serialize
    public boolean unique;
    @Serialize
    public IndexType type;
    @Serialize
    public long location;
    @Serialize
    public String method;
    @Serialize
    public String methodDisplayName;
    @Serialize
    public LogicalKey key;
    @Serialize
    public long keyId;


    public LogicalIndex(
            @Deserialize("id") final long id,
            @Deserialize("name") @NonNull final String name,
            @Deserialize("unique") final boolean unique,
            @Deserialize("method") final String method,
            @Deserialize("methodDisplayName") final String methodDisplayName,
            @Deserialize("type") final IndexType type,
            @Deserialize("location") final Long location,
            @Deserialize("keyId") final long keyId,
            @Deserialize("key") final LogicalKey key,
            @Deserialize("physicalName") final String physicalName ) {
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
            list.add( new CatalogIndexColumn( id, i++, columnName, this ) );
        }
        return list;
    }


    public Serializable[] getParameterArray( int ordinalPosition, String columnName ) {
        return new Serializable[]{
                Catalog.DATABASE_NAME,
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
    @Value
    public static class CatalogIndexColumn implements CatalogObject {

        private static final long serialVersionUID = -5596459769680478780L;

        public long indexId;
        public int ordinalPosition;

        public String columnName;

        public LogicalIndex index;


        @Override
        public Serializable[] getParameterArray() {
            return index.getParameterArray( ordinalPosition, columnName );
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

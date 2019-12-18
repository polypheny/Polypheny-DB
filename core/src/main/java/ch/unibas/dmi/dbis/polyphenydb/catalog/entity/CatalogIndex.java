/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
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

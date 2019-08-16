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


import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/**
 *
 */
@EqualsAndHashCode(callSuper = true)
public final class CatalogPrimaryKey extends CatalogKey {


    public CatalogPrimaryKey(
            final long id,
            final long tableId,
            @NonNull final String tableName,
            final long schemaId,
            @NonNull final String schemaName,
            final long databaseId,
            @NonNull final String databaseName ) {
        super( id, tableId, tableName, schemaId, schemaName, databaseId, databaseName );
    }


    public CatalogPrimaryKey( @NonNull final CatalogKey catalogKey ) {
        super(
                catalogKey.id,
                catalogKey.tableId,
                catalogKey.tableName,
                catalogKey.schemaId,
                catalogKey.schemaName,
                catalogKey.databaseId,
                catalogKey.databaseName,
                catalogKey.columnIds,
                catalogKey.columnNames );
    }


    // Used for creating ResultSets
    public List<CatalogPrimaryKeyColumn> getCatalogPrimaryKeyColumns() {
        int i = 1;
        LinkedList<CatalogPrimaryKeyColumn> list = new LinkedList<>();
        for ( String columnName : columnNames ) {
            list.add( new CatalogPrimaryKeyColumn( i++, columnName ) );
        }
        return list;
    }


    // Used for creating ResultSets
    @RequiredArgsConstructor
    public class CatalogPrimaryKeyColumn implements CatalogEntity {

        private static final long serialVersionUID = 5426944084650275437L;

        private final int keySeq;
        private final String columnName;


        @Override
        public Serializable[] getParameterArray() {
            return new Serializable[]{ databaseName, schemaName, tableName, columnName, keySeq, null };
        }


        @RequiredArgsConstructor
        public class PrimitiveCatalogPrimaryKeyColumn {

            public final String tableCat;
            public final String tableSchem;
            public final String tableName;
            public final String columnName;
            public final int keySeq;
            public final String pkName;
        }

    }


}

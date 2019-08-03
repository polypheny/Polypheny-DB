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
@EqualsAndHashCode(callSuper = false)
public final class CatalogForeignKey extends CatalogKey {

    private static final long serialVersionUID = -1496390493702171203L;

    public final long referencedKeyId;
    public final String referencedKeyName;
    public final long referencedKeyDatabaseId;
    public final String referencedKeyDatabaseName;
    public final long referencedKeySchemaId;
    public final String referencedKeySchemaName;
    public final long referencedKeyTableId;
    public final String referencedKeyTableName;
    public final Integer updateRule;
    public final Integer deleteRule;
    public final int deferrability;
    public List<Long> referencedKeyColumnIds;
    public List<String> referencedKeyColumnNames;


    public CatalogForeignKey(
            final long id,
            @NonNull final String name,
            final long tableId,
            @NonNull final String tableName,
            final long schemaId,
            @NonNull final String schemaName,
            final long databaseId,
            @NonNull final String databaseName,
            final boolean unique,
            final long referencedKeyId,
            @NonNull final String referencedKeyName,
            final long referencedKeyTableId,
            @NonNull final String referencedKeyTableName,
            final long referencedKeySchemaId,
            @NonNull final String referencedKeySchemaName,
            final long referencedKeyDatabaseId,
            @NonNull final String referencedKeyDatabaseName,
            final Integer updateRule,
            final Integer deleteRule,
            final int deferrability ) {
        super( id, name, tableId, tableName, schemaId, schemaName, databaseId, databaseName, unique );
        this.referencedKeyId = referencedKeyId;
        this.referencedKeyName = referencedKeyName;
        this.referencedKeyTableId = referencedKeyTableId;
        this.referencedKeyTableName = referencedKeyTableName;
        this.referencedKeySchemaId = referencedKeySchemaId;
        this.referencedKeySchemaName = referencedKeySchemaName;
        this.referencedKeyDatabaseId = referencedKeyDatabaseId;
        this.referencedKeyDatabaseName = referencedKeyDatabaseName;
        this.updateRule = updateRule;
        this.deleteRule = deleteRule;
        this.deferrability = deferrability;
    }


    // Used for creating ResultSets
    public List<CatalogForeignKeyColumn> getCatalogForeignKeyColumns() {
        int i = 1;
        LinkedList<CatalogForeignKeyColumn> list = new LinkedList<>();
        for ( String columnName : columnNames ) {
            list.add( new CatalogForeignKeyColumn( i, referencedKeyColumnNames.get( i - 1 ), columnName ) );
        }
        return list;
    }


    // Used for creating ResultSets
    @RequiredArgsConstructor
    public class CatalogForeignKeyColumn implements CatalogEntity {

        private final int keySeq;
        private final String referencedKeyColumnName;
        private final String foreignKeyColumnName;


        @Override
        public Serializable[] getParameterArray() {
            return new Serializable[]{ referencedKeyDatabaseName, referencedKeySchemaName, referencedKeyTableName, referencedKeyColumnName, databaseName, schemaName, tableName, foreignKeyColumnName, keySeq, updateRule, deleteRule, name, referencedKeyName, deferrability };
        }


        @RequiredArgsConstructor
        public class PrimitiveCatalogForeignKeyColumn {

            public final String pktableCat;
            public final String pktableSchem;
            public final String pktableName;
            public final String pkcolumnName;
            public final String fktableCat;
            public final String fktableSchem;
            public final String fktableName;
            public final String fkcolumnName;
            public final int keySeq;
            public final Integer updateRule;
            public final Integer deleteRule;
            public final String fkName;
            public final String pkName;
            public final int deferrability;
        }

    }


}

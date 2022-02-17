/*
 * Copyright 2019-2021 The Polypheny Project
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


import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.ForeignKeyOption;


@EqualsAndHashCode(callSuper = true)
public final class CatalogForeignKey extends CatalogKey {

    public final String name;
    public final long referencedKeyId;
    public final long referencedKeyDatabaseId;
    public final long referencedKeySchemaId;
    public final long referencedKeyTableId;
    public final ForeignKeyOption updateRule;
    public final ForeignKeyOption deleteRule;
    public final ImmutableList<Long> referencedKeyColumnIds;


    public CatalogForeignKey(
            final long id,
            @NonNull final String name,
            final long tableId,
            final long schemaId,
            final long databaseId,
            final long referencedKeyId,
            final long referencedKeyTableId,
            final long referencedKeySchemaId,
            final long referencedKeyDatabaseId,
            final List<Long> columnIds,
            final List<Long> referencedKeyColumnIds,
            final ForeignKeyOption updateRule,
            final ForeignKeyOption deleteRule ) {
        super( id, tableId, schemaId, databaseId, columnIds, EnforcementTime.ON_COMMIT );
        this.name = name;
        this.referencedKeyId = referencedKeyId;
        this.referencedKeyTableId = referencedKeyTableId;
        this.referencedKeySchemaId = referencedKeySchemaId;
        this.referencedKeyDatabaseId = referencedKeyDatabaseId;
        this.referencedKeyColumnIds = ImmutableList.copyOf( referencedKeyColumnIds );
        this.updateRule = updateRule;
        this.deleteRule = deleteRule;
    }


    @SneakyThrows
    public String getReferencedKeyDatabaseName() {
        return Catalog.getInstance().getDatabase( referencedKeyDatabaseId ).name;
    }


    @SneakyThrows
    public String getReferencedKeySchemaName() {
        return Catalog.getInstance().getSchema( referencedKeySchemaId ).name;
    }


    @SneakyThrows
    public String getReferencedKeyTableName() {
        return Catalog.getInstance().getTable( referencedKeyTableId ).name;
    }


    @SneakyThrows
    public List<String> getReferencedKeyColumnNames() {
        Catalog catalog = Catalog.getInstance();
        List<String> columnNames = new LinkedList<>();
        for ( long columnId : referencedKeyColumnIds ) {
            columnNames.add( catalog.getColumn( columnId ).name );
        }
        return columnNames;
    }


    // Used for creating ResultSets
    public List<CatalogForeignKeyColumn> getCatalogForeignKeyColumns() {
        int i = 1;
        LinkedList<CatalogForeignKeyColumn> list = new LinkedList<>();
        List<String> referencedKeyColumnNames = getReferencedKeyColumnNames();
        for ( String columnName : getColumnNames() ) {
            list.add( new CatalogForeignKeyColumn( tableId, name, i, referencedKeyColumnNames.get( i - 1 ), columnName ) );
            i++;
        }
        return list;
    }


    public Serializable[] getParameterArray( String referencedKeyColumnName, String foreignKeyColumnName, int keySeq ) {
        return new Serializable[]{
                getReferencedKeyDatabaseName(),
                getReferencedKeySchemaName(),
                getReferencedKeyTableName(),
                referencedKeyColumnName,
                getDatabaseName(),
                getSchemaName(),
                getTableName(),
                foreignKeyColumnName,
                keySeq,
                updateRule.getId(),
                deleteRule.getId(),
                name,
                null,
                null };
    }


    // Used for creating ResultSets
    @RequiredArgsConstructor
    public static class CatalogForeignKeyColumn implements CatalogEntity {

        private static final long serialVersionUID = -1496390493702171203L;

        private final long tableId;
        private final String foreignKeyName;

        private final int keySeq;
        private final String referencedKeyColumnName;
        private final String foreignKeyColumnName;


        @SneakyThrows
        @Override
        public Serializable[] getParameterArray() {
            return Catalog.getInstance()
                    .getForeignKey( tableId, foreignKeyName )
                    .getParameterArray( referencedKeyColumnName, foreignKeyColumnName, keySeq );
        }


        @RequiredArgsConstructor
        public static class PrimitiveCatalogForeignKeyColumn {

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
            public final Integer deferrability;

        }

    }

}

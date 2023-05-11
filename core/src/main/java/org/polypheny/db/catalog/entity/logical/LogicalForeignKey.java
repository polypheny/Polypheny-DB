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


import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogObject;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.snapshot.Snapshot;


@EqualsAndHashCode(callSuper = true)
public final class LogicalForeignKey extends LogicalKey {

    public final String name;
    public final long referencedKeyId;
    public final long referencedKeySchemaId;
    public final long referencedKeyTableId;
    public final ForeignKeyOption updateRule;
    public final ForeignKeyOption deleteRule;
    public final ImmutableList<Long> referencedKeyColumnIds;


    public LogicalForeignKey(
            final long id,
            @NonNull final String name,
            final long tableId,
            final long schemaId,
            final long referencedKeyId,
            final long referencedKeyTableId,
            final long referencedKeySchemaId,
            final List<Long> columnIds,
            final List<Long> referencedKeyColumnIds,
            final ForeignKeyOption updateRule,
            final ForeignKeyOption deleteRule ) {
        super( id, tableId, schemaId, columnIds, EnforcementTime.ON_COMMIT );
        this.name = name;
        this.referencedKeyId = referencedKeyId;
        this.referencedKeyTableId = referencedKeyTableId;
        this.referencedKeySchemaId = referencedKeySchemaId;
        this.referencedKeyColumnIds = ImmutableList.copyOf( referencedKeyColumnIds );
        this.updateRule = updateRule;
        this.deleteRule = deleteRule;
    }


    @SneakyThrows
    public String getReferencedKeySchemaName() {
        return Catalog.snapshot().getNamespace( referencedKeySchemaId ).name;
    }


    @SneakyThrows
    public String getReferencedKeyTableName() {
        return Catalog.snapshot().rel().getTable( referencedKeyTableId ).orElseThrow().name;
    }


    @SneakyThrows
    public List<String> getReferencedKeyColumnNames() {
        Snapshot snapshot = Catalog.snapshot();
        List<String> columnNames = new LinkedList<>();
        for ( long columnId : referencedKeyColumnIds ) {
            columnNames.add( snapshot.rel().getColumn( columnId ).orElseThrow().name );
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
                Catalog.DATABASE_NAME,
                getReferencedKeySchemaName(),
                getReferencedKeyTableName(),
                referencedKeyColumnName,
                Catalog.DATABASE_NAME,
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
    public static class CatalogForeignKeyColumn implements CatalogObject {

        private static final long serialVersionUID = 3287177728197412000L;

        private final long tableId;
        private final String foreignKeyName;

        private final int keySeq;
        private final String referencedKeyColumnName;
        private final String foreignKeyColumnName;


        @SneakyThrows
        @Override
        public Serializable[] getParameterArray() {
            return Catalog.snapshot()
                    .rel()
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

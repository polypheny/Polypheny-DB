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


import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.ForeignKeyOption;
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
public final class CatalogForeignKey extends CatalogKey {

    public final String name;
    public final long referencedKeyId;
    public final long referencedKeyDatabaseId;
    public final String referencedKeyDatabaseName;
    public final long referencedKeySchemaId;
    public final String referencedKeySchemaName;
    public final long referencedKeyTableId;
    public final String referencedKeyTableName;
    public final ForeignKeyOption updateRule;
    public final ForeignKeyOption deleteRule;
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
            final long referencedKeyId,
            final long referencedKeyTableId,
            @NonNull final String referencedKeyTableName,
            final long referencedKeySchemaId,
            @NonNull final String referencedKeySchemaName,
            final long referencedKeyDatabaseId,
            @NonNull final String referencedKeyDatabaseName,
            final ForeignKeyOption updateRule,
            final ForeignKeyOption deleteRule ) {
        super( id, tableId, tableName, schemaId, schemaName, databaseId, databaseName );
        this.name = name;
        this.referencedKeyId = referencedKeyId;
        this.referencedKeyTableId = referencedKeyTableId;
        this.referencedKeyTableName = referencedKeyTableName;
        this.referencedKeySchemaId = referencedKeySchemaId;
        this.referencedKeySchemaName = referencedKeySchemaName;
        this.referencedKeyDatabaseId = referencedKeyDatabaseId;
        this.referencedKeyDatabaseName = referencedKeyDatabaseName;
        this.updateRule = updateRule;
        this.deleteRule = deleteRule;
    }


    // Used for creating ResultSets
    public List<CatalogForeignKeyColumn> getCatalogForeignKeyColumns() {
        int i = 1;
        LinkedList<CatalogForeignKeyColumn> list = new LinkedList<>();
        for ( String columnName : columnNames ) {
            list.add( new CatalogForeignKeyColumn( i, referencedKeyColumnNames.get( i - 1 ), columnName ) );
            i++;
        }
        return list;
    }


    // Used for creating ResultSets
    @RequiredArgsConstructor
    public class CatalogForeignKeyColumn implements CatalogEntity {

        private static final long serialVersionUID = -1496390493702171203L;

        private final int keySeq;
        private final String referencedKeyColumnName;
        private final String foreignKeyColumnName;


        @Override
        public Serializable[] getParameterArray() {
            return new Serializable[]{
                    referencedKeyDatabaseName,
                    referencedKeySchemaName,
                    referencedKeyTableName,
                    referencedKeyColumnName,
                    databaseName,
                    schemaName,
                    tableName,
                    foreignKeyColumnName,
                    keySeq,
                    updateRule.getId(),
                    deleteRule.getId(),
                    name,
                    null,
                    null };
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
            public final Integer deferrability;
        }

    }


}

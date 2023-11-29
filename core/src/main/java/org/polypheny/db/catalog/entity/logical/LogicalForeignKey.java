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
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.PolyObject;
import org.polypheny.db.catalog.logistic.ForeignKeyOption;
import org.polypheny.db.catalog.snapshot.Snapshot;

@Value
@EqualsAndHashCode(callSuper = true)
public class LogicalForeignKey extends LogicalKey {

    @Serialize
    public String name;

    @Serialize
    public long referencedKeyId;

    @Serialize
    public long referencedKeySchemaId;

    @Serialize
    public long referencedKeyTableId;

    @Serialize
    public ForeignKeyOption updateRule;

    @Serialize
    public ForeignKeyOption deleteRule;

    @Serialize
    public ImmutableList<Long> referencedKeyColumnIds;


    public LogicalForeignKey(
            @Deserialize("id") final long id,
            @Deserialize("name") @NonNull final String name,
            @Deserialize("tableId") final long tableId,
            @Deserialize("namespaceId") final long namespaceId,
            @Deserialize("referencedKeyId") final long referencedKeyId,
            @Deserialize("referencedKeyTableId") final long referencedKeyTableId,
            @Deserialize("referencedKeySchemaId") final long referencedKeySchemaId,
            @Deserialize("columnIds") final List<Long> columnIds,
            @Deserialize("referencedKeyColumnIds") final List<Long> referencedKeyColumnIds,
            @Deserialize("updateRule") final ForeignKeyOption updateRule,
            @Deserialize("deleteRule") final ForeignKeyOption deleteRule ) {
        super( id, tableId, namespaceId, columnIds, EnforcementTime.ON_COMMIT );
        this.name = name;
        this.referencedKeyId = referencedKeyId;
        this.referencedKeyTableId = referencedKeyTableId;
        this.referencedKeySchemaId = referencedKeySchemaId;
        this.referencedKeyColumnIds = ImmutableList.copyOf( referencedKeyColumnIds );
        this.updateRule = updateRule;
        this.deleteRule = deleteRule;
    }


    public String getReferencedKeySchemaName() {
        return Catalog.snapshot().getNamespace( referencedKeySchemaId ).orElseThrow().name;
    }


    public String getReferencedKeyTableName() {
        return Catalog.snapshot().rel().getTable( referencedKeyTableId ).orElseThrow().name;
    }


    public List<String> getReferencedKeyColumnNames() {
        Snapshot snapshot = Catalog.snapshot();
        List<String> columnNames = new LinkedList<>();
        for ( long columnId : referencedKeyColumnIds ) {
            columnNames.add( snapshot.rel().getColumn( columnId ).orElseThrow().name );
        }
        return columnNames;
    }


    // Used for creating ResultSets
    public List<LogicalForeignKeyColumn> getCatalogForeignKeyColumns() {
        int i = 1;
        List<LogicalForeignKeyColumn> list = new LinkedList<>();
        List<String> referencedKeyColumnNames = getReferencedKeyColumnNames();
        for ( String columnName : getColumnNames() ) {
            list.add( new LogicalForeignKeyColumn( tableId, name, i, referencedKeyColumnNames.get( i - 1 ), columnName ) );
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
    public static class LogicalForeignKeyColumn implements PolyObject {

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
                    .orElseThrow()
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

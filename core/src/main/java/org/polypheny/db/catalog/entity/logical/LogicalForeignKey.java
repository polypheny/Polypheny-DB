/*
 * Copyright 2019-2024 The Polypheny Project
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
import java.io.Serial;
import java.util.ArrayList;
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
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;

@Value
@EqualsAndHashCode(callSuper = true)
public class LogicalForeignKey extends LogicalKey {

    @Serialize
    public String name;

    @Serialize
    public long referencedKeyId;

    @Serialize
    public long referencedKeyNamespaceId;

    @Serialize
    public long referencedKeyEntityId;

    @Serialize
    public ForeignKeyOption updateRule;

    @Serialize
    public ForeignKeyOption deleteRule;

    @Serialize
    public ImmutableList<Long> referencedKeyFieldIds;


    public LogicalForeignKey(
            @Deserialize("id") final long id,
            @Deserialize("name") @NonNull final String name,
            @Deserialize("entityId") final long entityId,
            @Deserialize("namespaceId") final long namespaceId,
            @Deserialize("referencedKeyId") final long referencedKeyId,
            @Deserialize("referencedKeyEntityId") final long referencedKeyEntityId,
            @Deserialize("referencedKeyNamespaceId") final long referencedKeyNamespaceId,
            @Deserialize("fieldIds") final List<Long> fieldIds,
            @Deserialize("referencedKeyFieldIds") final List<Long> referencedKeyFieldIds,
            @Deserialize("updateRule") final ForeignKeyOption updateRule,
            @Deserialize("deleteRule") final ForeignKeyOption deleteRule ) {
        super( id, entityId, namespaceId, fieldIds, EnforcementTime.ON_COMMIT );
        this.name = name;
        this.referencedKeyId = referencedKeyId;
        this.referencedKeyEntityId = referencedKeyEntityId;
        this.referencedKeyNamespaceId = referencedKeyNamespaceId;
        this.referencedKeyFieldIds = ImmutableList.copyOf( referencedKeyFieldIds );
        this.updateRule = updateRule;
        this.deleteRule = deleteRule;
    }


    public String getReferencedKeyNamespaceName() {
        return Catalog.snapshot().getNamespace( referencedKeyNamespaceId ).orElseThrow().name;
    }


    public String getReferencedKeyEntityName() {
        return Catalog.snapshot().rel().getTable( referencedKeyEntityId ).orElseThrow().name;
    }


    public List<String> getReferencedKeyFieldNames() {
        Snapshot snapshot = Catalog.snapshot();
        List<String> fieldsNames = new ArrayList<>();
        for ( long fieldId : referencedKeyFieldIds ) {
            fieldsNames.add( snapshot.rel().getColumn( fieldId ).orElseThrow().name );
        }
        return fieldsNames;
    }


    // Used for creating ResultSets
    public List<LogicalForeignKeyField> getCatalogForeignKeyFields() {
        int i = 1;
        List<LogicalForeignKeyField> list = new ArrayList<>();
        List<String> referencedKeyFieldNames = getReferencedKeyFieldNames();
        for ( String columnName : getFieldNames() ) {
            list.add( new LogicalForeignKeyField( entityId, name, i, referencedKeyFieldNames.get( i - 1 ), columnName ) );
            i++;
        }
        return list;
    }


    public PolyValue[] getParameterArray( String referencedKeyFieldName, String foreignKeyFieldName, int keySeq ) {
        return new PolyValue[]{
                PolyString.of( Catalog.DATABASE_NAME ),
                PolyString.of( getReferencedKeyNamespaceName() ),
                PolyString.of( getReferencedKeyEntityName() ),
                PolyString.of( referencedKeyFieldName ),
                PolyString.of( Catalog.DATABASE_NAME ),
                PolyString.of( getSchemaName() ),
                PolyString.of( getTableName() ),
                PolyString.of( foreignKeyFieldName ),
                PolyInteger.of( keySeq ),
                PolyInteger.of( updateRule.getId() ),
                PolyInteger.of( deleteRule.getId() ),
                PolyString.of( name ),
                null,
                null };
    }


    // Used for creating ResultSets
    @RequiredArgsConstructor
    public static class LogicalForeignKeyField implements PolyObject {

        @Serial
        private static final long serialVersionUID = 3287177728197412000L;

        private final long entityId;
        private final String foreignKeyName;

        private final int keySeq;
        private final String referencedKeyFieldName;
        private final String foreignKeyFieldName;


        @SneakyThrows
        @Override
        public PolyValue[] getParameterArray() {
            return Catalog.snapshot()
                    .rel()
                    .getForeignKey( entityId, foreignKeyName )
                    .orElseThrow()
                    .getParameterArray( referencedKeyFieldName, foreignKeyFieldName, keySeq );
        }


        public record PrimitiveCatalogForeignKeyColumn(String pktableCat, String pktableSchem, String pktableName, String pkcolumnName, String fktableCat, String fktableSchem, String fktableName, String fkcolumnName, int keySeq, Integer updateRule, Integer deleteRule, String fkName, String pkName, Integer deferrability) {

        }

    }

}

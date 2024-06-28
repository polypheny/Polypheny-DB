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
import java.util.ArrayList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import org.polypheny.db.catalog.Catalog;
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

}

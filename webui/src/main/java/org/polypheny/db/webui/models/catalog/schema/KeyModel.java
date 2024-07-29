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

package org.polypheny.db.webui.models.catalog.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalKey;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.webui.models.ForeignKeyModel;
import org.polypheny.db.webui.models.catalog.IdEntity;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
public class KeyModel extends IdEntity {

    @JsonProperty
    public long entityId;
    @JsonProperty
    public long namespaceId;
    @JsonProperty
    public List<Long> columnIds;
    @JsonProperty
    public boolean isPrimary;


    public KeyModel(
            @JsonProperty("id") @Nullable Long id,
            @JsonProperty("entityId") long entityId,
            @JsonProperty("namespaceId") long namespaceId,
            @JsonProperty("columnIds") List<Long> columnIds,
            @JsonProperty("isPrimary") boolean isPrimary ) {
        super( id, null );
        this.entityId = entityId;
        this.namespaceId = namespaceId;
        this.columnIds = columnIds;
        this.isPrimary = isPrimary;
    }


    public static KeyModel from( LogicalKey key ) {
        if ( key instanceof LogicalPrimaryKey ) {
            return new KeyModel( key.id, key.entityId, key.namespaceId, key.fieldIds, true );
        } else if ( key instanceof LogicalForeignKey foreignKey ) {
            return new ForeignKeyModel( key.id, key.entityId, key.namespaceId, key.fieldIds, foreignKey.referencedKeyEntityId, foreignKey.referencedKeyFieldIds );
        }
        return new KeyModel( key.id, key.entityId, key.namespaceId, key.fieldIds, false );
    }

}

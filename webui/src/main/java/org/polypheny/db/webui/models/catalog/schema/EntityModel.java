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
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.webui.models.catalog.IdEntity;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
public class EntityModel extends IdEntity {


    @JsonProperty
    public DataModel dataModel;

    @JsonProperty
    public EntityType entityType;

    @JsonProperty
    public Long namespaceId;

    @JsonProperty
    public boolean modifiable;


    public EntityModel(
            @JsonProperty("id") @Nullable Long id,
            @JsonProperty("name") @Nullable String name,
            @JsonProperty("namespaceId") @Nullable Long namespaceId,
            @JsonProperty("modifiable") boolean modifiable,
            @JsonProperty("dataModel") DataModel dataModel,
            @JsonProperty("entityType") EntityType entityType ) {
        super( id, name );
        this.namespaceId = namespaceId;
        this.dataModel = dataModel;
        this.entityType = entityType;
        this.modifiable = modifiable;
    }


    public static EntityModel from( LogicalEntity entity ) {
        return new EntityModel( entity.id, entity.name, entity.namespaceId, entity.modifiable, entity.dataModel, entity.entityType );
    }


}

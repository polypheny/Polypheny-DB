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
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.webui.models.catalog.IdEntity;

@EqualsAndHashCode(callSuper = true)
@Value
public class ConstraintModel extends IdEntity {

    @JsonProperty
    public long keyId;

    @JsonProperty
    public String type;


    public ConstraintModel(
            @JsonProperty("id") @Nullable Long id,
            @JsonProperty("name") @Nullable String name,
            @JsonProperty("keyId") long keyId,
            @JsonProperty("type") ConstraintType type ) {
        super( id, name );
        this.keyId = keyId;
        this.type = type.name();
    }


    public static ConstraintModel from( LogicalConstraint constraint ) {
        return new ConstraintModel( constraint.id, constraint.name, constraint.keyId, constraint.type );
    }

}

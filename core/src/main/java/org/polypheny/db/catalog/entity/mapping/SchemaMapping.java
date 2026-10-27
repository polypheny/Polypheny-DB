/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.catalog.entity.mapping;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.activej.serializer.annotations.SerializeNullable;
import io.activej.serializer.annotations.SerializeRecord;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;


/**
 * Mapping definition
 */
@SerializeRecord
public record SchemaMapping(
        @JsonProperty long id,
        @JsonProperty String name, // human-readable label for the mapping
        @JsonProperty MappingKind kind,
        @JsonProperty @SerializeNullable Long sourceEntityId,
        @JsonProperty long targetEntityId,
        @JsonProperty DataModel sourceModel,
        @JsonProperty DataModel targetModel,
        @JsonProperty List<MappingRule> rules ) {

    public SchemaMapping {
        Objects.requireNonNull( name, "name" );
        Objects.requireNonNull( kind, "kind" );
        Objects.requireNonNull( sourceModel, "sourceModel" );
        Objects.requireNonNull( targetModel, "targetModel" );
        if ( kind == MappingKind.PHYSICAL_TO_LOGICAL ) {
            if ( sourceEntityId != null ) {
                throw new IllegalArgumentException( "Physical-to-logical mappings resolve their source through an allocation; sourceEntityId must be null." );
            }
        } else {
            Objects.requireNonNull( sourceEntityId, "sourceEntityId is required for " + kind );
        }
        rules = List.copyOf( rules );
    }
}

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
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import io.activej.serializer.annotations.SerializeNullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.exceptions.MappingError;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;


/**
 * Mapping definition
 */
public record SchemaMapping(
        @JsonProperty @Deserialize("id") long id,
        @JsonProperty @Deserialize("name") String name, // human-readable label for the mapping
        @JsonProperty @Deserialize("kind") MappingKind kind,
        @JsonProperty @Deserialize("sourceEntityId") Long sourceEntityId,
        @JsonProperty @Deserialize("targetEntityId") long targetEntityId,
        @JsonProperty @Deserialize("sourceModel") DataModel sourceModel,
        @JsonProperty @Deserialize("targetModel") DataModel targetModel,
        @JsonProperty @Deserialize("rules") List<MappingRule> rules,
        @JsonProperty @Deserialize("sourceAllocationId") Long sourceAllocationId,
        @JsonProperty @Deserialize("excludedColumnRules") Map<Long, String> excludedColumnRules ) {

    public SchemaMapping( long id, String name, MappingKind kind, Long sourceEntityId, long targetEntityId, DataModel sourceModel,
            DataModel targetModel, List<MappingRule> rules, Long sourceAllocationId ) {
        this( id, name, kind, sourceEntityId, targetEntityId, sourceModel, targetModel, rules, sourceAllocationId, Map.of() );
    }

    public SchemaMapping withSourceAllocation( long allocationId ) {
        return new SchemaMapping( id, name, kind, sourceEntityId, targetEntityId, sourceModel, targetModel, rules, allocationId, excludedColumnRules );
    }

    public SchemaMapping {
        Objects.requireNonNull( name, "name" );
        Objects.requireNonNull( kind, "kind" );
        Objects.requireNonNull( sourceModel, "sourceModel" );
        Objects.requireNonNull( targetModel, "targetModel" );
        if ( kind == MappingKind.PHYSICAL_TO_LOGICAL ) {
            if ( sourceEntityId != null ) {
                throw MappingError.Validation.logicalSourceNotAllowed();
            }
        } else {
            Objects.requireNonNull( sourceEntityId, "sourceEntityId is required for " + kind );
        }
        rules = List.copyOf( rules );
        // Remember the last rule for each excluded relational column.
        // For exclude column, save, include flow the editor restores last saved rule for this column.
        // While the column is excluded, its saved rule is only editor metadata and does not run.
        excludedColumnRules = excludedColumnRules == null ? Map.of() : Map.copyOf( excludedColumnRules );
    }

    public <T extends MappingRule> boolean hasRule( Class<T> clazz ) {
        return hasRule( rules, clazz );
    }

    public static <T extends MappingRule> boolean hasRule( Collection<? extends MappingRule> rules, Class<T> clazz ) {
        return rules.stream().anyMatch( clazz::isInstance );
    }

    @Serialize(order = 1)
    public long id() {
        return id;
    }

    @Serialize(order = 2)
    public String name() {
        return name;
    }

    @Serialize(order = 3)
    public MappingKind kind() {
        return kind;
    }

    @Serialize(order = 4)
    public @SerializeNullable Long sourceEntityId() {
        return sourceEntityId;
    }

    @Serialize(order = 5)
    public long targetEntityId() {
        return targetEntityId;
    }

    @Serialize(order = 6)
    public DataModel sourceModel() {
        return sourceModel;
    }

    @Serialize(order = 7)
    public DataModel targetModel() {
        return targetModel;
    }

    @Serialize(order = 8)
    public List<MappingRule> rules() {
        return rules;
    }

    @Serialize(order = 9)
    public @SerializeNullable Long sourceAllocationId() {
        return sourceAllocationId;
    }

    @Serialize(order = 10)
    public Map<Long, String> excludedColumnRules() {
        return excludedColumnRules;
    }
}

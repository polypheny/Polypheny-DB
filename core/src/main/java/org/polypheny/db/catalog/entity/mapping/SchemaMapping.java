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
import io.activej.serializer.annotations.SerializeRecord;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;


/**
 * Mapping definition
 */
public record SchemaMapping(
        @JsonProperty long id,
        @JsonProperty String name, // human-readable label for the mapping
        @JsonProperty MappingKind kind,
        @JsonProperty Long sourceEntityId,
        @JsonProperty long targetEntityId,
        @JsonProperty DataModel sourceModel,
        @JsonProperty DataModel targetModel,
        @JsonProperty List<MappingRule> rules,
        @JsonProperty Long sourceAllocationId,
        @JsonProperty Map<Long, String> excludedColumnRules ) {

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
                throw new IllegalArgumentException( "Physical-to-logical mappings resolve their source through an allocation; sourceEntityId must be null." );
            }
        } else {
            Objects.requireNonNull( sourceEntityId, "sourceEntityId is required for " + kind );
        }
        rules = List.copyOf( rules );
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

    // Store editor metadata in the existing serialized list so older catalog records remain readable.
    @Serialize(order = 8)
    public List<MappingRule> serializedRules() {
        if ( excludedColumnRules.isEmpty() ) {
            return rules;
        }
        var stored = new ArrayList<>( rules );
        stored.add( new ExcludedColumnRules( excludedColumnRules ) );
        return stored;
    }

    @Serialize(order = 9)
    public @SerializeNullable Long sourceAllocationId() {
        return sourceAllocationId;
    }

    public static SchemaMapping fromSerialized(
            @Deserialize("id") long id, @Deserialize("name") String name, @Deserialize("kind") MappingKind kind,
            @Deserialize("sourceEntityId") Long sourceEntityId, @Deserialize("targetEntityId") long targetEntityId,
            @Deserialize("sourceModel") DataModel sourceModel, @Deserialize("targetModel") DataModel targetModel,
            @Deserialize("serializedRules") List<MappingRule> stored, @Deserialize("sourceAllocationId") Long sourceAllocationId ) {
        if ( !stored.isEmpty() && stored.get( stored.size() - 1 ) instanceof ExcludedColumnRules excluded ) {
            return new SchemaMapping( id, name, kind, sourceEntityId, targetEntityId, sourceModel, targetModel,
                    stored.subList( 0, stored.size() - 1 ), sourceAllocationId, excluded.ids() );
        }
        return new SchemaMapping( id, name, kind, sourceEntityId, targetEntityId, sourceModel, targetModel, stored, sourceAllocationId );
    }

    @SerializeRecord
    public record ExcludedColumnRules( Map<Long, String> ids ) implements MappingRule {
    }
}

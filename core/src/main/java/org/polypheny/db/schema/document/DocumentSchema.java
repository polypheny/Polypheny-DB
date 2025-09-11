/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.schema.document;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class DocumentSchema {

    public enum FieldType { BOOLEAN, STRING, NUMBER, INTEGER, ARRAY, OBJECT, DATE, TIMESTAMP, BINARY }
    public enum AdditionalProperties { ALLOW, FORBID }

    private final Set<String> required;
    private final Map<String, FieldType> types;
    private final AdditionalProperties additionalProperties;

    @JsonCreator
    public DocumentSchema(
            @JsonProperty("required") Set<String> required,
            @JsonProperty("types") Map<String, FieldType> types,
            @JsonProperty("additionalProperties") AdditionalProperties additionalProperties
    ) {
        this.required = Objects.requireNonNull(required, "required");
        this.types = Objects.requireNonNull(types, "types");
        this.additionalProperties = additionalProperties == null
                ? AdditionalProperties.ALLOW : additionalProperties;
    }

    @JsonProperty("required") public Set<String> required() { return required; }
    @JsonProperty("types") public Map<String, FieldType> types() { return types; }
    @JsonProperty("additionalProperties") public AdditionalProperties additionalProperties() { return additionalProperties; }

    public void validateOrThrow() {
        for (String p : required) if (p == null || p.isBlank()) throw new IllegalArgumentException("Required path must be non-empty");
        for (String p : types.keySet()) if (p == null || p.isBlank()) throw new IllegalArgumentException("Type path must be non-empty");
    }

    @Override public String toString() {
        return "DocumentSchema{required=" + required + ", types=" + types + ", addProps=" + additionalProperties + "}";
    }
}

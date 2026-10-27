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

package org.polypheny.db.catalog.snapshot;

import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;


/**
 * Provides an immutable view of mapping definitions, indexed by mapping ID and target logical entity ID.
 */
public interface MappingSnapshot {

    /**
     * Returns the definition with the given mapping ID, or an empty optional if it is absent.
     */
    @NonNull
    Optional<SchemaMapping> getMapping( long mappingId );

    /**
     * Returns the definition targeting the given logical entity ID, or an empty optional if it is absent.
     */
    @NonNull
    Optional<SchemaMapping> getMappingForLogicalEntity( long logicalEntityId );

    /**
     * Returns every definition exactly once in an immutable list in input iteration order.
     */
    @NonNull
    List<SchemaMapping> getMappings();

}

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

package org.polypheny.db.catalog.snapshot.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.rules.DirectColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.snapshot.MappingSnapshot;


/**
 * Builds an immutable mapping list and logical-entity index without accessing a live catalog or adapter.
 */
public final class MappingSnapshotImpl implements MappingSnapshot {

    private final List<SchemaMapping> mappings;
    private final Map<Long, SchemaMapping> mappingsById;
    private final Map<Long, SchemaMapping> mappingsByLogicalEntity;


    /**
     * Builds the immutable snapshot from supplied mappings and checks their internal consistency.
     * Existence of logical entities, columns, and allocations must be validated during catalog integration.
     *
     * @param mappings definitions to snapshot
     * @throws NullPointerException if the input collection or an element is null
     * @throws GenericRuntimeException if identities conflict or a rule implementation is unsupported
     */
    public MappingSnapshotImpl( Collection<SchemaMapping> mappings ) {
        Objects.requireNonNull( mappings, "mappings" );

        Map<Long, SchemaMapping> byId = new HashMap<>();
        Map<Long, SchemaMapping> byLogicalEntity = new HashMap<>();
        for ( SchemaMapping mapping : mappings ) {
            Objects.requireNonNull( mapping, "mappings element" );
            Set<Long> ruleColumnIds = new HashSet<>();
            for ( MappingRule rule : mapping.rules() ) {
                if ( !(rule instanceof DirectColumnMappingRule directRule) ) {
                    throw new GenericRuntimeException( "Mapping %d has unsupported rule type %s.", mapping.id(), rule.getClass().getName() );
                }
                if ( !ruleColumnIds.add( directRule.targetLogicalColumnId() ) ) {
                    throw new GenericRuntimeException(
                            "Mapping %d has duplicate logical column ID %d.",
                            mapping.id(), directRule.targetLogicalColumnId() );
                }
            }

            if ( byId.putIfAbsent( mapping.id(), mapping ) != null ) {
                throw new GenericRuntimeException( "Duplicate mapping ID %d.", mapping.id() );
            }
            SchemaMapping previous = byLogicalEntity.putIfAbsent( mapping.targetEntityId(), mapping );
            if ( previous != null ) {
                throw new GenericRuntimeException(
                        "Duplicate logical entity ID %d in mappings %d and %d.",
                        mapping.targetEntityId(), previous.id(), mapping.id() );
            }
        }

        this.mappings = List.copyOf( mappings );
        this.mappingsById = Map.copyOf( byId );
        this.mappingsByLogicalEntity = Map.copyOf( byLogicalEntity );
    }


    @Override
    public @NotNull Optional<SchemaMapping> getMapping( long mappingId ) {
        return Optional.ofNullable( mappingsById.get( mappingId ) );
    }


    @Override
    public @NotNull Optional<SchemaMapping> getMappingForLogicalEntity( long logicalEntityId ) {
        return Optional.ofNullable( mappingsByLogicalEntity.get( logicalEntityId ) );
    }


    @Override
    public @NotNull List<SchemaMapping> getMappings() {
        return mappings;
    }


}

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.mapping.DocumentMappingBinding;
import org.polypheny.db.algebra.mapping.RuleRepository;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.rules.ColumnMappingRule;
import org.polypheny.db.catalog.entity.mapping.rules.MappingRule;
import org.polypheny.db.catalog.exceptions.MappingError;
import org.polypheny.db.catalog.exceptions.MappingValidationException;
import org.polypheny.db.catalog.snapshot.MappingSnapshot;


/**
 * Builds an immutable mapping list and logical-entity index without accessing a live catalog or adapter.
 */
public final class MappingSnapshotImpl implements MappingSnapshot {

    private final List<SchemaMapping> mappings;
    private final Map<Long, SchemaMapping> mappingsById;
    private final Map<Long, List<SchemaMapping>> mappingsByLogicalEntity;


    /**
     * Builds the immutable snapshot from supplied mappings and checks their internal consistency.
     * Existence of logical entities, columns, and allocations must be validated during catalog integration.
     *
     * @param mappings definitions to snapshot
     * @throws NullPointerException if the input collection or an element is null
     * @throws MappingValidationException if identities conflict or a rule implementation is unsupported
     */
    public MappingSnapshotImpl( Collection<SchemaMapping> mappings ) {
        Objects.requireNonNull( mappings, "mappings" );

        Map<Long, SchemaMapping> byId = new HashMap<>();
        Map<Long, List<SchemaMapping>> byLogicalEntity = new HashMap<>();
        for ( SchemaMapping mapping : mappings ) {
            Objects.requireNonNull( mapping, "mappings element" );
            validateRules( mapping );

            if ( byId.putIfAbsent( mapping.id(), mapping ) != null ) {
                throw MappingError.Duplicate.mappingId( mapping.id() );
            }
            byLogicalEntity.computeIfAbsent( mapping.targetEntityId(), ignored -> new ArrayList<>() ).add( mapping );
        }

        this.mappings = List.copyOf( mappings );
        this.mappingsById = Map.copyOf( byId );
        byLogicalEntity.replaceAll( ( id, values ) -> List.copyOf( values ) );
        this.mappingsByLogicalEntity = Map.copyOf( byLogicalEntity );
    }


    private static void validateRules( SchemaMapping mapping ) {
        switch ( mapping.targetModel() ) {
            case RELATIONAL -> validateRelRules( mapping );
            case DOCUMENT -> validateDocumentRules( mapping );
            case GRAPH -> throw MappingError.Unsupported.models( mapping.id(), mapping.sourceModel(), mapping.targetModel() );
        }
    }


    private static void validateDocumentRules( SchemaMapping mapping ) {
        DocumentMappingBinding.validateIdentity( mapping );
    }


    private static void validateRelRules( SchemaMapping mapping ) {
        Set<Long> ruleColumnIds = new HashSet<>();
        for ( MappingRule rule : mapping.rules() ) {
            if ( RuleRepository.definitionFor( rule.getClass() ).isEmpty() ) {
                throw MappingError.Unsupported.rule( mapping.id(), rule.getClass().getName() );
            }
            if ( rule instanceof ColumnMappingRule columnRule ) {
                long columnId = columnRule.targetLogicalColumnId();
                if ( !ruleColumnIds.add( columnId ) ) {
                    throw MappingError.Duplicate.logicalColumn( mapping.id(), columnId );
                }
            } else {
                throw MappingError.Unsupported.ruleValidation( mapping.id(), rule.getClass().getName() );
            }
        }
    }


    @Override
    public @NotNull Optional<SchemaMapping> getMapping( long mappingId ) {
        return Optional.ofNullable( mappingsById.get( mappingId ) );
    }


    @Override
    public @NotNull List<SchemaMapping> getMappingsForLogicalEntity( long logicalEntityId ) {
        return mappingsByLogicalEntity.getOrDefault( logicalEntityId, List.of() );
    }

    @Override
    public @NotNull List<SchemaMapping> getMappings() {
        return mappings;
    }


}

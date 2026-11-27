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

package org.polypheny.db.catalog.mapping;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.polypheny.db.algebra.mapping.DocumentMappingBinding;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.SchemaMappingManager;
import org.polypheny.db.catalog.entity.mapping.rules.DocumentCopyEntireMappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.exceptions.MappingError;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.ddl.DocumentMappingUpdate;

/**
 * DocumentMappingState gathers the information needed to view, create, edit, or remove a document mapping.
 */
public record DocumentMappingState( LogicalAdapter adapter, SchemaMapping mapping, LogicalNamespace namespace,
                                    LogicalCollection collection, PhysicalCollection physical, String revision,
                                    String readOnlyReason, String removalBlockedReason, Long fixedTargetEntityId ) {

    public static DocumentMappingState load( Catalog catalog, long mappingId ) {
        var mapping = catalog.getSnapshot().mappings().getMapping( mappingId )
                .orElseThrow( () -> MappingError.Missing.mapping( mappingId ) );
        return resolve( catalog, mapping );
    }


    public static DocumentMappingState draft( Catalog catalog, long adapterId, long sourceEntityId, Long sourceAllocationId ) {
        return draft( catalog, adapterId, sourceEntityId, sourceAllocationId, null );
    }


    public static DocumentMappingState draft( Catalog catalog, long adapterId, long sourceEntityId, Long sourceAllocationId, Long existingTargetEntityId ) {
        var snapshot = catalog.getSnapshot();
        var source = snapshot.doc().getCollection( sourceEntityId ).orElseThrow( () -> MappingError.Missing.collection( sourceEntityId ) );
        var allocations = snapshot.alloc().getFromLogical( source.id ).stream()
                .filter( a -> a.adapterId == adapterId && (sourceAllocationId == null || sourceAllocationId == a.id) ).toList();
        if ( allocations.size() != 1 ) {
            throw MappingError.Missing.sourceAllocation( -1, sourceAllocationId );
        }
        var mapping = new SchemaMapping( -1, "New mapping", MappingKind.PHYSICAL_TO_LOGICAL, null, source.id,
                DataModel.DOCUMENT, DataModel.DOCUMENT, List.of( new DocumentCopyEntireMappingRule() ), allocations.get( 0 ).id );
        var state = resolve( catalog, mapping );
        String name = SchemaMappingManager.availableDefaultMappingName( snapshot.mappings().getMappings(), state.adapter.uniqueName, state.physical.name );
        if ( existingTargetEntityId != null ) {
            var target = snapshot.doc().getCollection( existingTargetEntityId ).orElseThrow( MappingError.Missing::resource );
            if ( !supportsExistingTarget( snapshot, target ) ) {
                throw MappingError.Unsupported.editor( "Existing-target mappings require a mapped, read-only source collection with one partition." );
            }
            if ( SchemaMappingManager.isPhysicalSourceMappedToTarget( catalog, mapping, target.id ) ) {
                throw MappingError.Duplicate.sourceTargetPair();
            }
            var namespace = snapshot.getNamespace( target.namespaceId ).orElseThrow( MappingError.Missing::targetNamespace );
            var revision = newRevision( state.revision, namespace.id, namespace.name, namespace.caseSensitive,
                    target.id, target.name, snapshot.mappings().getMappingsForLogicalEntity( target.id ) );
            var targetMapping = new SchemaMapping( -1, name, mapping.kind(), null, target.id, mapping.sourceModel(),
                    mapping.targetModel(), mapping.rules(), mapping.sourceAllocationId() );
            return new DocumentMappingState( state.adapter, targetMapping, namespace, target, state.physical, revision,
                    state.readOnlyReason, null, target.id );
        }
        String base = state.physical.name.replaceAll( "[^a-zA-Z0-9_]", "_" );
        if ( base.isEmpty() || Character.isDigit( base.charAt( 0 ) ) ) {
            base = "source_" + base;
        }
        base = base.substring( 0, Math.min( base.length(), SchemaMappingManager.IDENTIFIER_MAX_LENGTH - 16 ) ) + "_mapped";
        String targetName = base;
        for ( int suffix = 2; snapshot.doc().getCollection( source.namespaceId, targetName ).isPresent(); suffix++ ) {
            targetName = base + "_" + suffix;
        }
        return new DocumentMappingState( state.adapter, SchemaMappingManager.renameMapping( mapping, name ), state.namespace,
                source.toBuilder().name( targetName ).build(), state.physical, state.revision, state.readOnlyReason, null, null );
    }


    public static boolean supportsExistingTarget( Snapshot snapshot, LogicalCollection target ) {
        var mappings = snapshot.mappings().getMappingsForLogicalEntity( target.id );
        return target.entityType == EntityType.SOURCE && !target.modifiable
                && snapshot.getNamespace( target.namespaceId ).filter( namespace -> !namespace.hidden ).isPresent()
                && !mappings.isEmpty() && mappings.size() == snapshot.alloc().getFromLogical( target.id ).size()
                && snapshot.alloc().getPartitionsFromLogical( target.id ).size() == 1;
    }


    private static DocumentMappingState resolve( Catalog catalog, SchemaMapping mapping ) {
        DocumentMappingBinding.validateIdentity( mapping );
        var snapshot = catalog.getSnapshot();
        var collection = snapshot.doc().getCollection( mapping.targetEntityId() )
                .orElseThrow( () -> MappingError.Missing.collection( mapping.targetEntityId() ) );
        var namespace = snapshot.getNamespace( collection.namespaceId ).orElseThrow( MappingError.Missing::targetNamespace );
        var allocations = SchemaMappingManager.sourceAllocations( catalog, mapping );
        if ( allocations.size() != 1 ) {
            throw MappingError.Algebra.sourceBindingsAmbiguous();
        }
        var allocation = allocations.get( 0 );
        var adapter = snapshot.getAdapter( allocation.adapterId ).orElseThrow( () -> MappingError.Missing.adapter( allocation.adapterId ) );
        var adapterCatalog = catalog.getAdapterCatalog( adapter.id ).orElseThrow( () -> MappingError.Missing.adapterCatalog( adapter.id ) );
        var binding = DocumentMappingBinding.create( mapping, allocation, adapterCatalog );
        var physical = (PhysicalCollection) adapterCatalog.physicals.get( binding.physicalEntityId() ).normalize();
        String reason = SchemaMappingManager.documentReadOnlyReason( adapter.type, collection );
        String removalReason = mapping.id() < 0 ? null : SchemaMappingManager.removalBlockedReason( catalog, mapping );
        var siblings = snapshot.mappings().getMappingsForLogicalEntity( collection.id );
        Long fixedTarget = siblings.size() > 1 && mapping.id() >= 0 ? collection.id : null;
        var revision = newRevision( mapping, namespace.id, namespace.name, namespace.caseSensitive, namespace.hidden,
                collection.id, collection.name, collection.namespaceId, collection.entityType, collection.modifiable,
                adapter.id, adapter.uniqueName, adapter.adapterName, adapter.type, adapter.mode, allocation.id, allocation.placementId, allocation.partitionId,
                physical.id, physical.namespaceId, physical.namespaceName, physical.name, new java.util.TreeMap<>( adapter.settings ), siblings );
        return new DocumentMappingState( adapter, mapping, namespace, collection, physical, revision, reason, removalReason, fixedTarget );
    }


    private static String newRevision( Object... metadata ) {
        String revision;
        try {
            var digest = MessageDigest.getInstance( "SHA-256" );
            hash( digest, metadata );
            revision = HexFormat.of().formatHex( digest.digest() );
        } catch ( NoSuchAlgorithmException e ) {
            throw new IllegalStateException( "SHA-256 is unavailable", e );
        }
        return revision;
    }


    public boolean editable() {
        return readOnlyReason == null;
    }


    public boolean removable() {
        return removalBlockedReason == null;
    }


    public void validateRevision( String expectedRevision ) {
        if ( !revision.equals( expectedRevision ) ) {
            throw MappingError.Revision.stale();
        }
    }


    public void validateRemoval( String expectedRevision ) {
        validateRevision( expectedRevision );
        if ( !removable() ) {
            throw MappingError.Conflict.removalBlocked( mapping.name(), removalBlockedReason, List.of( collection.name ) );
        }
    }


    public DocumentMappingUpdate validate( DocumentMappingUpdate update, Snapshot snapshot ) {
        if ( update.adapterId() != adapter.id || update.mappingId() != mapping.id() ) {
            throw MappingError.Missing.resource();
        }
        validateRevision( update.expectedRevision() );
        if ( !editable() ) {
            throw MappingError.Unsupported.editor( readOnlyReason );
        }
        if ( update.sourceAllocationId() != null && update.sourceAllocationId() != physical.allocationId ) {
            throw MappingError.Algebra.allocationMismatch( mapping.id(), update.sourceAllocationId() );
        }
        long namespaceId = update.targetNamespaceId() == null ? namespace.id : update.targetNamespaceId();
        var targetNamespace = snapshot.getNamespace( namespaceId ).orElseThrow( MappingError.Missing::targetNamespace );
        if ( targetNamespace.dataModel != DataModel.DOCUMENT || targetNamespace.hidden ) {
            throw MappingError.Validation.fields( Map.of( "targetNamespaceId", "Choose a visible document namespace." ) );
        }
        Map<String, String> errors = new LinkedHashMap<>();
        boolean movesTarget = namespaceId != namespace.id;
        if ( fixedTargetEntityId != null && movesTarget ) {
            errors.put( "targetNamespaceId", "The existing target collection is fixed." );
        }
        String label = update.mappingName() == null ? "" : update.mappingName().trim();
        if ( label.isEmpty() || label.length() > SchemaMappingManager.MAPPING_NAME_MAX_LENGTH ) {
            errors.put( "mappingName", SchemaMappingManager.mappingNameMessage() );
        } else if ( SchemaMappingManager.isMappingNameTaken( snapshot.mappings().getMappings(), label, mapping.id() ) ) {
            errors.put( "mappingName", "A mapping with this name already exists." );
        }
        String name = update.logicalCollectionName() == null ? "" : update.logicalCollectionName().trim();
        if ( fixedTargetEntityId != null && !name.equals( collection.name ) ) {
            errors.put( "logicalCollectionName", "The existing target collection is fixed." );
        } else if ( name.isEmpty() || ((!name.equals( collection.name ) || (mapping.id() < 0 && fixedTargetEntityId == null))
                && (name.length() > SchemaMappingManager.IDENTIFIER_MAX_LENGTH || !name.matches( SchemaMappingManager.IDENTIFIER_PATTERN ))) ) {
            errors.put( "logicalCollectionName", SchemaMappingManager.identifierMessage() );
        }
        name = fixedTargetEntityId != null || targetNamespace.caseSensitive ? name : name.toLowerCase( Locale.ROOT );
        String targetName = name;
        if ( snapshot.doc().getCollections( namespaceId, null ).stream()
                .anyMatch( c -> ((mapping.id() < 0 && fixedTargetEntityId == null) || c.id != collection.id)
                        && (targetNamespace.caseSensitive ? c.name.equals( targetName ) : c.name.equalsIgnoreCase( targetName )) ) ) {
            errors.put( "logicalCollectionName", "An entity with this name already exists in the namespace." );
        }
        if ( !errors.isEmpty() ) {
            throw MappingError.Validation.fields( errors );
        }
        if ( mapping.id() >= 0 && (movesTarget || !name.equals( collection.name )) ) {
            var dependencies = SchemaMappingManager.documentDependencies( snapshot, collection.id );
            if ( !dependencies.isEmpty() ) {
                throw MappingError.Conflict.schemaChangeBlocked( true, dependencies );
            }
        }
        return new DocumentMappingUpdate( adapter.id, mapping.id(), update.expectedRevision(), label, name,
                namespaceId, update.existingTargetEntityId(), physical.allocationId );
    }


    private static void hash( MessageDigest digest, Object... values ) {
        for ( Object value : values ) {
            byte[] bytes = String.valueOf( value ).getBytes( StandardCharsets.UTF_8 );
            digest.update( ByteBuffer.allocate( Integer.BYTES ).putInt( bytes.length ).array() );
            digest.update( bytes );
        }
    }

}

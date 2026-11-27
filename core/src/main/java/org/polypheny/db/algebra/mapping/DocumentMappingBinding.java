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

package org.polypheny.db.algebra.mapping;

import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.catalogs.DocAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.rules.DocumentCopyEntireMappingRule;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.exceptions.MappingError;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;

/**
 * Connects a document mapping to the physical collection it will read.
 * It records the logical collection ID, allocation ID, adapter ID, and physical collection ID, and checks that they all refer to the correct source.
 */
public record DocumentMappingBinding( long allocationId, long logicalEntityId, long adapterId, long physicalEntityId ) {

    /**
     * checks that the mapping is physical-to-logical and document-to-document,
     * contains exactly one DocumentCopyEntireMappingRule, and has no excluded-column settings.
     */
    public static void validateIdentity( SchemaMapping mapping ) {
        if ( mapping.kind() != MappingKind.PHYSICAL_TO_LOGICAL
                || mapping.sourceModel() != DataModel.DOCUMENT || mapping.targetModel() != DataModel.DOCUMENT ) {
            throw MappingError.Unsupported.documentMappingRequired( mapping.id() );
        }
        if ( mapping.rules().size() != 1 || !(mapping.rules().get( 0 ) instanceof DocumentCopyEntireMappingRule)
                || !mapping.excludedColumnRules().isEmpty() ) {
            throw MappingError.Validation.documentIdentityRequired( mapping.id() );
        }
    }


    public static DocumentMappingBinding create( SchemaMapping mapping, AllocationEntity allocation, AdapterCatalog catalog ) {
        validateIdentity( mapping );
        if ( catalog == null ) {
            throw MappingError.Missing.adapterCatalog( allocation.adapterId );
        }
        if ( !(catalog instanceof DocAdapterCatalog) || !(allocation instanceof AllocationCollection) ) {
            throw MappingError.Unsupported.documentMappingRequired( mapping.id() );
        }
        if ( allocation.dataModel != DataModel.DOCUMENT ) {
            throw MappingError.Incompatible.sourceModel( mapping.id(), mapping.sourceModel(), allocation.id, allocation.dataModel );
        }
        var physicals = catalog.getPhysicalsFromAllocs( allocation.id );
        if ( physicals == null || physicals.size() != 1 || physicals.get( 0 ) == null
                || !(physicals.get( 0 ).normalize() instanceof PhysicalCollection collection) ) {
            throw MappingError.Missing.physicalCollection( mapping.id(), allocation.id );
        }
        var nativeCollection = physicals.get( 0 );
        if ( catalog.adapterId != allocation.adapterId || collection.adapterId != allocation.adapterId
                || nativeCollection.dataModel != DataModel.DOCUMENT || nativeCollection.id != collection.id
                || nativeCollection.allocationId != collection.allocationId || nativeCollection.logicalId != collection.logicalId
                || collection.dataModel != DataModel.DOCUMENT || collection.allocationId != allocation.id
                || collection.logicalId != allocation.logicalId || collection.namespaceId != allocation.namespaceId
                || !allocation.equals( catalog.allocations.get( allocation.id ) ) ) {
            throw MappingError.Algebra.physicalBindingMismatch( mapping.id(), allocation.id );
        }
        var binding = new DocumentMappingBinding( allocation.id, allocation.logicalId, allocation.adapterId, collection.id );
        binding.validate( mapping );
        return binding;
    }


    /**
     * checks that the binding matches the mapping’s target collection and any explicitly specified source allocation.
     */
    public void validate( SchemaMapping mapping ) {
        validateIdentity( mapping );
        if ( mapping.targetEntityId() != logicalEntityId
                || mapping.sourceAllocationId() != null && mapping.sourceAllocationId() != allocationId ) {
            throw MappingError.Algebra.allocationMismatch( mapping.id(), allocationId );
        }
    }

}

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.physical.PhysicalColumn;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.MappingError;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.util.Pair;


/**
 * Connects the mapping logical target to the registered physical source metadata for the selected allocation
 * It captures the adapter, allocation and physical table IDs, plus the physical columns.
 * The planner uses this binding to resolve each target column’s source field.
 */
public record PhysicalMappingBinding(
        long allocationId,
        long logicalEntityId,
        long adapterId,
        long physicalEntityId,
        Map<Long, PhysicalColumn> columns ) {

    public PhysicalMappingBinding {
        columns = Map.copyOf( columns );
    }


    /**
     * Binds the mapping’s logical target to the physical source in the selected allocation
     */
    public static PhysicalMappingBinding create( SchemaMapping mapping, AllocationEntity allocation, AdapterCatalog adapterCatalog ) {
        if ( adapterCatalog == null ) {
            throw MappingError.Missing.adapterCatalog( allocation.adapterId );
        }
        if ( !(adapterCatalog instanceof RelAdapterCatalog) ) {
            throw MappingError.Unsupported.relationalAdapterRequired( mapping.id(), allocation.id );
        }

        // Find the physical table registered for the selected allocation.
        List<PhysicalEntity> physicals = adapterCatalog.getPhysicalsFromAllocs( allocation.id );
        if ( physicals == null || physicals.isEmpty() || physicals.get( 0 ) == null ) {
            throw MappingError.Missing.physicalTable( mapping.id(), allocation.id );
        }
        if ( !(physicals.get( 0 ) instanceof PhysicalTable table) ) {
            throw MappingError.Unsupported.relationalSourceRequired( mapping.id(), allocation.id );
        }
        if ( table.allocationId != allocation.id || table.logicalId != allocation.logicalId
                || table.adapterId != allocation.adapterId || adapterCatalog.adapterId != allocation.adapterId ) {
            throw MappingError.Algebra.physicalBindingMismatch( mapping.id(), allocation.id );
        }

        Map<Long, PhysicalColumn> columns = new HashMap<>();
        for ( PhysicalColumn column : table.columns ) {
            var field = adapterCatalog.fields.get( Pair.of( allocation.id, column.id ) );
            if ( field == null ) {
                throw MappingError.Missing.physicalColumn( mapping.id(), column.id, allocation.id );
            }
            if ( !(field instanceof PhysicalColumn registered) || !registered.equals( column ) || registered.allocId != allocation.id
                    || registered.logicalEntityId != allocation.logicalId || registered.adapterId != allocation.adapterId ) {
                throw MappingError.Algebra.physicalColumnMismatch( mapping.id(), column.id, allocation.id );
            }
            columns.put( column.id, registered );
        }
        PhysicalMappingBinding binding = new PhysicalMappingBinding( allocation.id, allocation.logicalId, allocation.adapterId, table.id, columns );
        binding.validate( mapping );
        return binding;
    }


    public void validate( SchemaMapping mapping ) {
        if ( mapping.kind() != MappingKind.PHYSICAL_TO_LOGICAL
                || mapping.sourceModel() != DataModel.RELATIONAL || mapping.targetModel() != DataModel.RELATIONAL ) {
            throw MappingError.Unsupported.relationalMappingRequired( mapping.id() );
        }
        // check that the binding uses the mapping’s specified source allocation
        // allocation should be checked for multiple source to single target mapping
        if ( mapping.targetEntityId() != logicalEntityId || (mapping.sourceAllocationId() != null && mapping.sourceAllocationId() != allocationId) ) {
            throw MappingError.Algebra.allocationMismatch( mapping.id(), allocationId );
        }
    }

}

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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelMappingTransform;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.exceptions.MappingError;
import org.polypheny.db.catalog.exceptions.MappingException;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;


/**
 * Builds source scans from the query snapshot and attaches their mappings before combining them.
 * Physical scan dispatch is model-specific; mapped branches currently use relational bindings and UNION ALL.
 */
public final class MappingScanBuilder {

    private final Snapshot snapshot;
    private final Supplier<AlgBuilder> builderFactory;


    public MappingScanBuilder( Snapshot snapshot, Supplier<AlgBuilder> builderFactory ) {
        this.snapshot = snapshot;
        this.builderFactory = builderFactory;
    }


    public AlgNode build( AllocationEntity target ) {
        var branches = mappedBranches( target );
        if ( branches.isEmpty() ) {
            var adapter = AdapterManager.getInstance().getAdapter( target.adapterId ).orElseThrow();
            return createPhysicalScan( adapter, target );
        }
        return branches.size() == 1 ? branches.get( 0 ) : combineScans( branches, target.getTupleType() );
    }


    /**
     * Builds one branch per mapping attached to the target.
     */
    public List<LogicalRelMappingTransform> mappedBranches( AllocationEntity target ) {
        // A branch is one path in the query plan that reads a physical source and applies its mapping to produce rows in the logical targets schema.
        // Find all mappings for the target
        var mappings = snapshot.mappings().getMappingsForLogicalEntity( target.logicalId );
        // One mapping or non
        // When there are multiple mappings, build one branch per mapping
        List<LogicalRelMappingTransform> branches = new ArrayList<>();
        for ( SchemaMapping mapping : mappings ) {
            // Find source allocation
            AllocationEntity source = resolveAllocation( mapping, target, mappings.size() == 1 );
            branches.add( buildMappedScan( mapping, source, target ) );
        }
        return branches;
    }


    private AllocationEntity resolveAllocation( SchemaMapping mapping, AllocationEntity original, boolean allowFallback ) {
        if ( mapping.sourceAllocationId() == null && allowFallback ) {
            return original;
        }
        return snapshot.alloc().getFromLogical( original.logicalId ).stream()
                .filter( allocation -> Objects.equals( mapping.sourceAllocationId(), allocation.id ) ).findFirst()
                .orElseThrow( () -> MappingError.Missing.sourceAllocation( mapping.id(), mapping.sourceAllocationId() ) );
    }


    private LogicalRelMappingTransform buildMappedScan( SchemaMapping mapping, AllocationEntity source, AllocationEntity target ) {
        // Obtain source adapter physical scan
        var adapter = AdapterManager.getInstance().getAdapter( source.adapterId )
                .orElseThrow( () -> MappingError.Missing.adapter( source.adapterId ) );
        AlgNode input;
        try {
            input = createPhysicalScan( adapter, source );
        } catch ( RuntimeException e ) {
            if ( MappingException.findCause( e ).isPresent() ) {
                throw e;
            }
            throw MappingError.Operation.scanFailed( mapping.name(), source.adapterId, e );
        }
        // transform() attaches the mapping to the plan
        return LogicalRelMappingTransform.create( input, mapping,
                PhysicalMappingBinding.create( mapping, source, adapter.getCatalog() ), target.getTupleType() );
    }


    private AlgNode createPhysicalScan( Adapter<?> adapter, AllocationEntity source ) {
        return switch ( source.dataModel ) {
            case RELATIONAL -> adapter.getRelScan( source.id, builderFactory.get() );
            case DOCUMENT -> adapter.getDocumentScan( source.id, builderFactory.get() );
            case GRAPH -> adapter.getGraphScan( source.id, builderFactory.get() );
        };
    }


    private AlgNode combineScans( List<? extends AlgNode> branches, AlgDataType targetType ) {
        AlgBuilder union = builderFactory.get();
        branches.forEach( union::push );
        // Append the branches using UNION ALL
        AlgNode alg = union.union( true, branches.size() ).build();
        // Set operations derive a common row type without column IDs. Restore the logical target identity.
        List<RexNode> fields = new ArrayList<>();
        for ( int i = 0; i < alg.getTupleType().getFieldCount(); i++ ) {
            fields.add( new RexIndexRef( i, alg.getTupleType().getFields().get( i ).getType() ) );
        }
        return LogicalRelProject.create( alg, fields, targetType );
    }

}

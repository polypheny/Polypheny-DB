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
import javax.annotation.Nullable;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
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
        // Find all mappings for the target
        var mappings = snapshot.mappings().getMappingsForLogicalEntity( target.logicalId );
        // One mapping or non
        if ( mappings.isEmpty() ) {
            return buildMappedScan( null, target, target );
        }

        // When there are multiple mappings, build one branch per mapping
        List<AlgNode> branches = new ArrayList<>();
        for ( SchemaMapping mapping : mappings ) {
            // Find source allocation
            AllocationEntity source = resolveAllocation( mapping, target, mappings.size() == 1 );
            branches.add( buildMappedScan( mapping, source, target ) );
        }
        return branches.size() == 1 ? branches.get( 0 ) : combineScans( branches, target.getTupleType() );
    }


    private AllocationEntity resolveAllocation( SchemaMapping mapping, AllocationEntity original, boolean allowFallback ) {
        if ( mapping.sourceAllocationId() == null && allowFallback ) {
            return original;
        }
        return snapshot.alloc().getFromLogical( original.logicalId ).stream()
                .filter( allocation -> Objects.equals( mapping.sourceAllocationId(), allocation.id ) ).findFirst().orElseThrow();
    }


    private AlgNode buildMappedScan( @Nullable SchemaMapping mapping, AllocationEntity source, AllocationEntity target ) {
        // Obtain source adapter physical scan
        var adapter = AdapterManager.getInstance().getAdapter( source.adapterId ).orElseThrow();
        AlgNode input = createPhysicalScan( adapter, source );
        if ( mapping == null ) {
            return input;
        }
        // transform() attaches the mapping to the plan
        return builderFactory.get().push( input ).transform( mapping,
                PhysicalMappingBinding.create( mapping, source, adapter.getCatalog() ), target.getTupleType() ).build();
    }


    private AlgNode createPhysicalScan( Adapter<?> adapter, AllocationEntity source ) {
        return switch ( source.dataModel ) {
            case RELATIONAL -> adapter.getRelScan( source.id, builderFactory.get() );
            case DOCUMENT -> adapter.getDocumentScan( source.id, builderFactory.get() );
            case GRAPH -> adapter.getGraphScan( source.id, builderFactory.get() );
        };
    }


    private AlgNode combineScans( List<AlgNode> branches, AlgDataType targetType ) {
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

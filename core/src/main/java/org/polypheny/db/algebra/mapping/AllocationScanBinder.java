/*
 * Copyright 2019-2024 The Polypheny Project
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
import java.util.function.Supplier;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.mapping.SchemaMapping;
import org.polypheny.db.catalog.entity.mapping.SchemaMappingManager;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.MappingKind;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.tools.AlgBuilder;


public final class AllocationScanBinder {

    private AllocationScanBinder() {
    }


    public static AlgNode bindScan( Scan<?> scan, AllocationEntity allocation, Supplier<AlgBuilder> builder ) {
        return switch ( scan.getEntity().dataModel ) {
            case RELATIONAL -> handleRelationalEntity( scan, allocation, builder );
            case DOCUMENT -> handleDocumentEntity( scan, allocation, builder );
            case GRAPH -> handleGraphEntity( scan, allocation, builder );
        };
    }


    public static AlgNode expandMappedScans( AlgNode allocationPlan ) {
        // The allocation plan is captured before the physical scan rule binds mappings.
        return allocationPlan.accept( new AlgShuttleImpl() {
            @Override
            public AlgNode visit( LogicalRelScan scan ) {
                return expandMappedScan( scan );
            }


            @Override
            public AlgNode visit( LogicalDocumentScan scan ) {
                return expandMappedScan( scan );
            }


            @Override
            public AlgNode visit( LogicalLpgScan scan ) {
                return expandMappedScan( scan );
            }
        } );
    }


    private static AlgNode expandMappedScan( Scan<?> scan ) {
        if ( !(scan.getEntity() instanceof AllocationEntity allocation) ) {
            return scan;
        }

        var cluster = scan.getCluster();
        var snapshot = cluster.getSnapshot();
        DataModel scanModel = scan.getModel();
        boolean showMapping = snapshot.mappings().getMappingsForLogicalEntity( allocation.logicalId ).stream()
                .anyMatch( mapping -> supportsMappingExpansion( snapshot, mapping, allocation.dataModel, scanModel ) );
        if ( showMapping ) {
            return bindScan( scan, allocation, () -> AlgFactories.LOGICAL_BUILDER.create( cluster, snapshot ) );
        }
        return scan;
    }


    private static boolean supportsMappingExpansion( Snapshot snapshot, SchemaMapping mapping, DataModel allocationModel, DataModel scanModel ) {
        return mapping.kind() == MappingKind.PHYSICAL_TO_LOGICAL
                && mapping.targetModel() == allocationModel
                && mapping.targetModel() == scanModel
                && mapping.sourceModel() == DataModel.RELATIONAL
                && mapping.targetModel() == DataModel.RELATIONAL
                && SchemaMappingManager.usesColumnTransformations( snapshot, mapping );
    }


    /**
     * Attach mapping to the query plan
     * For shared target read all contributing sources
     */
    private static AlgNode handleRelationalEntity( Scan<?> scan, AllocationEntity alloc, Supplier<AlgBuilder> builder ) {
        AlgNode alg = new MappingScanBuilder( scan.getCluster().getSnapshot(), builder ).build( alloc );
        if ( scan.getModel() != scan.getEntity().dataModel ) {
            // cross-model queries need a transformer first, we let another rule handle that
            return builder.get().push( alg ).transform( scan.getTraitSet().getTrait( ModelTraitDef.INSTANCE ), scan.getTupleType(), true, null ).build();
        }
        if ( scan.getTupleType().getFieldCount() != alg.getTupleType().getFieldCount() ) {
            alg = reduce( alg, scan, builder.get() );
        }
        return attachReorder( alg, scan, builder.get() );
    }


    private static AlgNode handleDocumentEntity( Scan<?> scan, AllocationEntity alloc, Supplier<AlgBuilder> builder ) {
        AlgNode alg = new MappingScanBuilder( scan.getCluster().getSnapshot(), builder ).build( alloc );
        if ( scan.getModel() != scan.getEntity().dataModel ) {
            // cross-model queries need a transformer first, we let another rule handle that
            return builder.get().push( alg ).transform( scan.getTraitSet().getTrait( ModelTraitDef.INSTANCE ), scan.getTupleType(), true, null ).build();
        }
        return alg;
    }


    private static AlgNode handleGraphEntity( Scan<?> scan, AllocationEntity alloc, Supplier<AlgBuilder> builder ) {
        AlgNode alg = AdapterManager.getInstance().getAdapter( alloc.adapterId ).orElseThrow().getGraphScan( alloc.id, builder.get() );

        if ( scan.getModel() != scan.getEntity().dataModel ) {
            // cross-model queries need a transformer first, we let another rule handle that
            return builder.get().push( alg ).transform( scan.getTraitSet().getTrait( ModelTraitDef.INSTANCE ), scan.getTupleType(), true, alloc.name ).build();
        }
        return alg;
    }


    private static AlgNode reduce( AlgNode current, Scan<?> scan, AlgBuilder builder ) {
        builder.push( current );

        List<RexNode> projects = new ArrayList<>();
        for ( AlgDataTypeField field : scan.getTupleType().getFields() ) {
            if ( current.getTupleType().getField( field.getName(), true, false ) != null ) {
                projects.add( builder.field( field.getName() ) );
            }
        }
        return builder.project( projects ).build();
    }


    private static AlgNode attachReorder( AlgNode newAlg, Scan<?> original, AlgBuilder builder ) {
        if ( newAlg.getTupleType().equals( original.getTupleType() ) ) {
            return newAlg;
        }
        builder.push( newAlg );
        AlgDataType originalType = original.getTupleType();
        builder.reorder( originalType );
        return builder.build();
    }

}

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

package org.polypheny.db.algebra.rules;

import java.util.Optional;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.tools.AlgBuilder;

public class AllocationToPhysicalScanRule extends AlgOptRule {

    public static final AllocationToPhysicalScanRule REL_INSTANCE = new AllocationToPhysicalScanRule( LogicalRelScan.class );
    public static final AllocationToPhysicalScanRule DOC_INSTANCE = new AllocationToPhysicalScanRule( LogicalDocumentScan.class );
    public static final AllocationToPhysicalScanRule GRAPH_INSTANCE = new AllocationToPhysicalScanRule( LogicalLpgScan.class );


    public AllocationToPhysicalScanRule( Class<? extends Scan<?>> scan ) {
        super( operand( scan, any() ), AlgFactories.LOGICAL_BUILDER, AllocationToPhysicalScanRule.class.getSimpleName() + "_" + scan.getSimpleName() );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        Scan<?> scan = call.alg( 0 );
        Optional<AllocationEntity> oAlloc = scan.entity.unwrap( AllocationEntity.class );
        if ( oAlloc.isEmpty() ) {
            return;
        }

        AlgNode newAlg = switch ( scan.entity.dataModel ) {
            case RELATIONAL -> handleRelationalEntity( call, scan, oAlloc.get() );
            case DOCUMENT -> handleDocumentEntity( call, scan, oAlloc.get() );
            case GRAPH -> handleGraphEntity( call, scan, oAlloc.get() );
        };

        call.transformTo( newAlg );
    }


    private static AlgNode handleGraphEntity( AlgOptRuleCall call, Scan<?> scan, AllocationEntity alloc ) {
        AlgNode alg = AdapterManager.getInstance().getAdapter( alloc.adapterId ).orElseThrow().getGraphScan( alloc.id, call.builder() );

        if ( scan.getModel() != scan.entity.dataModel ) {
            // cross-model queries need a transformer first, we let another rule handle that
            AlgBuilder builder = call.builder().push( alg );

            alg = builder.transform( scan.getTraitSet().getTrait( ModelTraitDef.INSTANCE ), scan.getTupleType(), true, alloc.name ).build();

        }
        return alg;
    }


    private static AlgNode handleDocumentEntity( AlgOptRuleCall call, Scan<?> scan, AllocationEntity alloc ) {
        AlgNode alg = AdapterManager.getInstance().getAdapter( alloc.adapterId ).orElseThrow().getDocumentScan( alloc.id, call.builder() );

        if ( scan.getModel() != scan.entity.dataModel ) {
            // cross-model queries need a transformer first, we let another rule handle that
            alg = call.builder().push( alg ).transform( scan.getTraitSet().getTrait( ModelTraitDef.INSTANCE ), scan.getTupleType(), true, null ).build();
        }
        return alg;
    }


    private AlgNode handleRelationalEntity( AlgOptRuleCall call, Scan<?> scan, AllocationEntity alloc ) {
        AlgNode alg = AdapterManager.getInstance().getAdapter( alloc.adapterId ).orElseThrow().getRelScan( alloc.id, call.builder() );
        if ( scan.getModel() == scan.entity.dataModel ) {
            alg = attachReorder( alg, scan, call.builder() );
        }

        if ( scan.getModel() != scan.entity.dataModel ) {
            // cross-model queries need a transformer first, we let another rule handle that
            alg = call.builder().push( alg ).transform( scan.getTraitSet().getTrait( ModelTraitDef.INSTANCE ), scan.getTupleType(), true, null ).build();
        }
        return alg;
    }


    private AlgNode attachReorder( AlgNode newAlg, Scan<?> original, AlgBuilder builder ) {
        if ( newAlg.getTupleType().equals( original.getTupleType() ) ) {
            return newAlg;
        }
        builder.push( newAlg );
        AlgDataType originalType = original.getTupleType();
        builder.reorder( originalType );
        return builder.build();
    }

}

/*
 * Copyright 2019-2023 The Polypheny Project
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

import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;

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
        AllocationEntity alloc = scan.entity.unwrap( AllocationEntity.class );
        if ( alloc == null ) {
            return;
        }

        AlgNode newAlg = AdapterManager.getInstance().getAdapter( alloc.adapterId ).getScan( alloc.id, call.builder() );
        call.transformTo( newAlg );
    }

}

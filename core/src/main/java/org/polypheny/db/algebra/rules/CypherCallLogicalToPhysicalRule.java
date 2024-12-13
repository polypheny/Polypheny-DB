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

import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.NeoProcedureProvider;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.lpg.LpgCall;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgCall;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.Convention;

public class CypherCallLogicalToPhysicalRule extends AlgOptRule {

    public static final CypherCallLogicalToPhysicalRule INSTANCE = new CypherCallLogicalToPhysicalRule( LogicalLpgCall.class );

    public CypherCallLogicalToPhysicalRule( Class<? extends LpgCall> callAlg ) {
        super( operand( callAlg, Convention.NONE, r -> true, any() ), AlgFactories.LOGICAL_BUILDER, callAlg.getSimpleName() + "ToPhysical" );
    }

    @Override
    public void onMatch( AlgOptRuleCall call ) {
        LpgCall oldAlg = call.alg( 0 );
        Adapter neo = null;
        for ( Adapter a :  AdapterManager.getInstance().getStores().values() ) {
            if ( a.adapterName.contains( "Neo" ) ) {
                neo = a;
            }
        }
        if ( neo == null ) {
            throw new RuntimeException( "No neo adapter found" );
        }
        AlgNode newAlg = oldAlg;
        newAlg.getTraitSet().replace( ((NeoProcedureProvider)neo).getConvention(call.getPlanner()) );
        ((LpgCall)newAlg).setProcedureProvider( ((NeoProcedureProvider)neo).getStore() );
        //AlgNode newAlg = ((NeoProcedureProvider)neo).getCall( oldAlg.getCluster(), oldAlg.getTraitSet(), oldAlg.getNamespace(), oldAlg.getProcedureName(), oldAlg.getArguments() );
        if ( newAlg != null ) {
            call.transformTo( newAlg );
        }
    }

}

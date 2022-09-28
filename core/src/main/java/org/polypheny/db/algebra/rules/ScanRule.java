/*
 * Copyright 2019-2022 The Polypheny Project
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


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that converts a {@link LogicalScan} to the result of calling {@link AlgOptTable#toAlg}.
 */
public class ScanRule extends AlgOptRule {

    public static final ScanRule INSTANCE = new ScanRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a ScanRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public ScanRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( LogicalScan.class, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final LogicalScan oldAlg = call.alg( 0 );
        AlgNode newAlg = oldAlg.getTable().toAlg( oldAlg::getCluster );
        call.transformTo( newAlg );
    }

}

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


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.catalog.refactor.TranslatableEntity;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptEntity;
import org.polypheny.db.plan.AlgOptEntity.ToAlgContext;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that converts a {@link LogicalRelScan} to the result of calling {@link AlgOptEntity#toAlg}.
 */
public class ScanRule extends AlgOptRule {

    public static final ScanRule INSTANCE = new ScanRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a ScanRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public ScanRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( LogicalRelScan.class, any() ), algBuilderFactory, null );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final LogicalRelScan oldAlg = call.alg( 0 );
        AlgNode newAlg = oldAlg.getEntity().unwrap( TranslatableEntity.class ).toAlg( oldAlg::getCluster, oldAlg.getTraitSet() );
        call.transformTo( newAlg );
    }

}

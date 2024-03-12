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


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that converts a logical {@link Scan} to the result of calling {@link TranslatableEntity#toAlg(AlgCluster, AlgTraitSet)}.
 */
public class ScanRule extends AlgOptRule {

    public static final ScanRule INSTANCE = new ScanRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates a ScanRule.
     *
     * @param algBuilderFactory Builder for algebra expressions
     */
    public ScanRule( AlgBuilderFactory algBuilderFactory ) {
        super( operand( Scan.class, Convention.NONE, r -> true, any() ), algBuilderFactory, ScanRule.class.getSimpleName() );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Scan<?> oldAlg = call.alg( 0 );
        if ( oldAlg.getEntity().unwrap( TranslatableEntity.class ).isEmpty() ) {
            return;
        }
        AlgNode newAlg = oldAlg.getEntity().unwrap( TranslatableEntity.class ).get().toAlg( oldAlg.getCluster(), oldAlg.getTraitSet() );
        call.transformTo( newAlg );
    }


}

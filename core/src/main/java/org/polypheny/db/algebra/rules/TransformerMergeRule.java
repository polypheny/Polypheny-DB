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

import org.polypheny.db.algebra.logical.LogicalTransformer;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;

public class TransformerMergeRule extends AlgOptRule {

    public static TransformerMergeRule INSTANCE = new TransformerMergeRule();


    public TransformerMergeRule() {
        super( operand( LogicalTransformer.class, operand( LogicalTransformer.class, any() ) ) );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        LogicalTransformer parent = call.alg( 0 );
        LogicalTransformer child = call.alg( 1 );

        LogicalTransformer merged = LogicalTransformer.merge( parent, child );

        if ( merged != null ) {
            call.transformTo( merged );
        }
    }

}

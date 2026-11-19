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

package org.polypheny.db.algebra.enumerable;


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;


/**
 * Identifies the surviving conversions after the physical Calc rules have finished.
 * */
public final class EnumerableMappingConvertRule extends AlgOptRule {

    public static final EnumerableMappingConvertRule INSTANCE = new EnumerableMappingConvertRule();


    private EnumerableMappingConvertRule() {
        super( operand( EnumerableCalc.class, null,
                calc -> !(calc instanceof EnumerableMappingConvert)
                        && calc.getProgram().getExprList().stream().anyMatch( expression -> expression.isA( Kind.MAPPING_CONVERT ) ),
                any() ), AlgFactories.LOGICAL_BUILDER, "EnumerableMappingConvertRule" );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        EnumerableCalc calc = call.alg( 0 );
        call.transformTo( new EnumerableMappingConvert( calc.getTraitSet(), calc.getInput(), calc.getProgram() ) );
    }

}

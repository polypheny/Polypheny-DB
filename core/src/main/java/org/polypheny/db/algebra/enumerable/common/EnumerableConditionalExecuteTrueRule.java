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

package org.polypheny.db.algebra.enumerable.common;


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.common.ConditionalExecute.Condition;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.Convention;


public class EnumerableConditionalExecuteTrueRule extends ConverterRule {

    public EnumerableConditionalExecuteTrueRule() {
        super( LogicalConditionalExecute.class,
                lce -> lce.getCondition() == Condition.TRUE,
                Convention.NONE, EnumerableConvention.INSTANCE,
                AlgFactories.LOGICAL_BUILDER, "EnumerableConditionalExecuteTrueRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final LogicalConditionalExecute lce = (LogicalConditionalExecute) alg;
        return AlgOptRule.convert( lce.getRight(), lce.getRight().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
    }

}

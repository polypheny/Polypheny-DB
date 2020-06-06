/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.enumerable;


import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterRule;
import org.polypheny.db.rel.core.ConditionalExecute.Condition;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.logical.LogicalConditionalExecute;


public class EnumerableConditionalExecuteTrueRule extends ConverterRule {

    public EnumerableConditionalExecuteTrueRule() {
        super( LogicalConditionalExecute.class,
                lce -> lce.getCondition() == Condition.TRUE,
                Convention.NONE, EnumerableConvention.INSTANCE,
                RelFactories.LOGICAL_BUILDER, "EnumerableConditionalExecuteTrueRule" );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        final LogicalConditionalExecute lce = (LogicalConditionalExecute) rel;
        return RelOptRule.convert( lce.getRight(), lce.getRight().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
    }
}
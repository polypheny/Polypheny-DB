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
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.logical.LogicalConditionalExecute;


public class EnumerableConditionalExecuteRule extends ConverterRule {

    public EnumerableConditionalExecuteRule() {
        super( LogicalConditionalExecute.class,
                operand -> true,
                Convention.NONE, EnumerableConvention.INSTANCE,
                RelFactories.LOGICAL_BUILDER, "EnumerableConditionalExecuteRule" );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        final LogicalConditionalExecute lce = (LogicalConditionalExecute) rel;
        final RelNode input = RelOptRule.convert( lce.getLeft(), lce.getLeft().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        final RelNode action = RelOptRule.convert( lce.getRight(), lce.getRight().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        final EnumerableConditionalExecute ece =  EnumerableConditionalExecute.create( input, action, lce.getCondition(), lce.getExceptionClass(), lce.getExceptionMessage() );
        ece.setCheckDescription( lce.getCheckDescription() );
        ece.setCatalogSchema( lce.getCatalogSchema() );
        ece.setCatalogTable( lce.getCatalogTable() );
        ece.setCatalogColumns( lce.getCatalogColumns() );
        ece.setValues( lce.getValues() );
        return ece;
    }
}
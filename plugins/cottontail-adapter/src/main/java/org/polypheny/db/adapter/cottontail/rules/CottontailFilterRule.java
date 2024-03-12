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

package org.polypheny.db.adapter.cottontail.rules;


import org.polypheny.db.adapter.cottontail.CottontailConvention;
import org.polypheny.db.adapter.cottontail.algebra.CottontailFilter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.document.DocumentRules;
import org.polypheny.db.tools.AlgBuilderFactory;


public class CottontailFilterRule extends CottontailConverterRule {

    CottontailFilterRule( AlgBuilderFactory algBuilderFactory ) {
        super( Filter.class, r -> !DocumentRules.containsDocument( r ), Convention.NONE, CottontailConvention.INSTANCE, algBuilderFactory, "CottontailFilterRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        Filter filter = (Filter) alg;
        final AlgTraitSet traitSet = filter.getTraitSet().replace( out );

        return new CottontailFilter(
                filter.getCluster(),
                traitSet,
                convert( filter.getInput(), filter.getInput().getTraitSet().replace( out ) ),
                filter.getCondition() );
    }


    @Override
    public boolean matches( AlgOptRuleCall call ) {
        Filter filter = call.alg( 0 );
        RexNode condition = filter.getCondition();

        return this.isValidCondition( condition );
    }


    private boolean isValidCondition( RexNode condition ) {
        switch ( condition.getKind() ) {
            case AND:
            case OR:
            case NOT: {
                boolean allOperandsOkay = true;
                for ( RexNode operand : ((RexCall) condition).getOperands() ) {
                    allOperandsOkay = allOperandsOkay && isValidCondition( operand );
                }
                return allOperandsOkay;
            }
            case EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case IS_NULL:
            case IS_NOT_NULL:
                return checkAtomicCondition( condition );
            // no it's not translatable
            case LIKE:
            case IN:
            default:
                return false;
        }
    }


    private boolean checkAtomicCondition( RexNode condition ) {
        switch ( condition.getKind() ) {
            case EQUALS:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL: {
                RexCall call = (RexCall) condition;
                final RexNode left = call.operands.get( 0 );
                final RexNode right = call.operands.get( 1 );

                return checkAtomicConditionArguments( left, right ) || checkAtomicConditionArguments( right, left );
            }
            default:
                return false;
        }
    }


    private boolean checkAtomicConditionArguments( RexNode left, RexNode right ) {
        switch ( right.getKind() ) {
            case LITERAL:
            case DYNAMIC_PARAM:
            case ARRAY_VALUE_CONSTRUCTOR:
                break;
            default:
                return false;
        }
        return left.getKind() == Kind.INPUT_REF;
    }

}

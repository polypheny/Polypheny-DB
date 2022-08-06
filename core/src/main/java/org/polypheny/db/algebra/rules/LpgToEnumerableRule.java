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

import org.polypheny.db.adapter.enumerable.EnumerableAggregate;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableFilter;
import org.polypheny.db.adapter.enumerable.EnumerableLimit;
import org.polypheny.db.adapter.enumerable.EnumerableProject;
import org.polypheny.db.adapter.enumerable.EnumerableSort;
import org.polypheny.db.adapter.enumerable.EnumerableValues;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgTraitSet;

public class LpgToEnumerableRule extends AlgOptRule {

    public static LpgToEnumerableRule PROJECT_TO_ENUMERABLE = new LpgToEnumerableRule( Type.PROJECT, operand( LogicalLpgProject.class, any() ), "GRAPH_PROJECT_TO_ENUMERABLE" );

    public static LpgToEnumerableRule FILTER_TO_ENUMERABLE = new LpgToEnumerableRule( Type.FILTER, operand( LogicalLpgFilter.class, any() ), "GRAPH_FILTER_TO_ENUMERABLE" );

    public static LpgToEnumerableRule AGGREGATE_TO_ENUMERABLE = new LpgToEnumerableRule( Type.AGGREGATE, operand( LogicalLpgAggregate.class, any() ), "GRAPH_AGGREGATE_TO_ENUMERABLE" );

    public static LpgToEnumerableRule VALUES_TO_ENUMERABLE = new LpgToEnumerableRule( Type.VALUES, operand( LogicalLpgValues.class, none() ), "GRAPH_VALUES_TO_ENUMERABLE" );

    //public static GraphToEnumerableRule SORT_TO_ENUMERABLE = new GraphToEnumerableRule( Type.SORT, operand( LogicalGraphSort.class, any() ), "GRAPH_SORT_TO_ENUMERABLE" );

    private final Type type;


    public LpgToEnumerableRule( Type type, AlgOptRuleOperand operand, String description ) {
        super( operand, AlgFactories.LOGICAL_BUILDER, description );
        this.type = type;
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        if ( type == Type.PROJECT ) {
            convertProject( call );
        } else if ( type == Type.FILTER ) {
            convertFilter( call );
        } else if ( type == Type.AGGREGATE ) {
            convertAggregate( call );
        } else if ( type == Type.VALUES ) {
            convertValues( call );
        }
    }


    private void convertValues( AlgOptRuleCall call ) {
        LogicalLpgValues values = call.alg( 0 );
        if ( !values.isEmptyGraphValues() ) {
            return;
        }

        AlgNode node = EnumerableValues.create( values.getCluster(), values.getRowType(), values.getValues() );
        call.transformTo( node );
    }


    private void convertAggregate( AlgOptRuleCall call ) {
        LogicalLpgAggregate aggregate = call.alg( 0 );
        AlgTraitSet out = aggregate.getTraitSet().replace( EnumerableConvention.INSTANCE );

        try {
            AlgNode node = new EnumerableAggregate( aggregate.getCluster(), out, convert( aggregate.getInput(), EnumerableConvention.INSTANCE ), aggregate.indicator, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList() );
            call.transformTo( node );
        } catch ( InvalidAlgException e ) {
            //EnumerableRules.LOGGER.debug( e.toString() );
            throw new RuntimeException( e );
        }
    }


    private void convertSort( AlgOptRuleCall call ) {
        LogicalLpgSort sort = call.alg( 0 );
        //AlgTraitSet out = sort.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode input = AlgOptRule.convert( sort.getInput(), EnumerableConvention.INSTANCE );

        AlgNode node;
        if ( sort.getCollation().getFieldCollations().isEmpty() ) {
            node = EnumerableLimit.create( input, sort.getRexSkip(), sort.getRexLimit() );
        } else {
            node = EnumerableSort.create( input, sort.getCollation(), sort.getRexSkip(), sort.getRexLimit() );
        }

        call.transformTo( node );
    }


    private void convertFilter( AlgOptRuleCall call ) {
        LogicalLpgFilter filter = call.alg( 0 );
        AlgTraitSet out = filter.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode input = AlgOptRule.convert( filter.getInput(), EnumerableConvention.INSTANCE );

        EnumerableFilter enumerable = new EnumerableFilter( filter.getCluster(), out, input, filter.getCondition() );
        call.transformTo( enumerable );
    }


    private void convertProject( AlgOptRuleCall call ) {
        LogicalLpgProject project = call.alg( 0 );
        AlgTraitSet out = project.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode input = AlgOptRule.convert( project.getInput(), EnumerableConvention.INSTANCE );

        EnumerableProject enumerableProject = new EnumerableProject( project.getCluster(), out, input, project.getProjects(), project.getRowType() );
        call.transformTo( enumerableProject );
    }


    private enum Type {
        PROJECT,
        FILTER,
        AGGREGATE,
        VALUES, SORT
    }

}

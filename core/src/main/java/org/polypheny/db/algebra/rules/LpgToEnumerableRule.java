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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.EnumerableFilter;
import org.polypheny.db.algebra.enumerable.EnumerableLimit;
import org.polypheny.db.algebra.enumerable.EnumerableProject;
import org.polypheny.db.algebra.enumerable.EnumerableSort;
import org.polypheny.db.algebra.enumerable.EnumerableValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.ImmutableBitSet;

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

        AlgNode node = EnumerableValues.create( values.getCluster(), values.getTupleType(), values.getValues() );
        call.transformTo( node );
    }


    private void convertAggregate( AlgOptRuleCall call ) {
        LogicalLpgAggregate aggregate = call.alg( 0 );
        AlgTraitSet out = aggregate.getTraitSet().replace( EnumerableConvention.INSTANCE );

        try {
            AlgNode convert = convert( aggregate.getInput(), EnumerableConvention.INSTANCE );

            convert = wrapAggregate( call.builder(), aggregate, convert );

            call.transformTo( convert );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
    }


    private AlgNode wrapAggregate( AlgBuilder builder, LogicalLpgAggregate alg, AlgNode child ) {
        List<RexNode> nodes = new ArrayList<>();
        List<String> names = new ArrayList<>();

        List<Integer> groupIndexes = new ArrayList<>();
        for ( RexNameRef group : alg.groups ) {
            if ( group.getIndex().isEmpty() ) {
                throw new UnsupportedOperationException();
            }
            groupIndexes.add( group.getIndex().get() );
            RexNode node = new RexIndexRef( group.getIndex().get(), group.type );
            nodes.add( node );
            names.add( group.name );
        }
        ImmutableBitSet groupSet = ImmutableBitSet.of( groupIndexes );

        for ( LaxAggregateCall agg : alg.aggCalls ) {
            if ( agg.getInput().isEmpty() ) {
                nodes.add( new RexIndexRef( 0, child.getTupleType().getFields().get( 0 ).getType() ) );
                names.add( agg.name );
                continue;
            }

            RexNode node = agg.getInput().get();

            if ( agg.requiresCast( alg.getCluster() ).isPresent() ) {
                node = builder.getRexBuilder().makeAbstractCast( agg.requiresCast( alg.getCluster() ).get(), node );
            }

            nodes.add( node );
            names.add( agg.name );
        }

        LogicalRelProject project = (LogicalRelProject) LogicalRelProject.create( child, nodes, names ).copy( alg.getInput().getTraitSet().replace( ModelTrait.GRAPH ), alg.getInputs() );

        EnumerableProject enumerableProject = new EnumerableProject( project.getCluster(), alg.getInput().getTraitSet().replace( ModelTrait.GRAPH ).replace( EnumerableConvention.INSTANCE ), convert( project.getInput(), EnumerableConvention.INSTANCE ), project.getProjects(), project.getTupleType() );

        builder.push( enumerableProject );
        builder.push( LogicalRelAggregate.create( builder.build(), groupSet, null, alg.aggCalls.stream().map( a -> a.toAggCall( alg.getTupleType(), alg.getCluster() ) ).collect( Collectors.toList() ) ) );

        builder.rename( names );

        return builder.build();
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

        EnumerableProject enumerableProject = new EnumerableProject( project.getCluster(), out, input, project.getProjects(), project.getTupleType() );
        call.transformTo( enumerableProject );
    }


    private enum Type {
        PROJECT,
        FILTER,
        AGGREGATE,
        VALUES, SORT
    }

}

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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.AggregateCall;
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
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptRuleOperand;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.ValidatorUtil;

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

        for ( RexNameRef group : alg.groups ) {
            if ( group.getIndex().isEmpty() ) {
                throw new UnsupportedOperationException();
            }
            RexNode node = new RexIndexRef( group.getIndex().get(), group.type );
            nodes.add( node );
            names.add( group.name );
        }
        ImmutableBitSet groupSet = ImmutableBitSet.range( alg.groups.size() );

        List<AggregateCall> calls = new ArrayList<>();
        for ( LaxAggregateCall agg : alg.aggCalls ) {
            if ( agg.getInput().isEmpty() ) {
                calls.add( agg.toAggCall( List.of(), alg.getCluster() ) );
                continue;
            }

            RexNode node = agg.getInput().get();

            if ( agg.requiresCast( alg.getCluster() ).isPresent() ) {
                node = builder.getRexBuilder().makeAbstractCast( agg.requiresCast( alg.getCluster() ).get(), node );
            }

            calls.add( agg.toAggCall( List.of( nodes.size() ), alg.getCluster() ) );
            nodes.add( node );
            names.add( agg.name );
        }

        EnumerableProject enumerableProject = new EnumerableProject(
                alg.getCluster(), child.getTraitSet().replace( ModelTrait.GRAPH ).replace( EnumerableConvention.INSTANCE ),
                child, nodes, RexUtil.createStructType( alg.getCluster().getTypeFactory(), nodes, names, ValidatorUtil.EXPR_SUGGESTER ) );

        builder.push( enumerableProject );
        builder.push( LogicalRelAggregate.create( builder.build(), groupSet, null, calls ) );
        builder.rename( alg.getTupleType().getFieldNames() );

        return builder.build();
    }


    private void convertSort( AlgOptRuleCall call ) {
        LogicalLpgSort sort = call.alg( 0 );
        AlgNode input = AlgOptRule.convert( sort.getInput(), EnumerableConvention.INSTANCE );
        if ( !sort.getCollation().getFieldCollations().isEmpty() ) {
            input = EnumerableSort.create( input, sort.getCollation(), null, null );
        }
        if ( sort.offset != null || sort.fetch != null ) {
            input = new EnumerableLimit( sort.getCluster(), input.getTraitSet(), input, sort.offset, sort.fetch );
        }
        call.transformTo( input );
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

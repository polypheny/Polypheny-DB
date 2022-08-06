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

package org.polypheny.db.adapter.neo4j.rules.graph;

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.with_;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.adapter.neo4j.util.NeoStatements;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.OperatorStatement;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.lpg.LpgAggregate;
import org.polypheny.db.algebra.core.lpg.LpgProject;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.ImmutableBitSet;

public class NeoLpgAggregate extends LpgAggregate implements NeoGraphAlg {

    /**
     * Creates a {@link org.polypheny.db.adapter.neo4j.NeoConvention} of a {@link LpgAggregate}.
     *
     * @param cluster Cluster this expression belongs to
     * @param traits Traits active for this node, including {@link org.polypheny.db.schema.ModelTrait#GRAPH}
     */
    public NeoLpgAggregate( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        super( cluster, traits, child, indicator, groupSet, groupSets, aggCalls );
    }


    @Override
    public Aggregate copy( AlgTraitSet traitSet, AlgNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        return new NeoLpgAggregate( input.getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.2 );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {
        implementor.visitChild( 0, getInput() );
        if ( implementor.getLast() instanceof LpgProject ) {
            OperatorStatement last = implementor.removeLast();
            List<String> finalRow = new ArrayList<>();
            for ( AlgDataTypeField ignored : getRowType().getFieldList() ) {
                finalRow.add( null );
            }

            List<String> lastNames = implementor.getLast().getRowType().getFieldNames();
            List<String> currentNames = getRowType().getFieldNames();

            for ( int index : groupSet.asSet() ) {
                String name = lastNames.get( index );
                finalRow.set( currentNames.indexOf( name ), name );
            }
            for ( AggregateCall agg : aggCalls ) {
                List<String> refs = agg.getArgList().stream().map( lastNames::get ).collect( Collectors.toList() );
                if ( refs.isEmpty() ) {
                    refs.add( "*" );
                }
                finalRow.set( currentNames.indexOf( agg.name ), Objects.requireNonNull( NeoUtil.getOpAsNeo( agg.getAggregation().getOperatorName(), List.of(), agg.type ) ).apply( refs ) );
            }

            implementor.add( with_( list_( finalRow.stream().map( NeoStatements::literal_ ).collect( Collectors.toList() ) ) ) );

        }

    }

}

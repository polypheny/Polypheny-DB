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

package org.polypheny.db.adapter.neo4j.rules.graph;

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.with_;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.OperatorStatement;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.core.lpg.LpgAggregate;
import org.polypheny.db.algebra.core.lpg.LpgProject;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.type.entity.PolyString;

public class NeoLpgAggregate extends LpgAggregate implements NeoGraphAlg {

    /**
     * Creates a {@link org.polypheny.db.adapter.neo4j.NeoConvention} of a {@link LpgAggregate}.
     *
     * @param cluster Cluster this expression belongs to
     * @param traits Traits active for this node, including {@link org.polypheny.db.catalog.logistic.DataModel#GRAPH}
     */
    public NeoLpgAggregate( AlgCluster cluster, AlgTraitSet traits, AlgNode child, @NotNull List<RexNameRef> groups, List<LaxAggregateCall> aggCalls, AlgDataType tupleType ) {
        super( cluster, traits, child, groups, aggCalls, tupleType );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new NeoLpgAggregate( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), groups, aggCalls, rowType );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.2 );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {
        implementor.visitChild( 0, getInput() );
        if ( implementor.getLast() instanceof LpgProject || implementor.getLast() instanceof NeoLpgMatch ) {
            OperatorStatement last = implementor.removeLast();
            List<String> finalRow = new ArrayList<>();
            for ( AlgDataTypeField ignored : getTupleType().getFields() ) {
                finalRow.add( null );
            }

            List<String> lastNames = implementor.getLast().getTupleType().getFieldNames();
            List<String> currentNames = getTupleType().getFieldNames();

            for ( RexNameRef name : groups ) {
                finalRow.set( currentNames.indexOf( name.name ), name.name );
            }
            for ( LaxAggregateCall agg : aggCalls ) {
                List<String> refs = new ArrayList<>();
                if ( agg.getInput().isEmpty() ) {
                    refs.add( "*" );
                } else {
                    refs.add( lastNames.get( ((RexIndexRef) agg.getInput().get()).getIndex() ) );
                }
                int i = currentNames.indexOf( agg.name );
                if ( i == -1 ) {
                    i = currentNames.indexOf( agg.function.getOperatorName().name() );
                }

                finalRow.set( i, Objects.requireNonNull( NeoUtil.getOpAsNeo( agg.function.getOperatorName(), List.of(), null ) ).apply( refs ) );
            }

            implementor.add( with_( list_( finalRow.stream().map( e -> literal_( PolyString.of( e ) ) ).toList() ) ) );

        }

    }

}

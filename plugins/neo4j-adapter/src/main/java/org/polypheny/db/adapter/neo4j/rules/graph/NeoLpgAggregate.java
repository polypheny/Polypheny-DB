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

package org.polypheny.db.adapter.neo4j.rules.graph;

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.as_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.with_;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntFunction;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.NeoStatement;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.adapter.neo4j.util.Translator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.core.lpg.LpgAggregate;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
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
        final IntFunction<String> inputExpression;
        if ( getInput() instanceof NeoLpgProject project ) {
            // Translate the grouping keys and arguments in the projection's input scope.
            implementor.visitChild( 0, project.getInput() );
            Translator translator = new Translator( project.getTupleType(), project.getInput().getTupleType(),
                    Map.of(), null, implementor.getGraph().mappingLabel, false );
            inputExpression = index -> project.getProjects().get( index ).accept( translator );
        } else {
            implementor.visitChild( 0, getInput() );
            inputExpression = getInput().getTupleType().getFieldNames()::get;
        }

        List<String> resultNames = getTupleType().getFieldNames();
        List<NeoStatement> result = new ArrayList<>( Collections.nCopies( resultNames.size(), null ) );
        for ( RexNameRef group : groups ) {
            result.set( resultNames.indexOf( group.name ), alias( inputExpression.apply( group.getIndex().orElseThrow() ), group.name ) );
        }
        for ( LaxAggregateCall aggregate : aggCalls ) {
            String argument = aggregate.getInput().map( ref -> inputExpression.apply( ((RexIndexRef) ref).getIndex() ) ).orElse( "*" );
            String expression = Objects.requireNonNull( NeoUtil.getOpAsNeo( aggregate.function.getOperatorName(), List.of(), null, List.of() ) )
                    .apply( List.of( argument ) );
            result.set( resultNames.indexOf( aggregate.name ), alias( expression, aggregate.name ) );
        }
        implementor.add( with_( list_( result ) ) );
    }


    private static NeoStatement alias( String expression, String name ) {
        NeoStatement value = literal_( PolyString.of( expression ) );
        return expression.equals( name ) ? value : as_( value, literal_( PolyString.of( name ) ) );
    }

}

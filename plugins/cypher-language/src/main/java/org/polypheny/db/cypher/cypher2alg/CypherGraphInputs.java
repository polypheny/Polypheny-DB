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

package org.polypheny.db.cypher.cypher2alg;

import java.util.List;
import java.util.stream.IntStream;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgCall;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgJoin;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgShortestPathJoin;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.ImmutableBitSet;


/** Resolves graph operands in Cypher expressions against the graph of the enclosing query. */
final class CypherGraphInputs extends AlgShuttleImpl {

    private static final long GRAPH_REFERENCE = -1;
    private final LogicalEntity entity;
    private final AlgCluster cluster;
    private final RexBuilder rexBuilder;
    private final AlgDataType graphType;
    private boolean usesGraphInput;


    CypherGraphInputs( LogicalEntity entity, AlgCluster cluster ) {
        this.entity = entity;
        this.cluster = cluster;
        this.rexBuilder = cluster.getRexBuilder();
        this.graphType = cluster.getTypeFactory().createPolyType( PolyType.GRAPH );
    }


    static RexNode graphReference( AlgDataType type ) {
        return new RexDynamicParam( type, GRAPH_REFERENCE );
    }


    boolean usesGraphInput() {
        return usesGraphInput;
    }


    @Override
    public AlgNode visit( LogicalLpgProject project ) {
        AlgNode input = project.getInput().accept( this );
        GraphReference ref = new GraphReference( input.getTupleType().getFieldCount() );
        List<RexNode> values = project.getProjects().stream().map( value -> value.accept( ref ) ).toList();
        if ( ref.used ) {
            input = withGraph( input );
        }
        return input == project.getInput() ? project : new LogicalLpgProject( cluster, project.getTraitSet(), input, values, project.getNames() );
    }


    @Override
    public AlgNode visit( LogicalLpgFilter filter ) {
        AlgNode input = filter.getInput().accept( this );
        int width = input.getTupleType().getFieldCount();
        GraphReference ref = new GraphReference( width );
        RexNode condition = filter.getCondition().accept( ref );
        if ( !ref.used ) {
            return input == filter.getInput() ? filter : filter.copy( filter.getTraitSet(), List.of( input ) );
        }
        return removeGraph( LogicalLpgFilter.create( withGraph( input ), condition ), filter );
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        if ( other instanceof LogicalLpgShortestPathJoin shortest ) {
            return resolveShortestJoin( shortest );
        }
        if ( !(other instanceof LogicalLpgJoin join) ) {
            return super.visit( other );
        }
        AlgNode left = join.getLeft().accept( this );
        AlgNode right = join.getRight().accept( this );
        GraphReference ref = new GraphReference( left.getTupleType().getFieldCount() + right.getTupleType().getFieldCount() );
        RexNode condition = join.getCondition().accept( ref );
        if ( ref.used ) {
            return removeGraph( join.copy( join.getTraitSet(), condition, left, withGraph( right ), join.getJoinType(), join.isSemiJoinDone() ), join );
        }
        return left == join.getLeft() && right == join.getRight()
                ? join : join.copy( join.getTraitSet(), condition, left, right, join.getJoinType(), join.isSemiJoinDone() );
    }


    private AlgNode resolveShortestJoin( LogicalLpgShortestPathJoin join ) {
        AlgNode left = join.getLeft().accept( this );
        AlgNode right = join.getRight().accept( this );
        GraphReference ref = new GraphReference( left.getTupleType().getFieldCount() + right.getTupleType().getFieldCount() );
        RexNode condition = join.getCondition().accept( ref );
        if ( ref.used ) {
            return removeGraph( new LogicalLpgShortestPathJoin( left, withGraph( right ), condition, join.isOptional(),
                    join.getPathColumns(), join.getGroupColumns() ), join );
        }
        return left == join.getLeft() && right == join.getRight() ? join
                : join.copy( join.getTraitSet(), List.of( left, right ) );
    }


    private AlgNode withGraph( AlgNode input ) {
        usesGraphInput = true;
        AlgNode scan = new LogicalLpgScan( cluster, cluster.traitSet(), entity, GraphType.of() );
        RexCall extract = (RexCall) rexBuilder.makeCall( graphType,
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_GRAPH_VALUE ),
                List.of( rexBuilder.makeInputRef( graphType, 0 ) ) );
        AlgNode value = new LogicalLpgMatch( cluster, cluster.traitSet(), scan, List.of( extract ), List.of( PolyString.of( "$graph" ) ), true );
        // Finish each writing input row before reading the graph.
        return containsWrites( input )
                ? new LogicalLpgCall( input, value, cluster.createCorrel(), ImmutableBitSet.of() )
                : LogicalLpgJoin.create( input, value, rexBuilder.makeLiteral( true ), JoinAlgType.INNER );
    }


    private static boolean containsWrites( AlgNode node ) {
        return node.isDataModifying() || node.getInputs().stream().anyMatch( CypherGraphInputs::containsWrites );
    }


    private AlgNode removeGraph( AlgNode input, AlgNode original ) {
        List<RexNode> visible = IntStream.range( 0, original.getTupleType().getFieldCount() )
                .mapToObj( index -> (RexNode) rexBuilder.makeInputRef( input, index ) ).toList();
        return LogicalLpgProject.create( input, visible, original.getTupleType().getFieldNames().stream().map( PolyString::of ).toList() );
    }


    private final class GraphReference extends RexShuttle {

        private final int index;
        private boolean used;


        private GraphReference( int index ) {
            this.index = index;
        }


        @Override
        public RexNode visitDynamicParam( RexDynamicParam parameter ) {
            if ( parameter.getIndex() == GRAPH_REFERENCE && parameter.getType().getPolyType() == PolyType.GRAPH ) {
                used = true;
                return rexBuilder.makeInputRef( graphType, index );
            }
            return parameter;
        }

    }

}

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

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.labels_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.match_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.node_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.path_;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.adapter.neo4j.NeoGraphImplementor;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.NeoStatement;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.lpg.LpgMatch;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.util.Pair;

public class NeoLpgMatch extends LpgMatch implements NeoGraphAlg {

    /**
     * Creates a {@link org.polypheny.db.adapter.neo4j.NeoConvention} of a {@link LpgMatch}.
     *
     * @param cluster Cluster this expression belongs to
     * @param traits Traits active for this node, including {@link ModelTrait#GRAPH}
     * @param input Input algebraic expression
     */
    public NeoLpgMatch( AlgCluster cluster, AlgTraitSet traits, AlgNode input, List<RexCall> matches, List<PolyString> names ) {
        super( cluster, traits, input, matches, names );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {
        implementor.visitChild( 0, getInput() );

        List<NeoStatement> neoMatches = new ArrayList<>();
        for ( Pair<PolyString, RexCall> match : Pair.zip( names, matches ) ) {
            String mappingLabel = implementor.getGraph().mappingLabel;
            switch ( match.right.op.getOperatorName() ) {
                case CYPHER_NODE_EXTRACT:
                    neoMatches.add( node_( match.left, labels_( PolyString.of( mappingLabel ) ) ) );
                    break;
                case CYPHER_NODE_MATCH:
                    PolyNode node = ((RexLiteral) match.right.operands.get( 1 )).value.asNode();
                    if ( !match.left.value.isEmpty() ) {
                        node = new PolyNode( node.id, node.properties, node.labels, match.left );
                    }
                    neoMatches.add( node_( node, PolyString.of( mappingLabel ), false ) );
                    break;
                case CYPHER_PATH_MATCH:
                    neoMatches.add( path_( match.left, ((RexLiteral) match.right.operands.get( 1 )).value.asPath(), PolyString.of( mappingLabel ), false ) );
                    break;
            }
        }

        implementor.add( match_( list_( neoMatches ) ) );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new NeoLpgMatch( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), matches, names );
    }

}

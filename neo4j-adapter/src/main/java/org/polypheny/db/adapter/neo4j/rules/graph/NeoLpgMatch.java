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
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyPath;
import org.polypheny.db.util.Pair;

public class NeoLpgMatch extends LpgMatch implements NeoGraphAlg {

    /**
     * Creates a {@link org.polypheny.db.adapter.neo4j.NeoConvention} of a {@link LpgMatch}.
     *
     * @param cluster Cluster this expression belongs to
     * @param traits Traits active for this node, including {@link org.polypheny.db.schema.ModelTrait#GRAPH}
     * @param input Input algebraic expression
     */
    public NeoLpgMatch( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, List<RexCall> matches, List<String> names ) {
        super( cluster, traits, input, matches, names );
    }


    @Override
    public void implement( NeoGraphImplementor implementor ) {
        implementor.visitChild( 0, getInput() );

        List<NeoStatement> neoMatches = new ArrayList<>();
        for ( Pair<String, RexCall> match : Pair.zip( names, matches ) ) {
            String mappingLabel = implementor.getGraph().mappingLabel;
            switch ( match.right.op.getOperatorName() ) {
                case CYPHER_NODE_EXTRACT:
                    neoMatches.add( node_( match.left, labels_( mappingLabel ) ) );
                    break;
                case CYPHER_NODE_MATCH:
                    neoMatches.add( node_( ((RexLiteral) match.right.operands.get( 1 )).getValueAs( PolyNode.class ), mappingLabel, false ) );
                    break;
                case CYPHER_PATH_MATCH:
                    neoMatches.add( path_( match.left, ((RexLiteral) match.right.operands.get( 1 )).getValueAs( PolyPath.class ), mappingLabel, false ) );
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

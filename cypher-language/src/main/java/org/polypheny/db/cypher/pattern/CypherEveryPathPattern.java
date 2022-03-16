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

package org.polypheny.db.cypher.pattern;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.logical.graph.LogicalGraphValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyPath;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

@Getter
public class CypherEveryPathPattern extends CypherPattern {

    private final List<CypherNodePattern> nodes;
    private final List<CypherRelPattern> edges;


    /**
     * collections:
     * <code>
     * nodes[n0, n1, n2]
     * rel[r0, r1]
     * </code>
     *
     * resulting path:
     * <code>
     * [n0] - [r0] - [n1] - [r1] - [n2]
     * </code>
     *
     * @param nodes all nodes included in path
     * @param edges all relationship connections included in the path
     */
    public CypherEveryPathPattern( List<CypherNodePattern> nodes, List<CypherRelPattern> edges ) {
        super( ParserPos.ZERO );
        this.nodes = nodes;
        this.edges = edges;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.PATH;
    }


    private List<Pair<String, PolyNode>> getPolyNodes() {
        return this.nodes.stream().map( CypherNodePattern::getPolyNode ).collect( Collectors.toList() );
    }


    private List<Pair<String, PolyEdge>> getPolyEdges( List<Pair<String, PolyNode>> nodes ) {
        List<Pair<String, PolyEdge>> edges = new ArrayList<>();
        assert nodes.size() == this.edges.size() + 1;

        Pair<String, PolyNode> node = nodes.get( 0 );
        int i = 0;

        for ( CypherRelPattern edge : this.edges ) {
            Pair<String, PolyNode> next = nodes.get( ++i ); // next node
            edges.add( edge.getPolyRelationship( node.right.id, next.right.id ) );
            node = next;
        }

        return edges;
    }


    @Override
    public LogicalGraphValues getPatternValues( CypherContext context ) {
        List<Pair<String, PolyNode>> nodes = getPolyNodes();
        List<Pair<String, PolyEdge>> relationships = getPolyEdges( nodes );

        return LogicalGraphValues.create( context.cluster, context.cluster.traitSet(), nodes, context.nodeType, relationships, context.edgeType );
    }


    @Override
    public Pair<String, RexNode> getPatternMatch( CypherContext context ) {
        if ( edges.isEmpty() ) {
            return getNodeFilter( context );
        }
        return Pair.of( null, getPathFilter( context ) );
    }


    private Pair<String, RexNode> getNodeFilter( CypherContext context ) {
        // single node match MATCH (n) RETURN n
        assert nodes.size() == 1;
        context.addDefaultScanIfNecessary();
        Pair<String, PolyNode> nameNode = nodes.get( 0 ).getPolyNode();

        RexInputRef graphRef = context.rexBuilder.makeInputRef( context.nodeType, 0 );
        if ( nameNode.right.isBlank() ) {
            return Pair.of( nameNode.left, context.rexBuilder.makeCall( context.nodeType, OperatorRegistry.get( QueryLanguage.CYPHER, OperatorName.CYPHER_NODE_EXTRACT ), List.of( graphRef ) ) );
        } else {
            return Pair.of( nameNode.left, context.rexBuilder.makeCall( context.nodeType, OperatorRegistry.get( QueryLanguage.CYPHER, OperatorName.CYPHER_NODE_MATCH ), List.of( graphRef, new RexLiteral( nameNode.right, context.nodeType, PolyType.NODE ) ) ) );
        }
    }


    private RexNode getPathFilter( CypherContext context ) {
        List<Pair<String, PolyNode>> polyNodes = getPolyNodes();

        List<Pair<String, PolyEdge>> polyEdges = getPolyEdges( polyNodes );
        PolyPath path = PolyPath.create( polyNodes, polyEdges );

        return new RexCall(
                context.graphType,
                OperatorRegistry.get( QueryLanguage.CYPHER, OperatorName.CYPHER_PATH_MATCH ),
                List.of(
                        new RexInputRef( 0, context.graphType ),
                        new RexLiteral( path, context.typeFactory.createPathType( path.getPathType( context.nodeType, context.edgeType ) ), PolyType.PATH ) ) );
    }

}

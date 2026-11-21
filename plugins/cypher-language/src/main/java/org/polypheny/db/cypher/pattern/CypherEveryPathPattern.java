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

package org.polypheny.db.cypher.pattern;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.EdgeVariableHolder;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
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


    @Override
    public List<PolyString> getUnderlyingLabels() {
        return nodes.stream().flatMap( n -> n.getLabels().stream() ).toList();
    }


    private List<Pair<PolyString, PolyNode>> getPolyNodes( CypherContext context ) {
        List<Pair<PolyString, PolyNode>> nodes = new LinkedList<>();
        for ( CypherNodePattern node : this.nodes ) {
            Pair<PolyString, PolyNode> namedNode = node.getPolyNode( context );
            if ( namedNode.left != null && !namedNode.left.isNull() && context.getNodeVariable( namedNode.left ) != null ) {
                // this node exists already in the scope, we replace it with the existing one
                namedNode = context.getNodeVariable( namedNode.left );
            }
            nodes.add( namedNode );
            // Register immediately so later nodes in this pattern can reuse its binding.
            context.addNodes( List.of( namedNode ) );
        }

        return nodes;
    }


    @Override
    public List<CypherExpression> getPredicates() {
        return Stream.concat( nodes.stream().map( CypherNodePattern::getPredicate ), edges.stream().map( CypherRelPattern::getPredicate ) )
                .filter( Objects::nonNull )
                .toList();
    }


    private List<Pair<PolyString, EdgeVariableHolder>> getPolyEdges( List<Pair<PolyString, PolyNode>> nodes, CypherContext context ) {
        List<Pair<PolyString, EdgeVariableHolder>> edges = new ArrayList<>();
        assert nodes.size() == this.edges.size() + 1;

        Pair<PolyString, PolyNode> node = nodes.get( 0 );
        int i = 0;

        for ( CypherRelPattern edge : this.edges ) {
            Pair<PolyString, PolyNode> next = nodes.get( ++i ); // next node
            Pair<PolyString, PolyEdge> now = edge.getPolyEdge( node.right.id, next.right.id, context );
            edges.add( Pair.of( now.left, new EdgeVariableHolder( now.right, now.left, node.left, next.left ) ) );
            node = next;
        }

        return edges;
    }


    @Override
    public void getPatternValues( CypherContext context ) {
        getPatternValues( context, null );
    }


    public void getPatternValues( CypherContext context, PolyString pathName ) {
        List<Pair<PolyString, PolyNode>> nodes = getPolyNodes( context );
        List<Pair<PolyString, EdgeVariableHolder>> edges = getPolyEdges( nodes, context );
        context.addEdges( edges );
        if ( pathName != null ) {
            context.addCreatedPath( pathName, nodes, edges );
        }
    }


    @Override
    public Pair<PolyString, RexNode> getPatternMatch( CypherContext context ) {
        if ( edges.isEmpty() ) {
            return getNodeFilter( context );
        }
        RexNode path = getPathFilter( context );

        String name = path.getType().getFields().stream().map( AlgDataTypeField::getName ).collect( Collectors.joining( "-", "$", "$" ) );

        return Pair.of( PolyString.of( name ), path );
    }


    private Pair<PolyString, RexNode> getNodeFilter( CypherContext context ) {
        // single node match MATCH (n) RETURN n
        assert nodes.size() == 1;
        context.addDefaultScanIfNecessary();
        Pair<PolyString, PolyNode> nameNode = nodes.get( 0 ).getPolyNode();

        RexIndexRef graphRef = context.rexBuilder.makeInputRef( context.nodeType, 0 );
        if ( nameNode.right.isBlank() ) {
            Pair<PolyString, PolyNode> old = context.getNodeVariable( nameNode.left );
            if ( old != null ) {
                // variable exists already and is a reference
                nameNode = old;
            } else {
                // let's save it for later
                context.addNodes( List.of( nameNode ) );
            }

            RexNode node = context.getRexNode( nameNode.left.value );
            if ( node != null ) {
                // variable exist already and is a pointer
                return Pair.of( nameNode.left, node );
            }
            // simple extract
            return Pair.of( nameNode.left, context.rexBuilder.makeCall( context.nodeType, OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_NODE_EXTRACT ), List.of( graphRef ) ) );
        } else {
            // not simple extract, lets save the node
            context.addNodes( List.of( nameNode ) );
            return Pair.of( nameNode.left, context.rexBuilder.makeCall( context.nodeType, OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_NODE_MATCH ), List.of( graphRef, new RexLiteral( nameNode.right, context.nodeType, PolyType.NODE ) ) ) );
        }
    }


    RexNode getPathFilter( CypherContext context ) {
        List<Pair<PolyString, PolyNode>> polyNodes = new ArrayList<>();
        for ( CypherNodePattern node : nodes ) {
            Pair<PolyString, PolyNode> binding = node.getPolyNode();
            Pair<PolyString, PolyNode> previous = context.getNodeVariable( binding.left );
            polyNodes.add( binding.right.isBlank() && previous != null ? previous : binding );
        }
        // Register after collecting all occurrences so a repeated variable keeps each occurrence's constraints.
        context.addNodes( polyNodes );

        List<Pair<PolyString, EdgeVariableHolder>> polyEdges = getPolyEdges( polyNodes, null );
        PolyPath path = PolyPath.create( polyNodes, Pair.right( polyEdges ).stream().map( EdgeVariableHolder::asNamedEdge ).toList() );

        AlgDataType pathType = context.typeFactory.createPathType( path.getPathType( context.nodeType, context.edgeType ) );

        return new RexCall(
                pathType,
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_PATH_MATCH ),
                List.of(
                        new RexIndexRef( 0, context.graphType ),
                        new RexLiteral( path, pathType, PolyType.PATH ) ) );
    }

}

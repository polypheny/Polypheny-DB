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
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.EdgeVariableHolder;
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


    private List<Pair<String, PolyNode>> getPolyNodes( CypherContext context ) {
        List<Pair<String, PolyNode>> nodes = new LinkedList<>();
        for ( CypherNodePattern node : this.nodes ) {
            Pair<String, PolyNode> namedNode = node.getPolyNode();
            if ( namedNode.left != null && context.getNodeVariable( namedNode.left ) != null ) {
                // this node exists already in the scope, we replace it with the existing one
                namedNode = context.getNodeVariable( namedNode.left );
            }
            nodes.add( namedNode );
            if ( namedNode != null && namedNode.left != null ) {
                context.addNodes( List.of( Objects.requireNonNull( namedNode ) ) );
            }
        }

        return nodes;
    }


    private List<Pair<String, EdgeVariableHolder>> getPolyEdges( List<Pair<String, PolyNode>> nodes ) {
        List<Pair<String, EdgeVariableHolder>> edges = new ArrayList<>();
        assert nodes.size() == this.edges.size() + 1;

        Pair<String, PolyNode> node = nodes.get( 0 );
        int i = 0;

        for ( CypherRelPattern edge : this.edges ) {
            Pair<String, PolyNode> next = nodes.get( ++i ); // next node
            Pair<String, PolyEdge> now = edge.getPolyEdge( node.right.id, next.right.id );
            edges.add( Pair.of( now.left, new EdgeVariableHolder( now.right, now.left, node.left, next.left ) ) );
            node = next;
        }

        return edges;
    }


    @Override
    public void getPatternValues( CypherContext context ) {
        List<Pair<String, PolyNode>> nodes = getPolyNodes( context );
        List<Pair<String, EdgeVariableHolder>> edges = getPolyEdges( nodes );

        context.addNodes( nodes );
        context.addEdges( edges );

    }


    @Override
    public Pair<String, RexNode> getPatternMatch( CypherContext context ) {
        if ( edges.isEmpty() ) {
            return getNodeFilter( context );
        }
        RexNode path = getPathFilter( context );

        String name = path.getType().getFieldList().stream().map( AlgDataTypeField::getName ).collect( Collectors.joining( "-", "$", "$" ) );

        return Pair.of( name, path );
    }


    private Pair<String, RexNode> getNodeFilter( CypherContext context ) {
        // single node match MATCH (n) RETURN n
        assert nodes.size() == 1;
        context.addDefaultScanIfNecessary();
        Pair<String, PolyNode> nameNode = nodes.get( 0 ).getPolyNode();

        RexInputRef graphRef = context.rexBuilder.makeInputRef( context.nodeType, 0 );
        if ( nameNode.right.isBlank() ) {
            Pair<String, PolyNode> old = context.getNodeVariable( nameNode.left );
            if ( old != null ) {
                // variable exists already and is a reference
                nameNode = old;
            } else {
                // let's save it for later
                context.addNodes( List.of( nameNode ) );
            }

            RexNode node = context.getRexNode( nameNode.left );
            if ( node != null ) {
                // variable exist already and is a pointer
                return Pair.of( nameNode.left, node );
            }
            // simple extract
            return Pair.of( nameNode.left, context.rexBuilder.makeCall( context.nodeType, OperatorRegistry.get( QueryLanguage.CYPHER, OperatorName.CYPHER_NODE_EXTRACT ), List.of( graphRef ) ) );
        } else {
            // not simple extract, lets save the node
            context.addNodes( List.of( nameNode ) );
            return Pair.of( nameNode.left, context.rexBuilder.makeCall( context.nodeType, OperatorRegistry.get( QueryLanguage.CYPHER, OperatorName.CYPHER_NODE_MATCH ), List.of( graphRef, new RexLiteral( nameNode.right, context.nodeType, PolyType.NODE ) ) ) );
        }
    }


    private RexNode getPathFilter( CypherContext context ) {
        List<Pair<String, PolyNode>> polyNodes = getPolyNodes( context );

        List<Pair<String, EdgeVariableHolder>> polyEdges = getPolyEdges( polyNodes );
        PolyPath path = PolyPath.create( polyNodes, Pair.right( polyEdges ).stream().map( EdgeVariableHolder::asNamedEdge ).collect( Collectors.toList() ) );

        AlgDataType pathType = context.typeFactory.createPathType( path.getPathType( context.nodeType, context.edgeType ) );

        return new RexCall(
                pathType,
                OperatorRegistry.get( QueryLanguage.CYPHER, OperatorName.CYPHER_PATH_MATCH ),
                List.of(
                        new RexInputRef( 0, context.graphType ),
                        new RexLiteral( path, pathType, PolyType.PATH ) ) );
    }

}

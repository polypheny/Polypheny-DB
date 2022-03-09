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
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.logical.graph.LogicalGraphPattern;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.PolyCollections.PolyMap;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyGraph;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

@Getter
public class CypherEveryPathPattern extends CypherPattern {

    private final List<CypherNodePattern> nodes;
    private final List<CypherRelPattern> relationships;


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
     * @param relationships all relationship connections included in the path
     */
    public CypherEveryPathPattern( List<CypherNodePattern> nodes, List<CypherRelPattern> relationships ) {
        super( ParserPos.ZERO );
        this.nodes = nodes;
        this.relationships = relationships;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.PATH;
    }


    private List<Pair<String, PolyNode>> getPolyNodes() {
        return this.nodes.stream().map( CypherNodePattern::getPolyNode ).collect( Collectors.toList() );
    }


    private List<Pair<String, PolyEdge>> getPolyRelationships( List<PolyNode> nodes ) {
        List<Pair<String, PolyEdge>> rels = new ArrayList<>();
        assert nodes.size() == relationships.size() + 1;

        Iterator<CypherRelPattern> relIter = relationships.iterator();
        PolyNode node = nodes.get( 0 );
        int i = 0;
        while ( relIter.hasNext() ) {
            i++;
            PolyNode next = nodes.get( i );
            rels.add( relationships.get( i - 1 ).getPolyRelationship( node.id, next.id ) );
            node = next;
        }

        return rels;
    }


    @Override
    public LogicalGraphPattern getPatternValues( CypherContext context ) {
        List<Pair<String, PolyNode>> nodes = getPolyNodes();
        List<Pair<String, PolyEdge>> relationships = getPolyRelationships( Pair.right( nodes ) );

        return LogicalGraphPattern.create( context.cluster, context.cluster.traitSet(), nodes, context.nodeType, relationships, context.relType );
    }


    @Override
    public RexNode getPatternFilter( CypherContext context ) {
        List<PolyNode> polyNodes = Pair.right( getPolyNodes() );
        PolyMap<Long, PolyNode> nodes = new PolyMap<>( polyNodes.stream().collect( Collectors.toMap( e -> e.id, e -> e ) ) );
        PolyMap<Long, PolyEdge> relationships = new PolyMap<>( getPolyRelationships( polyNodes ).stream().collect( Collectors.toMap( e -> e.right.id, e -> e.right ) ) );
        PolyGraph graph = new PolyGraph( nodes, relationships );

        return new RexCall(
                context.graphType,
                OperatorRegistry.get( OperatorName.CYPHER_PATTERN_MATCH ),
                List.of(
                        new RexLiteral( graph, context.graphType, PolyType.GRAPH ),
                        new RexInputRef( 0, context.graphType ) ) );
    }

}

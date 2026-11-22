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

package org.polypheny.db.cypher.expression;

import java.util.List;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.cypher.cypher2alg.CypherSyntaxException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.pattern.CypherEveryPathPattern;
import org.polypheny.db.cypher.pattern.CypherNodePattern;
import org.polypheny.db.cypher.pattern.CypherRelPattern;
import org.polypheny.db.cypher.pattern.CypherShortestPathPattern;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.util.Pair;


/** Lowers shortestPath(...) when the endpoints come from an enclosing row. */
final class CypherShortestPathValue {

    private CypherShortestPathValue() {
    }


    static RexNode toRex( CypherShortestPathPattern shortest, CypherContext context, RexType type ) {
        CypherEveryPathPattern path = shortest.getSingleSegment();
        CypherNodePattern first = path.getNodes().get( 0 );
        CypherNodePattern last = path.getNodes().get( 1 );
        CypherRelPattern relationship = path.getEdges().get( 0 );
        if ( first.getVariable() == null || last.getVariable() == null ) {
            throw new CypherSyntaxException( "shortestPath() in an expression requires two bound node variables." );
        }
        RexNode start = first.getVariable().getRex( context, type ).right;
        RexNode end = last.getVariable().getRex( context, type ).right;
        RexNode boundRelationship = value( relationship.getVariable(), context, type );

        Pair<PolyString, PolyNode> left = first.withMatchProperties( first.getVariable(), null ).getPolyNode();
        Pair<PolyString, PolyNode> right = last.withMatchProperties( last.getVariable(), null ).getPolyNode();
        Pair<PolyString, PolyEdge> edge = relationship.withMatchProperties( relationship.getVariable(), null ).getPolyEdge( left.right.id, right.right.id );
        PolyPath template = PolyPath.create( List.of( left, right ), List.of( edge ) );
        AlgDataType pathType = context.typeFactory.createPathType( template.getPathType( context.typeFactory ) );
        AlgDataType nullablePath = context.typeFactory.createTypeWithNullability( pathType, true );
        RexNode result = context.rexBuilder.makeCall( nullablePath,
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_SHORTEST_PATH_VALUE ),
                List.of( context.graphValue(), new RexLiteral( template, pathType, PolyType.PATH ), start, end, boundRelationship,
                        value( first.getProperties(), context, type ), value( relationship.getProperties(), context, type ), value( last.getProperties(), context, type ) ) );

        List<CypherExpression> predicates = path.getPredicates();
        if ( predicates.isEmpty() ) {
            return result;
        }
        RexNode condition = RexUtil.composeConjunction( context.rexBuilder, predicates.stream()
                .map( predicate -> context.booleanPredicate( predicate.getRex( context, RexType.FILTER ).right ) ).toList() );
        return context.rexBuilder.makeCall( nullablePath, OperatorRegistry.get( OperatorName.CASE ),
                List.of( condition, result, context.rexBuilder.makeNullLiteral( nullablePath ) ) );
    }


    private static RexNode value( CypherExpression expression, CypherContext context, RexType type ) {
        return expression == null ? context.rexBuilder.constantNull() : expression.getRex( context, type ).right;
    }

}

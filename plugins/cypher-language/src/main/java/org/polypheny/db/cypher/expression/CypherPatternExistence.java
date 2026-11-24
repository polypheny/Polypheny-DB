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

package org.polypheny.db.cypher.expression;

import static org.polypheny.db.cypher.CypherPathMatchUtil.isRelationshipList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.cypher.cypher2alg.CypherSyntaxException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.pattern.CypherEveryPathPattern;
import org.polypheny.db.cypher.pattern.CypherNodePattern;
import org.polypheny.db.cypher.pattern.CypherRelPattern;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.util.Pair;


/** Lowers a relationship pattern used as a Boolean expression. */
final class CypherPatternExistence {

    private final CypherContext context;
    private final RexType rexType;
    private final Set<String> names = new HashSet<>();
    private final Map<String, PolyType> bindingTypes = new HashMap<>();
    private final List<RexNode> bindings = new ArrayList<>();
    private final List<RexNode> properties = new ArrayList<>();


    private CypherPatternExistence( CypherContext context, RexType rexType ) {
        this.context = context;
        this.rexType = rexType;
    }


    static RexNode toRex( CypherEveryPathPattern pattern, CypherContext context, RexType rexType ) {
        return new CypherPatternExistence( context, rexType ).compile( pattern );
    }


    private RexNode compile( CypherEveryPathPattern pattern ) {
        for ( CypherNodePattern node : pattern.getNodes() ) {
            addBinding( node.getVariable(), PolyType.NODE );
        }
        for ( CypherRelPattern relationship : pattern.getEdges() ) {
            addBinding( relationship.getVariable(), relationship.getPathLength() == null ? PolyType.EDGE : PolyType.ARRAY );
        }

        List<Pair<PolyString, PolyNode>> nodes = new ArrayList<>();
        for ( CypherNodePattern node : pattern.getNodes() ) {
            ElementProperties checks = addProperties( node.getVariable(), node.getProperties() );
            Pair<PolyString, PolyNode> template = node.withMatchProperties( checks.binding(), null ).getPolyNode();
            template.right.getProperties().putAll( checks.literals() );
            nodes.add( template );
        }
        List<Pair<PolyString, PolyEdge>> relationships = new ArrayList<>();
        for ( int i = 0; i < pattern.getEdges().size(); i++ ) {
            CypherRelPattern relationship = pattern.getEdges().get( i );
            ElementProperties checks = addProperties( relationship.getVariable(), relationship.getProperties() );
            Pair<PolyString, PolyEdge> template = relationship.withMatchProperties( checks.binding(), null )
                    .getPolyEdge( nodes.get( i ).right.id, nodes.get( i + 1 ).right.id );
            template.right.getProperties().putAll( checks.literals() );
            relationships.add( template );
        }
        PolyPath template = PolyPath.create( nodes, relationships );
        AlgDataType pathType = context.typeFactory.createPathType( template.getPathType( context.typeFactory ) );
        QueryLanguage cypher = QueryLanguage.from( "cypher" );
        List<RexNode> operands = new ArrayList<>( List.of( context.graphValue(), new RexLiteral( template, pathType, PolyType.PATH ), map( bindings ) ) );
        operands.addAll( properties );
        RexNode exists = context.rexBuilder.makeCall( context.booleanType, OperatorRegistry.get( cypher, OperatorName.CYPHER_PATTERN_EXISTS ),
                operands );
        if ( pattern.getPredicates().isEmpty() ) {
            return exists;
        }

        List<RexNode> predicates = new ArrayList<>();
        predicates.add( exists );
        for ( CypherExpression predicate : pattern.getPredicates() ) {
            RexNode condition = context.booleanPredicate( predicate.getRex( context, RexType.FILTER ).right );
            predicates.add( context.rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_TRUE ), condition ) );
        }
        return RexUtil.composeConjunction( context.rexBuilder, predicates );
    }


    private void addBinding( CypherVariable variable, PolyType expected ) {
        if ( variable == null ) {
            return;
        }
        String name = variable.getName();
        PolyType previous = bindingTypes.putIfAbsent( name, expected );
        if ( previous != null ) {
            if ( previous != expected ) {
                throw new CypherSyntaxException( "Pattern variable '%s' has conflicting types: %s and %s.".formatted( name, previous, expected ) );
            }
            return;
        }
        names.add( name );
        RexNode value = variable.getRex( context, rexType ).right;
        PolyType actual = value.getType().getPolyType();
        boolean compatible = expected == PolyType.ARRAY ? isRelationshipList( value.getType() ) : actual == expected;
        if ( !compatible && actual != PolyType.ANY && actual != PolyType.NULL ) {
            String required = expected == PolyType.ARRAY ? "a relationship list" : expected.name();
            throw new CypherSyntaxException( "Pattern variable '%s' requires %s but has type %s.".formatted( name, required, value.getType() ) );
        }
        bindings.add( context.rexBuilder.makeLiteral( name ) );
        bindings.add( value );
    }


    private ElementProperties addProperties( CypherVariable variable, CypherExpression expression ) {
        if ( expression == null ) {
            return new ElementProperties( variable, Map.of() );
        }
        RexNode map = expression.getRex( context, rexType ).right;
        if ( map.getType().getPolyType() != PolyType.MAP ) {
            throw new CypherSyntaxException( "A pattern requires a map of property values." );
        }
        CypherPatternProperties split = CypherPatternProperties.split( map );
        if ( split.computed() == null ) {
            return new ElementProperties( variable, split.literals() );
        }
        CypherVariable capture = variable;
        if ( capture == null ) {
            String name = "$exists" + names.size();
            while ( !names.add( name ) ) {
                name += "_";
            }
            capture = new CypherVariable( ParserPos.ZERO, name );
        }
        properties.add( context.rexBuilder.makeLiteral( capture.getName() ) );
        properties.add( split.computed() );
        return new ElementProperties( capture, split.literals() );
    }


    private record ElementProperties( CypherVariable binding, Map<PolyString, PolyValue> literals ) {

    }


    private RexNode map( List<RexNode> entries ) {
        AlgDataType type = context.typeFactory.createMapType( context.typeFactory.createPolyType( PolyType.VARCHAR ),
                context.typeFactory.createPolyType( PolyType.ANY ) );
        return context.rexBuilder.makeCall( type, OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_MAP_CONSTRUCTOR ), entries );
    }

}

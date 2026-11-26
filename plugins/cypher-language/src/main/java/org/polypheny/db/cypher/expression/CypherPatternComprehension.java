/*
 * Copyright 2019-2023 The Polypheny Project
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.cypher.cypher2alg.CypherSyntaxException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.pattern.CypherEveryPathPattern;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

@Getter
public class CypherPatternComprehension extends CypherExpression {

    private final ParserPos patternPos;
    private final CypherPattern pattern;
    private final CypherVariable variable;
    private final CypherExpression where;
    private final CypherExpression projection;


    public CypherPatternComprehension( ParserPos pos, ParserPos patternPos, CypherPattern pattern, CypherVariable variable, CypherExpression where, CypherExpression projection ) {
        super( pos );
        this.patternPos = patternPos;
        this.pattern = pattern;
        this.variable = variable;
        this.where = where;
        this.projection = projection;
    }


    @Override
    protected Pair<PolyString, RexNode> toRex( CypherContext context, RexType type ) {
        if ( !(pattern instanceof CypherEveryPathPattern path) ) {
            throw new CypherSyntaxException( "Pattern comprehension requires a relationship pattern." );
        }
        RexNode paths = CypherPatternExpression.paths( path, context, type );
        AlgDataType pathType = paths.getType().getComponentType();
        RexNode result = context.listBinding( null, pathType, element -> {
            Map<String, RexNode> bindings = new LinkedHashMap<>();
            for ( AlgDataTypeField field : pathType.getFields() ) {
                RexNode value = context.rexBuilder.makeCall( field.getType(),
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_EXTRACT_FROM_PATH ),
                        List.of( element, context.rexBuilder.makeLiteral( field.getName() ) ) );
                bindings.putIfAbsent( field.getName(), value );
            }
            if ( variable != null && bindings.putIfAbsent( variable.getName(), element ) != null ) {
                throw new CypherSyntaxException( "Pattern path variable '" + variable.getName() + "' is already bound." );
            }
            return context.withLocalBindings( bindings, () -> {
                List<CypherExpression> predicates = new ArrayList<>( path.getPredicates() );
                if ( where != null ) {
                    predicates.add( where );
                }
                RexNode predicate = RexUtil.composeConjunction( context.rexBuilder, predicates.stream()
                        .map( condition -> context.booleanPredicate( condition.getRex( context, RexType.FILTER ).right ) ).toList() );
                RexNode value = projection.getRex( context, RexType.PROJECT ).right;
                return CypherListComprehension.project( context, paths, element, predicate, value );
            } );
        } );
        return Pair.of( null, result );
    }

}

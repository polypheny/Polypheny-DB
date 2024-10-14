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

package org.polypheny.db.cypher.clause;

import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.expression.CypherAggregate;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherExpression.ExpressionType;
import org.polypheny.db.cypher.expression.CypherFunctionInvocation;
import org.polypheny.db.cypher.expression.CypherLiteral;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


@Getter
public class CypherReturnItem extends CypherReturn {

    private final CypherExpression expression;
    private CypherVariable variable;
    private int beginOffset;
    private int endOffset;


    public CypherReturnItem( ParserPos pos, CypherExpression expression, CypherVariable variable ) {
        super( pos );
        this.expression = expression;
        this.variable = variable;
    }


    public CypherReturnItem( ParserPos pos, CypherExpression expression, int beginOffset, int endOffset ) {
        super( pos );
        this.expression = expression;
        this.beginOffset = beginOffset;
        this.endOffset = endOffset;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.RETURN;
    }


    @Override
    @Nullable
    public Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {
        if ( variable != null ) {
            String name = variable.getName();
            if ( this.expression instanceof CypherFunctionInvocation ) {
                CypherFunctionInvocation func = (CypherFunctionInvocation) this.expression;

                switch ( func.getOperatorName() ) {
                    case CYPHER_POINT: {
                        CypherLiteral mapExpression = (CypherLiteral) func.getArguments().get( 0 );
                        // RexBuilder cannot handle CypherLiteral values, so we have to extract the values first.
                        Map<String, Object> map = new HashMap<>();
                        mapExpression.getMapValue().forEach( ( key, value ) -> {
                            if ( value instanceof CypherLiteral ) {
                                map.put( key, ((CypherLiteral) value).getValue() );
                            }
                        } );
                        RexNode node = context.rexBuilder.makeLiteral(
                                map,
                                context.typeFactory.createMapType(
                                        context.typeFactory.createPolyType( PolyType.VARCHAR ),
                                        context.typeFactory.createPolyType( PolyType.ANY ) ),
                                true );
                        return Pair.of( PolyString.of( name ), new RexCall(
                                context.geometryType,
                                OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_POINT ),
                                List.of( node ) ) );
                    }
                    case DISTANCE: {
                        throw new NotImplementedException( "TODO" );
                    }
                    default:
                        throw new NotImplementedException( "Cypher Function to alg conversion missing: " + func.getOperatorName() );
                }
            }

            // name -> aggregate
            // renaming of the field
            if ( expression.getType() == ExpressionType.AGGREGATE ) {
                return ((CypherAggregate) expression).getAggregate( context, name );
            }

            return Pair.of( PolyString.of( name ), expression.getRex( context, type ).right );
        } else {
            if ( expression.getType() == ExpressionType.AGGREGATE ) {
                return ((CypherAggregate) expression).getAggregate( context, null );
            }
            return expression.getRex( context, type );
        }
    }

}

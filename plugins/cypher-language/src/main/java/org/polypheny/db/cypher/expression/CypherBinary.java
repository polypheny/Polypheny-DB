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
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.BinaryOperator;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

@Getter
public class CypherBinary extends CypherExpression {

    private final OperatorName op;
    private final CypherExpression left;
    private final CypherExpression right;


    public CypherBinary( ParserPos pos, OperatorName op, CypherExpression left, CypherExpression right ) {
        super( pos );
        this.op = op;
        this.left = left;
        this.right = right;
    }


    @Override
    public Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {

        Pair<PolyString, RexNode> left = this.left.getRex( context, type );
        Pair<PolyString, RexNode> right = this.right.getRex( context, type );

        if ( (op == OperatorName.EQUALS || op == OperatorName.NOT_EQUALS)
                && (left.right.getType().getPolyType() == PolyType.ANY || right.right.getType().getPolyType() == PolyType.ANY) ) {
            // Property values can have different types in different rows of the same graph.
            OperatorName dynamicComparison = op == OperatorName.EQUALS ? OperatorName.CYPHER_EQUALS : OperatorName.CYPHER_NOT_EQUALS;
            return Pair.of( null, context.rexBuilder.makeCall(
                    context.typeFactory.createTypeWithNullability( context.booleanType, true ),
                    OperatorRegistry.get( QueryLanguage.from( "cypher" ), dynamicComparison ), List.of( left.right, right.right ) ) );
        }
        if ( OperatorRegistry.get( op ) instanceof BinaryOperator ) {
            // when we have a binary comparison, we have to adjust potential mismatches
            // this is the case if one of the sides is a literal, which has an explicit type
            if ( left.right.getType().getPolyType().getFamily() != right.right.getType().getPolyType().getFamily() ) {
                if ( left.right.isA( Kind.LITERAL ) && right.right.isA( Kind.LITERAL ) ) {
                    throw new UnsupportedOperationException( "Both binary sides define non matching types" );
                } else if ( left.right.isA( Kind.LITERAL ) ) {
                    // left defines type
                    right = Pair.of( right.left, context.rexBuilder.makeCast( left.right.getType(), right.right ) );
                } else {
                    // right defines type
                    left = Pair.of( left.left, context.rexBuilder.makeCast( right.right.getType(), left.right ) );
                }
            }
        }

        return Pair.of( null, context.rexBuilder.makeCall( context.booleanType, OperatorRegistry.get( op ), List.of( left.right, right.right ) ) );
    }

}

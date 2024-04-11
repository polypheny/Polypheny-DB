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
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;

@Getter
public class CypherGate extends CypherExpression {

    private final Gate gate;
    private List<CypherExpression> expressions;
    private CypherExpression left;
    private CypherExpression right;


    public CypherGate( ParserPos pos, Gate gate, CypherExpression left, CypherExpression right ) {
        super( pos );
        this.gate = gate;
        this.left = left;
        this.right = right;
    }


    public CypherGate( Gate gate, List<CypherExpression> expressions ) {
        super( ParserPos.ZERO );
        this.gate = gate;
        this.expressions = expressions;
    }


    @Override
    public Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {
        OperatorName operatorName = null;
        switch ( gate ) {
            case OR:
                operatorName = OperatorName.OR;
                break;
            case AND:
                operatorName = OperatorName.AND;
                break;
            case XOR:
                throw new UnsupportedOperationException();
            case NOT:
                return handleSingular( context, type, OperatorName.NOT );
        }

        return Pair.of( null, new RexCall(
                context.booleanType,
                OperatorRegistry.get( operatorName ),
                List.of( left.getRex( context, type ).right, right.getRex( context, type ).right ) ) );

    }


    private Pair<PolyString, RexNode> handleSingular( CypherContext context, RexType type, OperatorName operatorName ) {
        return Pair.of( null, new RexCall(
                context.booleanType,
                OperatorRegistry.get( operatorName ),
                List.of( left.getRex( context, type ).right ) ) );
    }


    public enum Gate {
        OR,
        AND,
        XOR,
        NOT
    }

}

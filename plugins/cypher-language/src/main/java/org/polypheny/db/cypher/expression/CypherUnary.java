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

import lombok.Getter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.functions.CypherFunctions;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.Pair;

@Getter
public class CypherUnary extends CypherExpression {

    private final OperatorName unaryPlus;
    private final CypherExpression expression;


    public CypherUnary( ParserPos pos, OperatorName unaryPlus, CypherExpression expression ) {
        super( pos );
        this.unaryPlus = unaryPlus;
        this.expression = expression;
    }


    @Override
    public Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {
        return Pair.of( null, CypherFormula.createCall( context, unaryPlus,
                context.rexBuilder.makeLiteral( PolyLong.of( 0 ) ), expression.getRex( context, type ).right ) );
    }


    @Override
    public PolyValue getComparable() {
        return CypherFunctions.arithmetic( PolyString.of( unaryPlus.name() ), PolyLong.of( 0 ), expression.getComparable() );
    }

}

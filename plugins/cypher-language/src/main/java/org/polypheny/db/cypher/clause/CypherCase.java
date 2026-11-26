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

package org.polypheny.db.cypher.clause;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Pair;


@Getter
public class CypherCase extends CypherExpression {

    private final CypherExpression condition;
    private final List<CypherExpression> when;
    private final List<CypherExpression> then;
    private final CypherExpression elseCase;


    public CypherCase(
            ParserPos pos,
            CypherExpression condition,
            List<CypherExpression> when,
            List<CypherExpression> then,
            CypherExpression elseCase ) {
        super( pos );
        this.condition = condition;
        this.when = when;
        this.then = then;
        this.elseCase = elseCase;
    }


    @Override
    protected Pair<PolyString, RexNode> toRex( CypherContext context, RexType type ) {
        List<RexNode> operands = new ArrayList<>();
        List<AlgDataType> resultTypes = new ArrayList<>();
        if ( condition != null ) {
            operands.add( condition.getRex( context, RexType.PROJECT ).right );
        }
        RexType testType = condition == null ? RexType.FILTER : RexType.PROJECT;
        for ( int i = 0; i < when.size(); i++ ) {
            RexNode test = when.get( i ).getRex( context, testType ).right;
            RexNode value = then.get( i ).getRex( context, RexType.PROJECT ).right;
            operands.add( condition == null ? context.booleanPredicate( test ) : test );
            operands.add( value );
            resultTypes.add( value.getType() );
        }
        RexNode fallback = elseCase == null ? context.rexBuilder.constantNull() : elseCase.getRex( context, RexType.PROJECT ).right;
        operands.add( fallback );
        resultTypes.add( fallback.getType() );
        AlgDataType resultType = PolyTypeUtil.commonDynamicType( context.typeFactory, resultTypes );
        return Pair.of( null, context.rexBuilder.makeCall( resultType,
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_CASE ), operands ) );
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.CASE;
    }

}

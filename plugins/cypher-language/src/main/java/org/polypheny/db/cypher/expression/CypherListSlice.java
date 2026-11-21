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

import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.Pair;

@Getter
public class CypherListSlice extends CypherExpression {

    private final CypherExpression subject;
    private final CypherExpression from;
    private final CypherExpression to;


    // 1..3
    public CypherListSlice( ParserPos pos, CypherExpression subject, CypherExpression from, CypherExpression to ) {
        super( pos );
        this.subject = subject;
        this.from = from;
        this.to = to;
    }


    @Override
    public Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {
        RexNode value = subject.getRex( context, type ).right;
        requireListOperand( value, "Slicing requires a list." );
        RexNode lower = from == null ? context.rexBuilder.makeLiteral( PolyLong.of( 0 ) ) : from.getRex( context, type ).right;
        RexNode upper = to == null ? context.rexBuilder.makeLiteral( PolyLong.of( Long.MAX_VALUE ) ) : to.getRex( context, type ).right;
        CypherListLookup.requireIntegerIndex( lower );
        CypherListLookup.requireIntegerIndex( upper );

        AlgDataType listType = value.getType() instanceof ArrayType array
                ? context.typeFactory.createArrayType( array.getComponentType(), -1, array.getDimension() )
                : context.typeFactory.createArrayType( context.typeFactory.createPolyType( PolyType.ANY ), -1, 1 );
        return Pair.of( null, context.rexBuilder.makeCall( context.typeFactory.createTypeWithNullability( listType, true ),
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_SLICE ), List.of( value, lower, upper ) ) );
    }

}

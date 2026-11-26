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
import org.polypheny.db.cypher.cypher2alg.CypherSyntaxException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.functions.CypherFunctions;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

@Getter
public class CypherFormula extends CypherExpression {


    private final OperatorName op;
    private final CypherExpression left;
    private final CypherExpression right;


    public CypherFormula( ParserPos pos, OperatorName operatorName, CypherExpression left, CypherExpression right ) {
        super( pos );
        op = operatorName;
        this.left = left;
        this.right = right;
    }


    @Override
    protected Pair<PolyString, RexNode> toRex( CypherContext context, RexType type ) {
        return Pair.of( null, createCall( context, op, left.getRex( context, type ).right, right.getRex( context, type ).right ) );
    }


    @Override
    public PolyValue getComparable() {
        return CypherFunctions.arithmetic( PolyString.of( op.name() ), left.getComparable(), right.getComparable() );
    }


    static RexNode createCall( CypherContext context, OperatorName op, RexNode left, RexNode right ) {
        AlgDataType result = resultType( context, op, left.getType(), right.getType() );
        return context.rexBuilder.makeCall( context.typeFactory.createTypeWithNullability( result, true ),
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_ARITHMETIC ),
                List.of( context.rexBuilder.makeLiteral( op.name() ), left, right ) );
    }


    private static AlgDataType resultType( CypherContext context, OperatorName op, AlgDataType left, AlgDataType right ) {
        PolyType a = left.getPolyType();
        PolyType b = right.getPolyType();
        PolyType result = PolyType.ANY;
        if ( op == OperatorName.PLUS && (a == PolyType.ARRAY || b == PolyType.ARRAY) ) {
            AlgDataType x = a == PolyType.ARRAY ? left.getComponentType() : left;
            AlgDataType y = b == PolyType.ARRAY ? right.getComponentType() : right;
            AlgDataType component = listComponentType( context, left, right, x, y );
            return context.typeFactory.createArrayType( component, -1, 1 );
        }
        if ( op == OperatorName.PLUS && ((PolyType.CHAR_TYPES.contains( a ) && (PolyType.CHAR_TYPES.contains( b ) || PolyType.NUMERIC_TYPES.contains( b )))
                || (PolyType.CHAR_TYPES.contains( b ) && PolyType.NUMERIC_TYPES.contains( a ))) ) {
            result = PolyType.VARCHAR;
        } else if ( PolyType.NUMERIC_TYPES.contains( a ) && PolyType.NUMERIC_TYPES.contains( b ) ) {
            if ( op == OperatorName.POWER || PolyType.APPROX_TYPES.contains( a ) || PolyType.APPROX_TYPES.contains( b ) ) {
                result = PolyType.DOUBLE;
            } else {
                result = a == PolyType.DECIMAL || b == PolyType.DECIMAL ? PolyType.DECIMAL : PolyType.BIGINT;
            }
        } else if ( !requiresRuntimeTypeCheck( a ) && !requiresRuntimeTypeCheck( b ) ) {
            throw new CypherSyntaxException( "InvalidArgumentType: Invalid operand types for " + op + ": " + a + " and " + b + "." );
        }
        return context.typeFactory.createPolyType( result );
    }


    private static boolean requiresRuntimeTypeCheck( PolyType type ) {
        return switch ( type ) {
            case ANY, NULL, DATE, TIME, TIMESTAMP, INTERVAL, CYPHER_TEMPORAL -> true;
            default -> false;
        };
    }


    private static AlgDataType listComponentType( CypherContext context, AlgDataType left, AlgDataType right, AlgDataType x, AlgDataType y ) {
        if ( left instanceof ArrayType array && array.getCardinality() == 0 ) {
            return y;
        }
        if ( right instanceof ArrayType array && array.getCardinality() == 0 ) {
            return x;
        }
        if ( PolyTypeUtil.equalSansNullability( context.typeFactory, x, y ) ) {
            return context.typeFactory.createTypeWithNullability( x, x.isNullable() || y.isNullable() );
        }
        // Concatenation preserves each element's type, including mixed integers and floats.
        return context.typeFactory.createPolyType( PolyType.ANY );
    }

}

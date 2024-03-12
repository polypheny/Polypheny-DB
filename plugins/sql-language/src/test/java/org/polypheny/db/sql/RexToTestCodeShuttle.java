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

package org.polypheny.db.sql;


import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.type.PolyType;


/**
 *  For instance, it converts {@code AND(=(?0.bool0, true), =(?0.bool1, true))} to {@code isTrue(and(eq(vBool(0), trueLiteral), eq(vBool(1), trueLiteral)))}.
 */
public class RexToTestCodeShuttle extends RexVisitorImpl<String> {

    private static final Map<Operator, String> OP_METHODS =
            ImmutableMap.<Operator, String>builder()
                    .put( OperatorRegistry.get( OperatorName.AND ), "and" )
                    .put( OperatorRegistry.get( OperatorName.OR ), "or" )
                    .put( OperatorRegistry.get( OperatorName.CASE ), "case_" )
                    .put( OperatorRegistry.get( OperatorName.COALESCE ), "coalesce" )
                    .put( OperatorRegistry.get( OperatorName.IS_NULL ), "isNull" )
                    .put( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), "isNotNull" )
                    .put( OperatorRegistry.get( OperatorName.IS_UNKNOWN ), "isUnknown" )
                    .put( OperatorRegistry.get( OperatorName.IS_TRUE ), "isTrue" )
                    .put( OperatorRegistry.get( OperatorName.IS_NOT_TRUE ), "isNotTrue" )
                    .put( OperatorRegistry.get( OperatorName.IS_FALSE ), "isFalse" )
                    .put( OperatorRegistry.get( OperatorName.IS_NOT_FALSE ), "isNotFalse" )
                    .put( OperatorRegistry.get( OperatorName.IS_DISTINCT_FROM ), "isDistinctFrom" )
                    .put( OperatorRegistry.get( OperatorName.IS_NOT_DISTINCT_FROM ), "isNotDistinctFrom" )
                    .put( OperatorRegistry.get( OperatorName.NULLIF ), "nullIf" )
                    .put( OperatorRegistry.get( OperatorName.NOT ), "not" )
                    .put( OperatorRegistry.get( OperatorName.GREATER_THAN ), "gt" )
                    .put( OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ), "ge" )
                    .put( OperatorRegistry.get( OperatorName.LESS_THAN ), "lt" )
                    .put( OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ), "le" )
                    .put( OperatorRegistry.get( OperatorName.EQUALS ), "eq" )
                    .put( OperatorRegistry.get( OperatorName.NOT_EQUALS ), "ne" )
                    .put( OperatorRegistry.get( OperatorName.PLUS ), "plus" )
                    .put( OperatorRegistry.get( OperatorName.UNARY_PLUS ), "unaryPlus" )
                    .put( OperatorRegistry.get( OperatorName.MINUS ), "sub" )
                    .put( OperatorRegistry.get( OperatorName.UNARY_MINUS ), "unaryMinus" )
                    .build();


    protected RexToTestCodeShuttle() {
        super( true );
    }


    @Override
    public String visitCall( RexCall call ) {
        Operator operator = call.getOperator();
        String method = OP_METHODS.get( operator );

        StringBuilder sb = new StringBuilder();
        if ( method != null ) {
            sb.append( method );
            sb.append( '(' );
        } else {
            sb.append( "rexBuilder.makeCall(" );
            sb.append( "SqlStdOperatorTable." );
            sb.append( operator.getName().replace( ' ', '_' ) );
            sb.append( ", " );
        }
        List<RexNode> operands = call.getOperands();
        for ( int i = 0; i < operands.size(); i++ ) {
            RexNode operand = operands.get( i );
            if ( i > 0 ) {
                sb.append( ", " );
            }
            sb.append( operand.accept( this ) );
        }
        sb.append( ')' );
        return sb.toString();
    }


    @Override
    public String visitLiteral( RexLiteral literal ) {
        AlgDataType type = literal.getType();

        if ( type.getPolyType() == PolyType.BOOLEAN ) {
            if ( literal.isNull() ) {
                return "nullBool";
            }
            return literal.toString() + "Literal";
        }
        if ( type.getPolyType() == PolyType.INTEGER ) {
            if ( literal.isNull() ) {
                return "nullInt";
            }
            return "literal(" + literal.getValue() + ")";
        }
        if ( type.getPolyType() == PolyType.VARCHAR ) {
            if ( literal.isNull() ) {
                return "nullVarchar";
            }
        }
        return "/*" + literal.getPolyType().getName() + "*/" + literal.toString();
    }


    @Override
    public String visitFieldAccess( RexFieldAccess fieldAccess ) {
        StringBuilder sb = new StringBuilder();
        sb.append( "v" );
        AlgDataType type = fieldAccess.getType();
        switch ( type.getPolyType() ) {
            case BOOLEAN:
                sb.append( "Bool" );
                break;
            case INTEGER:
                sb.append( "Int" );
                break;
            case VARCHAR:
                sb.append( "Varchar" );
                break;
        }
        if ( !type.isNullable() ) {
            sb.append( "NotNull" );
        }
        sb.append( "(" );
        sb.append( fieldAccess.getField().getIndex() % 10 );
        sb.append( ")" );
        return sb.toString();
    }

}


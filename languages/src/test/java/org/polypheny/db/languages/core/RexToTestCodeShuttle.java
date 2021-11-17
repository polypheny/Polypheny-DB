/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.languages.core;


import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.core.Operator;
import org.polypheny.db.core.StdOperatorRegistry;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.type.PolyType;


/**
 * Converts {@link RexNode} into a string form usable for inclusion into {@link RexProgramFuzzyTest}. For instance, it converts {@code AND(=(?0.bool0, true), =(?0.bool1, true))} to {@code isTrue(and(eq(vBool(0), trueLiteral), eq(vBool(1), trueLiteral)))}.
 */
public class RexToTestCodeShuttle extends RexVisitorImpl<String> {

    private static final Map<Operator, String> OP_METHODS =
            ImmutableMap.<Operator, String>builder()
                    .put( StdOperatorRegistry.get( "AND" ), "and" )
                    .put( StdOperatorRegistry.get( "OR" ), "or" )
                    .put( StdOperatorRegistry.get( "CASE" ), "case_" )
                    .put( StdOperatorRegistry.get( "COALESCE" ), "coalesce" )
                    .put( StdOperatorRegistry.get( "IS_NULL" ), "isNull" )
                    .put( StdOperatorRegistry.get( "IS_NOT_NULL" ), "isNotNull" )
                    .put( StdOperatorRegistry.get( "IS_UNKNOWN" ), "isUnknown" )
                    .put( StdOperatorRegistry.get( "IS_TRUE" ), "isTrue" )
                    .put( StdOperatorRegistry.get( "IS_NOT_TRUE" ), "isNotTrue" )
                    .put( StdOperatorRegistry.get( "IS_FALSE" ), "isFalse" )
                    .put( StdOperatorRegistry.get( "IS_NOT_FALSE" ), "isNotFalse" )
                    .put( StdOperatorRegistry.get( "IS_DISTINCT_FROM" ), "isDistinctFrom" )
                    .put( StdOperatorRegistry.get( "IS_NOT_DISTINCT_FROM" ), "isNotDistinctFrom" )
                    .put( StdOperatorRegistry.get( "NULLIF" ), "nullIf" )
                    .put( StdOperatorRegistry.get( "NOT" ), "not" )
                    .put( StdOperatorRegistry.get( "GREATER_THAN" ), "gt" )
                    .put( StdOperatorRegistry.get( "GREATER_THAN_OR_EQUAL" ), "ge" )
                    .put( StdOperatorRegistry.get( "LESS_THAN" ), "lt" )
                    .put( StdOperatorRegistry.get( "LESS_THAN_OR_EQUAL" ), "le" )
                    .put( StdOperatorRegistry.get( "EQUALS" ), "eq" )
                    .put( StdOperatorRegistry.get( "NOT_EQUALS" ), "ne" )
                    .put( StdOperatorRegistry.get( "PLUS" ), "plus" )
                    .put( StdOperatorRegistry.get( "UNARY_PLUS" ), "unaryPlus" )
                    .put( StdOperatorRegistry.get( "MINUS" ), "sub" )
                    .put( StdOperatorRegistry.get( "UNARY_MINUS" ), "unaryMinus" )
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
        RelDataType type = literal.getType();

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
        return "/*" + literal.getTypeName().getName() + "*/" + literal.toString();
    }


    @Override
    public String visitFieldAccess( RexFieldAccess fieldAccess ) {
        StringBuilder sb = new StringBuilder();
        sb.append( "v" );
        RelDataType type = fieldAccess.getType();
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


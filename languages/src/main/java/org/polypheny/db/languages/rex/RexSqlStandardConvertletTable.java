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

package org.polypheny.db.languages.rex;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.polypheny.db.core.Node;
import org.polypheny.db.core.NodeList;
import org.polypheny.db.core.Operator;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.core.StdOperatorRegistry;
import org.polypheny.db.languages.sql.SqlBasicCall;
import org.polypheny.db.languages.sql.SqlCall;
import org.polypheny.db.languages.sql.SqlDataTypeSpec;
import org.polypheny.db.languages.sql.SqlNode;
import org.polypheny.db.languages.sql.SqlNodeList;
import org.polypheny.db.languages.sql.SqlOperator;
import org.polypheny.db.languages.sql.fun.SqlCaseOperator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyTypeUtil;


/**
 * Standard implementation of {@link RexSqlConvertletTable}.
 */
public class RexSqlStandardConvertletTable extends RexSqlReflectiveConvertletTable {


    public RexSqlStandardConvertletTable() {
        super();

        // Register convertlets

        registerEquivOp( StdOperatorRegistry.get( "GREATER_THAN_OR_EQUAL" ) );
        registerEquivOp( StdOperatorRegistry.get( "GREATER_THAN" ) );
        registerEquivOp( StdOperatorRegistry.get( "LESS_THAN_OR_EQUAL" ) );
        registerEquivOp( StdOperatorRegistry.get( "LESS_THAN" ) );
        registerEquivOp( StdOperatorRegistry.get( "EQUALS" ) );
        registerEquivOp( StdOperatorRegistry.get( "NOT_EQUALS" ) );
        registerEquivOp( StdOperatorRegistry.get( "AND" ) );
        registerEquivOp( StdOperatorRegistry.get( "OR" ) );
        registerEquivOp( StdOperatorRegistry.get( "NOT_IN" ) );
        registerEquivOp( StdOperatorRegistry.get( "IN" ) );
        registerEquivOp( StdOperatorRegistry.get( "LIKE" ) );
        registerEquivOp( StdOperatorRegistry.get( "NOT_LIKE" ) );
        registerEquivOp( StdOperatorRegistry.get( "SIMILAR_TO" ) );
        registerEquivOp( StdOperatorRegistry.get( "NOT_SIMILAR_TO" ) );
        registerEquivOp( StdOperatorRegistry.get( "PLUS" ) );
        registerEquivOp( StdOperatorRegistry.get( "MINUS" ) );
        registerEquivOp( StdOperatorRegistry.get( "MULTIPLE" ) );
        registerEquivOp( StdOperatorRegistry.get( "DIVIDE" ) );

        registerEquivOp( StdOperatorRegistry.get( "NOT" ) );

        registerEquivOp( StdOperatorRegistry.get( "IS_NOT_NULL" ) );
        registerEquivOp( StdOperatorRegistry.get( "IS_NULL" ) );

        registerEquivOp( StdOperatorRegistry.get( "IS_NOT_TRUE" ) );
        registerEquivOp( StdOperatorRegistry.get( "IS_TRUE" ) );

        registerEquivOp( StdOperatorRegistry.get( "IS_NOT_FALSE" ) );
        registerEquivOp( StdOperatorRegistry.get( "IS_FALSE" ) );

        registerEquivOp( StdOperatorRegistry.get( "IS_NOT_UNKNOWN" ) );
        registerEquivOp( StdOperatorRegistry.get( "IS_UNKNOWN" ) );

        registerEquivOp( StdOperatorRegistry.get( "UNARY_MINUS" ) );
        registerEquivOp( StdOperatorRegistry.get( "UNARY_PLUS" ) );

        registerCaseOp( StdOperatorRegistry.get( "CASE" ) );
        registerEquivOp( StdOperatorRegistry.get( "CONCAT" ) );

        registerEquivOp( StdOperatorRegistry.get( "BETWEEN" ) );
        registerEquivOp( StdOperatorRegistry.get( "SYMMETRIC_BETWEEN" ) );

        registerEquivOp( StdOperatorRegistry.get( "NOT_BETWEEN" ) );
        registerEquivOp( StdOperatorRegistry.get( "SYMMETRIC_NOT_BETWEEN" ) );

        registerEquivOp( StdOperatorRegistry.get( "IS_NOT_DISTINCT_FROM" ) );
        registerEquivOp( StdOperatorRegistry.get( "IS_DISTINCT_FROM" ) );

        registerEquivOp( StdOperatorRegistry.get( "MINUS_DATE" ) );
        registerEquivOp( StdOperatorRegistry.get( "EXTRACT" ) );

        registerEquivOp( StdOperatorRegistry.get( "SUBSTRING" ) );
        registerEquivOp( StdOperatorRegistry.get( "CONVERT" ) );
        registerEquivOp( StdOperatorRegistry.get( "TRANSLATE" ) );
        registerEquivOp( StdOperatorRegistry.get( "OVERLAY" ) );
        registerEquivOp( StdOperatorRegistry.get( "TRIM" ) );
        registerEquivOp( StdOperatorRegistry.get( "ORACLE_TRANSLATE3" ) );
        registerEquivOp( StdOperatorRegistry.get( "POSITION" ) );
        registerEquivOp( StdOperatorRegistry.get( "CHAR_LENGTH" ) );
        registerEquivOp( StdOperatorRegistry.get( "CHARACTER_LENGTH" ) );
        registerEquivOp( StdOperatorRegistry.get( "UPPER" ) );
        registerEquivOp( StdOperatorRegistry.get( "LOWER" ) );
        registerEquivOp( StdOperatorRegistry.get( "INITCAP" ) );

        registerEquivOp( StdOperatorRegistry.get( "POWER" ) );
        registerEquivOp( StdOperatorRegistry.get( "SQRT" ) );
        registerEquivOp( StdOperatorRegistry.get( "MOD" ) );
        registerEquivOp( StdOperatorRegistry.get( "LN" ) );
        registerEquivOp( StdOperatorRegistry.get( "LOG10" ) );
        registerEquivOp( StdOperatorRegistry.get( "ABS" ) );
        registerEquivOp( StdOperatorRegistry.get( "EXP" ) );
        registerEquivOp( StdOperatorRegistry.get( "FLOOR" ) );
        registerEquivOp( StdOperatorRegistry.get( "CEIL" ) );

        registerEquivOp( StdOperatorRegistry.get( "NULLIF" ) );
        registerEquivOp( StdOperatorRegistry.get( "COALESCE" ) );

        registerTypeAppendOp( StdOperatorRegistry.get( "CASE" ) );
    }


    /**
     * Converts a call to an operator into a {@link SqlCall} to the same operator.
     *
     * Called automatically via reflection.
     *
     * @param converter Converter
     * @param call Call
     * @return Sql call
     */
    public SqlNode convertCall( RexToSqlNodeConverter converter, RexCall call ) {
        if ( get( call ) == null ) {
            return null;
        }

        final SqlOperator op = (SqlOperator) call.getOperator();
        final List<RexNode> operands = call.getOperands();

        final SqlNode[] exprs = (SqlNode[]) convertExpressionList( converter, operands );
        if ( exprs == null ) {
            return null;
        }
        return new SqlBasicCall( op, exprs, ParserPos.ZERO );
    }


    private Node[] convertExpressionList( RexToSqlNodeConverter converter, List<RexNode> nodes ) {
        final SqlNode[] exprs = new SqlNode[nodes.size()];
        for ( int i = 0; i < nodes.size(); i++ ) {
            RexNode node = nodes.get( i );
            exprs[i] = converter.convertNode( node );
            if ( exprs[i] == null ) {
                return null;
            }
        }
        return exprs;
    }


    /**
     * Creates and registers a convertlet for an operator in which the SQL and Rex representations are structurally equivalent.
     *
     * @param op operator instance
     */
    protected void registerEquivOp( Operator op ) {
        registerOp( op, new EquivConvertlet( op ) );
    }


    /**
     * Creates and registers a convertlet for an operator in which the SQL representation needs the result type appended as an extra argument (e.g. CAST).
     *
     * @param op operator instance
     */
    private void registerTypeAppendOp( final Operator op ) {
        registerOp(
                op, ( converter, call ) -> {
                    SqlNode[] operands = (SqlNode[]) convertExpressionList( converter, call.operands );
                    if ( operands == null ) {
                        return null;
                    }
                    List<SqlNode> operandList = new ArrayList<>( Arrays.asList( operands ) );
                    SqlDataTypeSpec typeSpec = (SqlDataTypeSpec) PolyTypeUtil.convertTypeToSpec( call.getType() );
                    operandList.add( typeSpec );
                    return new SqlBasicCall(
                            (SqlOperator) op,
                            operandList.toArray( new SqlNode[0] ),
                            ParserPos.ZERO );
                } );
    }


    /**
     * Creates and registers a convertlet for the CASE operator, which takes different forms for SQL vs Rex.
     *
     * @param op instance of CASE operator
     */
    private void registerCaseOp( final Operator op ) {
        registerOp(
                op, ( converter, call ) -> {
                    assert op instanceof SqlCaseOperator;
                    SqlNode[] operands = (SqlNode[]) convertExpressionList( converter, call.operands );
                    if ( operands == null ) {
                        return null;
                    }
                    SqlNodeList whenList = new SqlNodeList( ParserPos.ZERO );
                    NodeList thenList = new SqlNodeList( ParserPos.ZERO );
                    int i = 0;
                    while ( i < operands.length - 1 ) {
                        whenList.add( operands[i] );
                        ++i;
                        thenList.add( operands[i] );
                        ++i;
                    }
                    Node elseExpr = operands[i];
                    return op.createCall( null, ParserPos.ZERO, null, whenList, thenList, elseExpr );
                } );
    }


    /**
     * Convertlet that converts a {@link SqlCall} to a {@link RexCall} of the same operator.
     */
    private class EquivConvertlet implements RexSqlConvertlet {

        private final Operator op;


        EquivConvertlet( Operator op ) {
            this.op = op;
        }


        @Override
        public Node convertCall( RexToSqlNodeConverter converter, RexCall call ) {
            Node[] operands = convertExpressionList( converter, call.operands );
            if ( operands == null ) {
                return null;
            }
            return new SqlBasicCall( (SqlOperator) op, (SqlNode[]) operands, ParserPos.ZERO );
        }

    }

}

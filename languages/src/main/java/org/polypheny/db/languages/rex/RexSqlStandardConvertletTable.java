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
import org.polypheny.db.core.nodes.Node;
import org.polypheny.db.core.nodes.NodeList;
import org.polypheny.db.core.nodes.Operator;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.StdOperatorRegistry;
import org.polypheny.db.core.operators.OperatorName;
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

        registerEquivOp( StdOperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.GREATER_THAN ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.LESS_THAN ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.EQUALS ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.NOT_EQUALS ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.AND ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.OR ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.NOT_IN ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.IN ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.LIKE ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.NOT_LIKE ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.SIMILAR_TO ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.NOT_SIMILAR_TO ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.PLUS ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.MINUS ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.MULTIPLY ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.DIVIDE ) );

        registerEquivOp( StdOperatorRegistry.get( OperatorName.NOT ) );

        registerEquivOp( StdOperatorRegistry.get( OperatorName.IS_NOT_NULL ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.IS_NULL ) );

        registerEquivOp( StdOperatorRegistry.get( OperatorName.IS_NOT_TRUE ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.IS_TRUE ) );

        registerEquivOp( StdOperatorRegistry.get( OperatorName.IS_NOT_FALSE ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.IS_FALSE ) );

        registerEquivOp( StdOperatorRegistry.get( OperatorName.IS_NOT_UNKNOWN ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.IS_UNKNOWN ) );

        registerEquivOp( StdOperatorRegistry.get( OperatorName.UNARY_MINUS ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.UNARY_PLUS ) );

        registerCaseOp( StdOperatorRegistry.get( OperatorName.CASE ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.CONCAT ) );

        registerEquivOp( StdOperatorRegistry.get( OperatorName.BETWEEN ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.SYMMETRIC_BETWEEN ) );

        registerEquivOp( StdOperatorRegistry.get( OperatorName.NOT_BETWEEN ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.SYMMETRIC_NOT_BETWEEN ) );

        registerEquivOp( StdOperatorRegistry.get( OperatorName.IS_NOT_DISTINCT_FROM ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.IS_DISTINCT_FROM ) );

        registerEquivOp( StdOperatorRegistry.get( OperatorName.MINUS_DATE ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.EXTRACT ) );

        registerEquivOp( StdOperatorRegistry.get( OperatorName.SUBSTRING ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.CONVERT ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.TRANSLATE ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.OVERLAY ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.TRIM ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.ORACLE_TRANSLATE3 ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.POSITION ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.CHAR_LENGTH ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.CHARACTER_LENGTH ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.UPPER ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.LOWER ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.INITCAP ) );

        registerEquivOp( StdOperatorRegistry.get( OperatorName.POWER ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.SQRT ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.MOD ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.LN ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.LOG10 ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.ABS ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.EXP ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.FLOOR ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.CEIL ) );

        registerEquivOp( StdOperatorRegistry.get( OperatorName.NULLIF ) );
        registerEquivOp( StdOperatorRegistry.get( OperatorName.COALESCE ) );

        registerTypeAppendOp( StdOperatorRegistry.get( OperatorName.CAST ) );
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

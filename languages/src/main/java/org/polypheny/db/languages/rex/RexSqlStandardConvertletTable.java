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
import org.polypheny.db.core.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
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

        registerEquivOp( OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.GREATER_THAN ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.LESS_THAN ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.EQUALS ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.NOT_EQUALS ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.AND ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.OR ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.NOT_IN ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.IN ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.LIKE ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.NOT_LIKE ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.SIMILAR_TO ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.NOT_SIMILAR_TO ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.PLUS ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.MINUS ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.MULTIPLY ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.DIVIDE ) );

        registerEquivOp( OperatorRegistry.get( OperatorName.NOT ) );

        registerEquivOp( OperatorRegistry.get( OperatorName.IS_NOT_NULL ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.IS_NULL ) );

        registerEquivOp( OperatorRegistry.get( OperatorName.IS_NOT_TRUE ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.IS_TRUE ) );

        registerEquivOp( OperatorRegistry.get( OperatorName.IS_NOT_FALSE ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.IS_FALSE ) );

        registerEquivOp( OperatorRegistry.get( OperatorName.IS_NOT_UNKNOWN ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.IS_UNKNOWN ) );

        registerEquivOp( OperatorRegistry.get( OperatorName.UNARY_MINUS ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.UNARY_PLUS ) );

        registerCaseOp( OperatorRegistry.get( OperatorName.CASE ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.CONCAT ) );

        registerEquivOp( OperatorRegistry.get( OperatorName.BETWEEN ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.SYMMETRIC_BETWEEN ) );

        registerEquivOp( OperatorRegistry.get( OperatorName.NOT_BETWEEN ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.SYMMETRIC_NOT_BETWEEN ) );

        registerEquivOp( OperatorRegistry.get( OperatorName.IS_NOT_DISTINCT_FROM ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.IS_DISTINCT_FROM ) );

        registerEquivOp( OperatorRegistry.get( OperatorName.MINUS_DATE ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.EXTRACT ) );

        registerEquivOp( OperatorRegistry.get( OperatorName.SUBSTRING ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.CONVERT ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.TRANSLATE ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.OVERLAY ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.TRIM ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.ORACLE_TRANSLATE3 ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.POSITION ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.CHAR_LENGTH ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.CHARACTER_LENGTH ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.UPPER ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.LOWER ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.INITCAP ) );

        registerEquivOp( OperatorRegistry.get( OperatorName.POWER ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.SQRT ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.MOD ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.LN ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.LOG10 ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.ABS ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.EXP ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.FLOOR ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.CEIL ) );

        registerEquivOp( OperatorRegistry.get( OperatorName.NULLIF ) );
        registerEquivOp( OperatorRegistry.get( OperatorName.COALESCE ) );

        registerTypeAppendOp( OperatorRegistry.get( OperatorName.CAST ) );
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

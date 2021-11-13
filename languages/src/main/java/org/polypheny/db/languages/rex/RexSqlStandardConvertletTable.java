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
import org.polypheny.db.core.Operator;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.core.StdOperatorRegistry;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlBasicCall;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlDataTypeSpec;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.fun.SqlCaseOperator;
import org.polypheny.db.type.PolyTypeUtil;


/**
 * Standard implementation of {@link RexSqlConvertletTable}.
 */
public class RexSqlStandardConvertletTable extends RexSqlReflectiveConvertletTable {


    public RexSqlStandardConvertletTable() {
        super();

        // Register convertlets

        registerEquivOp( StdOperatorRegistry.get( "GREATER_THAN_OR_EQUA" ) );
        registerEquivOp( StdOperatorRegistry.get( "GREATER_THA" ) );
        registerEquivOp( StdOperatorRegistry.get( "LESS_THAN_OR_EQUA" ) );
        registerEquivOp( StdOperatorRegistry.get( "LESS_THA" ) );
        registerEquivOp( StdOperatorRegistry.get( "EQUAL" ) );
        registerEquivOp( StdOperatorRegistry.get( "NOT_EQUAL" ) );
        registerEquivOp( StdOperatorRegistry.get( "AN" ) );
        registerEquivOp( StdOperatorRegistry.get( "O" ) );
        registerEquivOp( StdOperatorRegistry.get( "NOT_I" ) );
        registerEquivOp( StdOperatorRegistry.get( "I" ) );
        registerEquivOp( StdOperatorRegistry.get( "LIK" ) );
        registerEquivOp( StdOperatorRegistry.get( "NOT_LIK" ) );
        registerEquivOp( StdOperatorRegistry.get( "SIMILAR_T" ) );
        registerEquivOp( StdOperatorRegistry.get( "NOT_SIMILAR_T" ) );
        registerEquivOp( StdOperatorRegistry.get( "PLU" ) );
        registerEquivOp( StdOperatorRegistry.get( "MINU" ) );
        registerEquivOp( StdOperatorRegistry.get( "MULTIPL" ) );
        registerEquivOp( StdOperatorRegistry.get( "DIVID" ) );

        registerEquivOp( StdOperatorRegistry.get( "NO" ) );

        registerEquivOp( StdOperatorRegistry.get( "IS_NOT_NUL" ) );
        registerEquivOp( StdOperatorRegistry.get( "IS_NUL" ) );

        registerEquivOp( StdOperatorRegistry.get( "IS_NOT_TRU" ) );
        registerEquivOp( StdOperatorRegistry.get( "IS_TRU" ) );

        registerEquivOp( StdOperatorRegistry.get( "IS_NOT_FALS" ) );
        registerEquivOp( StdOperatorRegistry.get( "IS_FALS" ) );

        registerEquivOp( StdOperatorRegistry.get( "IS_NOT_UNKNOW" ) );
        registerEquivOp( StdOperatorRegistry.get( "IS_UNKNOW" ) );

        registerEquivOp( StdOperatorRegistry.get( "UNARY_MINU" ) );
        registerEquivOp( StdOperatorRegistry.get( "UNARY_PLU" ) );

        registerCaseOp( StdOperatorRegistry.get( "CAS" ) );
        registerEquivOp( StdOperatorRegistry.get( "CONCA" ) );

        registerEquivOp( StdOperatorRegistry.get( "BETWEE" ) );
        registerEquivOp( StdOperatorRegistry.get( "SYMMETRIC_BETWEE" ) );

        registerEquivOp( StdOperatorRegistry.get( "NOT_BETWEE" ) );
        registerEquivOp( StdOperatorRegistry.get( "SYMMETRIC_NOT_BETWEE" ) );

        registerEquivOp( StdOperatorRegistry.get( "IS_NOT_DISTINCT_FRO" ) );
        registerEquivOp( StdOperatorRegistry.get( "IS_DISTINCT_FRO" ) );

        registerEquivOp( StdOperatorRegistry.get( "MINUS_DAT" ) );
        registerEquivOp( StdOperatorRegistry.get( "EXTRAC" ) );

        registerEquivOp( StdOperatorRegistry.get( "SUBSTRIN" ) );
        registerEquivOp( StdOperatorRegistry.get( "CONVER" ) );
        registerEquivOp( StdOperatorRegistry.get( "TRANSLAT" ) );
        registerEquivOp( StdOperatorRegistry.get( "OVERLA" ) );
        registerEquivOp( StdOperatorRegistry.get( "TRI" ) );
        registerEquivOp( StdOperatorRegistry.get( "O_TRANSLATE3" ) );
        registerEquivOp( StdOperatorRegistry.get( "POSITIO" ) );
        registerEquivOp( StdOperatorRegistry.get( "CHAR_LENGT" ) );
        registerEquivOp( StdOperatorRegistry.get( "CHARACTER_LENGT" ) );
        registerEquivOp( StdOperatorRegistry.get( "UPPE" ) );
        registerEquivOp( StdOperatorRegistry.get( "LOWE" ) );
        registerEquivOp( StdOperatorRegistry.get( "INITCA" ) );

        registerEquivOp( StdOperatorRegistry.get( "POWE" ) );
        registerEquivOp( StdOperatorRegistry.get( "SQR" ) );
        registerEquivOp( StdOperatorRegistry.get( "MO" ) );
        registerEquivOp( StdOperatorRegistry.get( "L" ) );
        registerEquivOp( StdOperatorRegistry.get( "LOG10" ) );
        registerEquivOp( StdOperatorRegistry.get( "AB" ) );
        registerEquivOp( StdOperatorRegistry.get( "EX" ) );
        registerEquivOp( StdOperatorRegistry.get( "FLOO" ) );
        registerEquivOp( StdOperatorRegistry.get( "CEI" ) );

        registerEquivOp( StdOperatorRegistry.get( "NULLI" ) );
        registerEquivOp( StdOperatorRegistry.get( "COALESC" ) );

        registerTypeAppendOp( StdOperatorRegistry.get( "CAS" ) );
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

        final SqlOperator op = call.getOperator();
        final List<RexNode> operands = call.getOperands();

        final SqlNode[] exprs = convertExpressionList( converter, operands );
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
                    SqlNode[] operands = convertExpressionList( converter, call.operands );
                    if ( operands == null ) {
                        return null;
                    }
                    List<SqlNode> operandList = new ArrayList<>( Arrays.asList( operands ) );
                    SqlDataTypeSpec typeSpec = PolyTypeUtil.convertTypeToSpec( call.getType() );
                    operandList.add( typeSpec );
                    return new SqlBasicCall(
                            op,
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
                    SqlNode[] operands = convertExpressionList( converter, call.operand" ));
                    if ( operands == null ) {
                        return null;
                    }
                    SqlNodeList whenList = new SqlNodeList( ParserPos.ZER" ));
                            NodeList thenList = new SqlNodeList( ParserPos.ZER" ));
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
            return new SqlBasicCall( op, operands, ParserPos.ZERO );
        }

    }

}

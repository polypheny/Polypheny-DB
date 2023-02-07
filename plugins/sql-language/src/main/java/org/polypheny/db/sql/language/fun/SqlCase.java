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

package org.polypheny.db.sql.language.fun;


import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.util.UnmodifiableArrayList;


/**
 * A <code>SqlCase</code> is a node of a parse tree which represents a case statement. It warrants its own node type just because we have a lot of methods to put somewhere.
 */
public class SqlCase extends SqlCall {

    SqlNode value;
    SqlNodeList whenList;
    SqlNodeList thenList;
    SqlNode elseExpr;


    /**
     * Creates a SqlCase expression.
     *
     * @param pos Parser position
     * @param value The value (null for boolean case)
     * @param whenList List of all WHEN expressions
     * @param thenList List of all THEN expressions
     * @param elseExpr The implicit or explicit ELSE expression
     */
    public SqlCase( ParserPos pos, SqlNode value, SqlNodeList whenList, SqlNodeList thenList, SqlNode elseExpr ) {
        super( pos );
        this.value = value;
        this.whenList = whenList;
        this.thenList = thenList;
        this.elseExpr = elseExpr;
    }


    /**
     * Creates a call to the switched form of the case operator, viz:
     *
     * <blockquote><code>CASE value<br>
     * WHEN whenList[0] THEN thenList[0]<br>
     * WHEN whenList[1] THEN thenList[1]<br>
     * ...<br>
     * ELSE elseClause<br>
     * END</code></blockquote>
     */
    public static SqlCase createSwitched( ParserPos pos, SqlNode value, SqlNodeList whenList, SqlNodeList thenList, SqlNode elseClause ) {
        if ( null != value ) {
            List<SqlNode> list = whenList.getSqlList();
            for ( int i = 0; i < list.size(); i++ ) {
                SqlNode e = list.get( i );
                final SqlCall call;
                if ( e instanceof SqlNodeList ) {
                    call = (SqlCall) OperatorRegistry.get( OperatorName.IN ).createCall( pos, value, e );
                } else {
                    call = (SqlCall) OperatorRegistry.get( OperatorName.EQUALS ).createCall( pos, value, e );
                }
                whenList.set( i, call );
            }
        }

        if ( null == elseClause ) {
            elseClause = SqlLiteral.createNull( pos );
        }

        return new SqlCase( pos, null, whenList, thenList, elseClause );
    }


    @Override
    public Kind getKind() {
        return Kind.CASE;
    }


    @Override
    public Operator getOperator() {
        return OperatorRegistry.get( OperatorName.CASE );
    }


    @Override
    public List<Node> getOperandList() {
        return UnmodifiableArrayList.of( value, whenList, thenList, elseExpr );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return UnmodifiableArrayList.of( value, whenList, thenList, elseExpr );
    }


    @Override
    public void setOperand( int i, Node operand ) {
        switch ( i ) {
            case 0:
                value = (SqlNode) operand;
                break;
            case 1:
                whenList = (SqlNodeList) operand;
                break;
            case 2:
                thenList = (SqlNodeList) operand;
                break;
            case 3:
                elseExpr = (SqlNode) operand;
                break;
            default:
                throw new AssertionError( i );
        }
    }


    public SqlNode getValueOperand() {
        return value;
    }


    public SqlNodeList getWhenOperands() {
        return whenList;
    }


    public SqlNodeList getThenOperands() {
        return thenList;
    }


    public SqlNode getElseOperand() {
        return elseExpr;
    }

}


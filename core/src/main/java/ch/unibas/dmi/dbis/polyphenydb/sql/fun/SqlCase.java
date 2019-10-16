/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.fun;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.util.UnmodifiableArrayList;
import java.util.List;


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
    public SqlCase( SqlParserPos pos, SqlNode value, SqlNodeList whenList, SqlNodeList thenList, SqlNode elseExpr ) {
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
    public static SqlCase createSwitched( SqlParserPos pos, SqlNode value, SqlNodeList whenList, SqlNodeList thenList, SqlNode elseClause ) {
        if ( null != value ) {
            List<SqlNode> list = whenList.getList();
            for ( int i = 0; i < list.size(); i++ ) {
                SqlNode e = list.get( i );
                final SqlCall call;
                if ( e instanceof SqlNodeList ) {
                    call = SqlStdOperatorTable.IN.createCall( pos, value, e );
                } else {
                    call = SqlStdOperatorTable.EQUALS.createCall( pos, value, e );
                }
                list.set( i, call );
            }
        }

        if ( null == elseClause ) {
            elseClause = SqlLiteral.createNull( pos );
        }

        return new SqlCase( pos, null, whenList, thenList, elseClause );
    }


    @Override
    public SqlKind getKind() {
        return SqlKind.CASE;
    }


    @Override
    public SqlOperator getOperator() {
        return SqlStdOperatorTable.CASE;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return UnmodifiableArrayList.of( value, whenList, thenList, elseExpr );
    }


    @Override
    public void setOperand( int i, SqlNode operand ) {
        switch ( i ) {
            case 0:
                value = operand;
                break;
            case 1:
                whenList = (SqlNodeList) operand;
                break;
            case 2:
                thenList = (SqlNodeList) operand;
                break;
            case 3:
                elseExpr = operand;
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


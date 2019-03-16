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

package ch.unibas.dmi.dbis.polyphenydb.sql;


import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlReturnTypeInference;
import org.apache.calcite.linq4j.Ord;


/**
 * A generalization of a binary operator to involve several (two or more) arguments, and keywords between each pair of arguments.
 *
 * For example, the <code>BETWEEN</code> operator is ternary, and has syntax <code><i>exp1</i> BETWEEN <i>exp2</i> AND <i>exp3</i></code>.
 */
public class SqlInfixOperator extends SqlSpecialOperator {

    private final String[] names;


    protected SqlInfixOperator(
            String[] names,
            SqlKind kind,
            int precedence,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker ) {
        super(
                names[0],
                kind,
                precedence,
                true,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker );
        assert names.length > 1;
        this.names = names;
    }


    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }


    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        assert call.operandCount() == names.length + 1;
        final boolean needWhitespace = needsSpace();
        for ( Ord<SqlNode> operand : Ord.zip( call.getOperandList() ) ) {
            if ( operand.i > 0 ) {
                writer.setNeedWhitespace( needWhitespace );
                writer.keyword( names[operand.i - 1] );
                writer.setNeedWhitespace( needWhitespace );
            }
            operand.e.unparse( writer, leftPrec, getLeftPrec() );
        }
    }
}

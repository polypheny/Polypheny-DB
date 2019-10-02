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


import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformance;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;


/**
 * Enumeration of possible syntactic types of {@link SqlOperator operators}.
 */
public enum SqlSyntax {
    /**
     * Function syntax, as in "Foo(x, y)".
     */
    FUNCTION {
        @Override
        public void unparse( SqlWriter writer, SqlOperator operator, SqlCall call, int leftPrec, int rightPrec ) {
            SqlUtil.unparseFunctionSyntax( operator, writer, call );
        }
    },

    /**
     * Function syntax, as in "Foo(x, y)", but uses "*" if there are no arguments, for example "COUNT(*)".
     */
    FUNCTION_STAR {
        @Override
        public void unparse( SqlWriter writer, SqlOperator operator, SqlCall call, int leftPrec, int rightPrec ) {
            SqlUtil.unparseFunctionSyntax( operator, writer, call );
        }
    },

    /**
     * Binary operator syntax, as in "x + y".
     */
    BINARY {
        @Override
        public void unparse( SqlWriter writer, SqlOperator operator, SqlCall call, int leftPrec, int rightPrec ) {
            SqlUtil.unparseBinarySyntax( operator, call, writer, leftPrec, rightPrec );
        }
    },

    /**
     * Prefix unary operator syntax, as in "- x".
     */
    PREFIX {
        @Override
        public void unparse( SqlWriter writer, SqlOperator operator, SqlCall call, int leftPrec, int rightPrec ) {
            assert call.operandCount() == 1;
            writer.keyword( operator.getName() );
            call.operand( 0 ).unparse( writer, operator.getLeftPrec(), operator.getRightPrec() );
        }
    },

    /**
     * Postfix unary operator syntax, as in "x ++".
     */
    POSTFIX {
        @Override
        public void unparse( SqlWriter writer, SqlOperator operator, SqlCall call, int leftPrec, int rightPrec ) {
            assert call.operandCount() == 1;
            call.operand( 0 ).unparse( writer, operator.getLeftPrec(), operator.getRightPrec() );
            writer.keyword( operator.getName() );
        }
    },

    /**
     * Special syntax, such as that of the SQL CASE operator, "CASE x WHEN 1 THEN 2 ELSE 3 END".
     */
    SPECIAL {
        @Override
        public void unparse( SqlWriter writer, SqlOperator operator, SqlCall call, int leftPrec, int rightPrec ) {
            // You probably need to override the operator's unparse method.
            throw Util.needToImplement( this );
        }
    },

    /**
     * Function syntax which takes no parentheses if there are no arguments, for example "CURRENTTIME".
     *
     * @see SqlConformance#allowNiladicParentheses()
     */
    FUNCTION_ID {
        @Override
        public void unparse( SqlWriter writer, SqlOperator operator, SqlCall call, int leftPrec, int rightPrec ) {
            SqlUtil.unparseFunctionSyntax( operator, writer, call );
        }
    },

    /**
     * Syntax of an internal operator, which does not appear in the SQL.
     */
    INTERNAL {
        @Override
        public void unparse( SqlWriter writer, SqlOperator operator, SqlCall call, int leftPrec, int rightPrec ) {
            throw new UnsupportedOperationException( "Internal operator '" + operator + "' " + "cannot be un-parsed" );
        }
    };


    /**
     * Converts a call to an operator of this syntax into a string.
     */
    public abstract void unparse( SqlWriter writer, SqlOperator operator, SqlCall call, int leftPrec, int rightPrec );
}


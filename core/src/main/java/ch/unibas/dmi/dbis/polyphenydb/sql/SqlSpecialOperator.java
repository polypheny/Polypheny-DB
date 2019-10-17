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


import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlReturnTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.util.PrecedenceClimbingParser;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.util.function.Predicate;


/**
 * Generic operator for nodes with special syntax.
 */
public class SqlSpecialOperator extends SqlOperator {


    public SqlSpecialOperator( String name, SqlKind kind ) {
        this( name, kind, 2 );
    }


    public SqlSpecialOperator( String name, SqlKind kind, int prec ) {
        this( name, kind, prec, true, null, null, null );
    }


    public SqlSpecialOperator( String name, SqlKind kind, int prec, boolean leftAssoc, SqlReturnTypeInference returnTypeInference, SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker ) {
        super(
                name,
                kind,
                prec,
                leftAssoc,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker );
    }


    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }


    /**
     * Reduces a list of operators and arguments according to the rules of precedence and associativity. Returns the ordinal of the node which replaced the expression.
     *
     * The default implementation throws {@link UnsupportedOperationException}.
     *
     * @param ordinal indicating the ordinal of the current operator in the list on which a possible reduction can be made
     * @param list List of alternating {@link ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserUtil.ToTreeListItem} and {@link SqlNode}
     * @return ordinal of the node which replaced the expression
     */
    public ReduceResult reduceExpr( int ordinal, TokenSequence list ) {
        throw Util.needToImplement( this );
    }


    /**
     * List of tokens: the input to a parser. Every token is either an operator ({@link SqlOperator}) or an expression ({@link SqlNode}), and every token has a position.
     */
    public interface TokenSequence {

        int size();

        SqlOperator op( int i );

        SqlParserPos pos( int i );

        boolean isOp( int i );

        SqlNode node( int i );

        void replaceSublist( int start, int end, SqlNode e );

        /**
         * Creates a parser whose token sequence is a copy of a subset of this token sequence.
         */
        PrecedenceClimbingParser parser( int start, Predicate<PrecedenceClimbingParser.Token> predicate );
    }


    /**
     * Result of applying {@link ch.unibas.dmi.dbis.polyphenydb.util.PrecedenceClimbingParser.Special#apply}.
     * Tells the caller which range of tokens to replace, and with what.
     */
    public class ReduceResult {

        public final int startOrdinal;
        public final int endOrdinal;
        public final SqlNode node;


        public ReduceResult( int startOrdinal, int endOrdinal, SqlNode node ) {
            this.startOrdinal = startOrdinal;
            this.endOrdinal = endOrdinal;
            this.node = node;
        }
    }
}


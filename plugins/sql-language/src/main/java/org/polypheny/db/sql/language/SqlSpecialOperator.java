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

package org.polypheny.db.sql.language;


import java.util.function.Predicate;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.sql.language.parser.SqlParserUtil.ToTreeListItem;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.PrecedenceClimbingParser;
import org.polypheny.db.util.Util;


/**
 * Generic operator for nodes with special syntax.
 */
public class SqlSpecialOperator extends SqlOperator {


    public SqlSpecialOperator( String name, Kind kind ) {
        this( name, kind, 2 );
    }


    public SqlSpecialOperator( String name, Kind kind, int prec ) {
        this( name, kind, prec, true, null, null, null );
    }


    public SqlSpecialOperator( String name, Kind kind, int prec, boolean leftAssoc, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker ) {
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
    public SqlSyntax getSqlSyntax() {
        return SqlSyntax.SPECIAL;
    }


    /**
     * Reduces a list of operators and arguments according to the rules of precedence and associativity. Returns the ordinal of the node which replaced the expression.
     *
     * The default implementation throws {@link UnsupportedOperationException}.
     *
     * @param ordinal indicating the ordinal of the current operator in the list on which a possible reduction can be made
     * @param list List of alternating {@link ToTreeListItem} and {@link SqlNode}
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

        ParserPos pos( int i );

        boolean isOp( int i );

        SqlNode node( int i );

        void replaceSublist( int start, int end, SqlNode e );

        /**
         * Creates a parser whose token sequence is a copy of a subset of this token sequence.
         */
        PrecedenceClimbingParser parser( int start, Predicate<PrecedenceClimbingParser.Token> predicate );

    }


    /**
     * Result of applying {@link org.polypheny.db.util.PrecedenceClimbingParser.Special#apply}.
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


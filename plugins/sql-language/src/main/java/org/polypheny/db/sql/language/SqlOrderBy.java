/*
 * Copyright 2019-2024 The Polypheny Project
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


import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree node that represents an {@code ORDER BY} on a query other than a {@code SELECT} (e.g. {@code VALUES} or {@code UNION}).
 *
 * It is a purely syntactic operator, and is eliminated by {@code org.polypheny.db.sql.validate.SqlValidatorImpl#performUnconditionalRewrites} and replaced with the ORDER_OPERAND of SqlSelect.
 */
public class SqlOrderBy extends SqlCall {

    public static final SqlSpecialOperator OPERATOR = new Operator() {
        @Override
        public Call createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
            return new SqlOrderBy( pos, (SqlNode) operands[0], (SqlNodeList) operands[1], (SqlNode) operands[2], (SqlNode) operands[3] );
        }
    };

    public final SqlNode query;
    public final SqlNodeList orderList;
    public final SqlNode offset;
    public final SqlNode fetch;


    public SqlOrderBy( ParserPos pos, SqlNode query, SqlNodeList orderList, SqlNode offset, SqlNode fetch ) {
        super( pos );
        this.query = query;
        this.orderList = orderList;
        this.offset = offset;
        this.fetch = fetch;
    }


    @Override
    public Kind getKind() {
        return Kind.ORDER_BY;
    }


    @Override
    public org.polypheny.db.nodes.Operator getOperator() {
        return OPERATOR;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( query, orderList, offset, fetch );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( query, orderList, offset, fetch );
    }


    /**
     * Definition of {@code ORDER BY} operator.
     */
    private static class Operator extends SqlSpecialOperator {

        private Operator() {
            // NOTE:  make precedence lower then SELECT to avoid extra parens
            super( "ORDER BY", Kind.ORDER_BY, 0 );
        }


        @Override
        public SqlSyntax getSqlSyntax() {
            return SqlSyntax.POSTFIX;
        }


        @Override
        public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
            SqlOrderBy orderBy = (SqlOrderBy) call;
            final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.ORDER_BY );
            orderBy.query.unparse( writer, getLeftPrec(), getRightPrec() );
            if ( orderBy.orderList != SqlNodeList.EMPTY ) {
                writer.sep( getName() );
                final SqlWriter.Frame listFrame = writer.startList( SqlWriter.FrameTypeEnum.ORDER_BY_LIST );
                unparseListClause( writer, orderBy.orderList );
                writer.endList( listFrame );
            }
            if ( orderBy.offset != null ) {
                final SqlWriter.Frame frame2 = writer.startList( SqlWriter.FrameTypeEnum.OFFSET );
                writer.newlineAndIndent();
                writer.keyword( "OFFSET" );
                orderBy.offset.unparse( writer, -1, -1 );
                writer.keyword( "ROWS" );
                writer.endList( frame2 );
            }
            if ( orderBy.fetch != null ) {
                final SqlWriter.Frame frame3 = writer.startList( SqlWriter.FrameTypeEnum.FETCH );
                writer.newlineAndIndent();
                writer.keyword( "FETCH" );
                writer.keyword( "NEXT" );
                orderBy.fetch.unparse( writer, -1, -1 );
                writer.keyword( "ROWS" );
                writer.keyword( "ONLY" );
                writer.endList( frame3 );
            }
            writer.endList( frame );
        }

    }

}


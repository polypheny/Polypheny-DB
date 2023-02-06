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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.BasicNodeVisitor.ArgHandler;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * An operator describing a query. (Not a query itself.)
 *
 * Operands are:
 *
 * <ul>
 * <li>0: distinct ({@link SqlLiteral})</li>
 * <li>1: selectClause ({@link SqlNodeList})</li>
 * <li>2: fromClause ({@link SqlCall} to "join" operator)</li>
 * <li>3: whereClause ({@link SqlNode})</li>
 * <li>4: havingClause ({@link SqlNode})</li>
 * <li>5: groupClause ({@link SqlNode})</li>
 * <li>6: windowClause ({@link SqlNodeList})</li>
 * <li>7: orderClause ({@link SqlNode})</li>
 * </ul>
 */
public class SqlSelectOperator extends SqlOperator {

    public static final SqlSelectOperator INSTANCE = new SqlSelectOperator();


    private SqlSelectOperator() {
        super( "SELECT", Kind.SELECT, 2, true, ReturnTypes.SCOPE, null, null );
    }


    @Override
    public SqlSyntax getSqlSyntax() {
        return SqlSyntax.SPECIAL;
    }


    @Override
    public SqlCall createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
        assert functionQualifier == null;
        return new SqlSelect(
                pos,
                (SqlNodeList) operands[0],
                (SqlNodeList) operands[1],
                (SqlNode) operands[2],
                (SqlNode) operands[3],
                (SqlNodeList) operands[4],
                (SqlNode) operands[5],
                (SqlNodeList) operands[6],
                (SqlNodeList) operands[7],
                (SqlNode) operands[8],
                (SqlNode) operands[9] );
    }


    /**
     * Creates a call to the <code>SELECT</code> operator.
     *
     * @param keywordList List of keywords such DISTINCT and ALL, or null
     * @param selectList The SELECT clause, or null if empty
     * @param fromClause The FROM clause
     * @param whereClause The WHERE clause, or null if not present
     * @param groupBy The GROUP BY clause, or null if not present
     * @param having The HAVING clause, or null if not present
     * @param windowDecls The WINDOW clause, or null if not present
     * @param orderBy The ORDER BY clause, or null if not present
     * @param offset Expression for number of rows to discard before returning first row
     * @param fetch Expression for number of rows to fetch
     * @param pos The parser position, or {@link ParserPos#ZERO} if not specified; must not be null.
     * @return A {@link SqlSelect}, never null
     */
    public SqlSelect createCall(
            SqlNodeList keywordList,
            SqlNodeList selectList,
            SqlNode fromClause,
            SqlNode whereClause,
            SqlNodeList groupBy,
            SqlNode having,
            SqlNodeList windowDecls,
            SqlNodeList orderBy,
            SqlNode offset,
            SqlNode fetch,
            ParserPos pos ) {
        return new SqlSelect(
                pos,
                keywordList,
                selectList,
                fromClause,
                whereClause,
                groupBy,
                having,
                windowDecls,
                orderBy,
                offset,
                fetch );
    }


    @Override
    public <R> void acceptCall( NodeVisitor<R> visitor, Call call, boolean onlyExpressions, ArgHandler<R> argHandler ) {
        if ( !onlyExpressions ) {
            // None of the arguments to the SELECT operator are expressions.
            super.acceptCall( visitor, call, onlyExpressions, argHandler );
        }
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        SqlSelect select = (SqlSelect) call;
        final SqlWriter.Frame selectFrame = writer.startList( SqlWriter.FrameTypeEnum.SELECT );
        writer.sep( "SELECT" );
        for ( int i = 0; i < select.keywordList.size(); i++ ) {
            final SqlNode keyword = select.keywordList.getSqlList().get( i );
            keyword.unparse( writer, 0, 0 );
        }
        SqlNode selectClause = select.selectList;
        if ( selectClause == null ) {
            selectClause = SqlIdentifier.star( ParserPos.ZERO );
        }
        final SqlWriter.Frame selectListFrame = writer.startList( SqlWriter.FrameTypeEnum.SELECT_LIST );
        unparseListClause( writer, selectClause );
        writer.endList( selectListFrame );

        if ( select.from != null ) {
            // Polypheny-DB SQL requires FROM but MySQL does not.
            writer.sep( "FROM" );

            // For FROM clause, use precedence just below join operator to make sure that an un-joined nested select will be properly parenthesized
            final SqlWriter.Frame fromFrame = writer.startList( SqlWriter.FrameTypeEnum.FROM_LIST );
            select.from.unparse(
                    writer,
                    SqlJoin.OPERATOR.getLeftPrec() - 1,
                    SqlJoin.OPERATOR.getRightPrec() - 1 );
            writer.endList( fromFrame );
        }

        if ( select.where != null ) {
            writer.sep( "WHERE" );

            if ( !writer.isAlwaysUseParentheses() ) {
                SqlNode node = select.where;

                // decide whether to split on ORs or ANDs
                Kind whereSepKind = Kind.AND;
                if ( (node instanceof SqlCall) && node.getKind() == Kind.OR ) {
                    whereSepKind = Kind.OR;
                }

                // unroll whereClause
                final List<SqlNode> list = new ArrayList<>( 0 );
                while ( node.getKind() == whereSepKind ) {
                    assert node instanceof SqlCall;
                    final SqlCall call1 = (SqlCall) node;
                    list.add( 0, call1.operand( 1 ) );
                    node = call1.operand( 0 );
                }
                list.add( 0, node );

                // unparse in a WhereList frame
                final SqlWriter.Frame whereFrame = writer.startList( SqlWriter.FrameTypeEnum.WHERE_LIST );
                unparseListClause(
                        writer,
                        new SqlNodeList( list, select.where.getPos() ),
                        whereSepKind );
                writer.endList( whereFrame );
            } else {
                select.where.unparse( writer, 0, 0 );
            }
        }
        if ( select.groupBy != null ) {
            writer.sep( "GROUP BY" );
            final SqlWriter.Frame groupFrame = writer.startList( SqlWriter.FrameTypeEnum.GROUP_BY_LIST );
            if ( select.groupBy.getList().isEmpty() ) {
                final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.SIMPLE, "(", ")" );
                writer.endList( frame );
            } else {
                unparseListClause( writer, select.groupBy );
            }
            writer.endList( groupFrame );
        }
        if ( select.having != null ) {
            writer.sep( "HAVING" );
            select.having.unparse( writer, 0, 0 );
        }
        if ( select.windowDecls.size() > 0 ) {
            writer.sep( "WINDOW" );
            final SqlWriter.Frame windowFrame = writer.startList( SqlWriter.FrameTypeEnum.WINDOW_DECL_LIST );
            for ( SqlNode windowDecl : select.windowDecls.getSqlList() ) {
                writer.sep( "," );
                windowDecl.unparse( writer, 0, 0 );
            }
            writer.endList( windowFrame );
        }
        if ( select.orderBy != null && select.orderBy.size() > 0 ) {
            writer.sep( "ORDER BY" );
            final SqlWriter.Frame orderFrame = writer.startList( SqlWriter.FrameTypeEnum.ORDER_BY_LIST );
            unparseListClause( writer, select.orderBy );
            writer.endList( orderFrame );
        }
        writer.fetchOffset( select.fetch, select.offset );
        writer.endList( selectFrame );
    }


    @Override
    public boolean argumentMustBeScalar( int ordinal ) {
        return ordinal == SqlSelect.WHERE_OPERAND;
    }

}


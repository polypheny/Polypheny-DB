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
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.Setter;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeList;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.Select;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * A <code>SqlSelect</code> is a node of a parse tree which represents a select statement. It warrants its own node type just because we have a lot of methods to put somewhere.
 */
public class SqlSelect extends SqlCall implements Select {

    // constants representing operand positions
    public static final int FROM_OPERAND = 2;
    public static final int WHERE_OPERAND = 3;
    public static final int HAVING_OPERAND = 5;

    SqlNodeList keywordList;
    @Setter
    SqlNodeList selectList;
    @Setter
    SqlNode from;
    @Setter
    SqlNode where;
    @Setter
    SqlNodeList groupBy;
    @Setter
    SqlNode having;
    SqlNodeList windowDecls;
    @Setter
    SqlNodeList orderBy;
    @Setter
    SqlNode offset;
    @Setter
    SqlNode fetch;


    public SqlSelect(
            ParserPos pos,
            SqlNodeList keywordList,
            SqlNodeList selectList,
            SqlNode from,
            SqlNode where,
            SqlNodeList groupBy,
            SqlNode having,
            SqlNodeList windowDecls,
            SqlNodeList orderBy,
            SqlNode offset,
            SqlNode fetch ) {
        super( pos );
        this.keywordList = Objects.requireNonNull(
                keywordList != null
                        ? keywordList
                        : new SqlNodeList( pos ) );
        this.selectList = selectList;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.windowDecls = Objects.requireNonNull(
                windowDecls != null
                        ? windowDecls
                        : new SqlNodeList( pos ) );
        this.orderBy = orderBy;
        this.offset = offset;
        this.fetch = fetch;
    }


    @Override
    public Operator getOperator() {
        return SqlSelectOperator.INSTANCE;
    }


    @Override
    public Kind getKind() {
        return Kind.SELECT;
    }


    @Override
    public @Nullable String getEntity() {
        return from.getEntity();
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( keywordList, selectList, from, where, groupBy, having, windowDecls, orderBy, offset, fetch );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( keywordList, selectList, from, where, groupBy, having, windowDecls, orderBy, offset, fetch );
    }


    @Override
    public void setOperand( int i, Node operand ) {
        switch ( i ) {
            case 0:
                keywordList = Objects.requireNonNull( (SqlNodeList) operand );
                break;
            case 1:
                selectList = (SqlNodeList) operand;
                break;
            case 2:
                from = (SqlNode) operand;
                break;
            case 3:
                where = (SqlNode) operand;
                break;
            case 4:
                groupBy = (SqlNodeList) operand;
                break;
            case 5:
                having = (SqlNode) operand;
                break;
            case 6:
                windowDecls = Objects.requireNonNull( (SqlNodeList) operand );
                break;
            case 7:
                orderBy = (SqlNodeList) operand;
                break;
            case 8:
                offset = (SqlNode) operand;
                break;
            case 9:
                fetch = (SqlNode) operand;
                break;
            default:
                throw new AssertionError( i );
        }
    }


    public final boolean isDistinct() {
        return getModifierNode( SqlSelectKeyword.DISTINCT ) != null;
    }


    public final SqlNode getModifierNode( SqlSelectKeyword modifier ) {
        for ( Node keyword : keywordList ) {
            SqlSelectKeyword keyword2 = ((SqlLiteral) keyword).symbolValue( SqlSelectKeyword.class );
            if ( keyword2 == modifier ) {
                return (SqlNode) keyword;
            }
        }
        return null;
    }


    @Override
    public final Node getFrom() {
        return from;
    }


    public final SqlNode getSqlFrom() {
        return from;
    }


    public final SqlNodeList getGroup() {
        return groupBy;
    }


    public final SqlNode getHaving() {
        return having;
    }


    public final SqlNodeList getSqlSelectList() {
        return selectList;
    }


    @Override
    public final NodeList getSelectList() {
        return selectList;
    }


    public final SqlNode getWhere() {
        return where;
    }


    @Nonnull
    public final SqlNodeList getWindowList() {
        return windowDecls;
    }


    public final SqlNodeList getOrderList() {
        return orderBy;
    }


    public final SqlNode getOffset() {
        return offset;
    }


    public final SqlNode getFetch() {
        return fetch;
    }


    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        validator.validateQuery( this, scope, validator.getUnknownType() );
    }


    // Override SqlCall, to introduce a sub-query frame.
    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        if ( !writer.inQuery() ) {
            // If this SELECT is the topmost item in a sub-query, introduce a new frame.
            // (The topmost item in the sub-query might be a UNION or ORDER. In this case, we don't need a wrapper frame.)
            final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.SUB_QUERY, "(", ")" );
            writer.getDialect().unparseCall( writer, this, 0, 0 );
            writer.endList( frame );
        } else {
            writer.getDialect().unparseCall( writer, this, leftPrec, rightPrec );
        }
    }


    public boolean hasOrderBy() {
        return orderBy != null && orderBy.size() != 0;
    }


    public boolean hasWhere() {
        return where != null;
    }


    public boolean isKeywordPresent( SqlSelectKeyword targetKeyWord ) {
        return getModifierNode( targetKeyWord ) != null;
    }

}


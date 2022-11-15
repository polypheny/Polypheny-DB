/*
 * Copyright 2019-2022 The Polypheny Project
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
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.util.ImmutableNullableList;
import org.polypheny.db.util.Pair;


/**
 * A <code>SqlUpdate</code> is a node of a parse tree which represents an UPDATE statement.
 */
public class SqlUpdate extends SqlCall {

    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator( "UPDATE", Kind.UPDATE );

    SqlNode targetTable;
    SqlNodeList targetColumnList;
    SqlNodeList sourceExpressionList;
    SqlNode condition;
    SqlSelect sourceSelect;
    SqlIdentifier alias;


    public SqlUpdate( ParserPos pos, SqlNode targetTable, SqlNodeList targetColumnList, SqlNodeList sourceExpressionList, SqlNode condition, SqlSelect sourceSelect, SqlIdentifier alias ) {
        super( pos );
        this.targetTable = targetTable;
        this.targetColumnList = targetColumnList;
        this.sourceExpressionList = sourceExpressionList;
        this.condition = condition;
        this.sourceSelect = sourceSelect;
        assert sourceExpressionList.size() == targetColumnList.size();
        this.alias = alias;
    }


    @Override
    public Kind getKind() {
        return Kind.UPDATE;
    }


    @Override
    public Operator getOperator() {
        return OPERATOR;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( targetTable, targetColumnList, sourceExpressionList, condition, alias );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( targetTable, targetColumnList, sourceExpressionList, condition, alias );
    }


    @Override
    public void setOperand( int i, Node operand ) {
        switch ( i ) {
            case 0:
                assert operand instanceof SqlIdentifier;
                targetTable = (SqlNode) operand;
                break;
            case 1:
                targetColumnList = (SqlNodeList) operand;
                break;
            case 2:
                sourceExpressionList = (SqlNodeList) operand;
                break;
            case 3:
                condition = (SqlNode) operand;
                break;
            case 4:
                sourceExpressionList = (SqlNodeList) operand;
                break;
            case 5:
                alias = (SqlIdentifier) operand;
                break;
            default:
                throw new AssertionError( i );
        }
    }


    /**
     * @return the identifier for the target table of the update
     */
    public SqlNode getTargetTable() {
        return targetTable;
    }


    /**
     * @return the alias for the target table of the update
     */
    public SqlIdentifier getAlias() {
        return alias;
    }


    public void setAlias( SqlIdentifier alias ) {
        this.alias = alias;
    }


    /**
     * @return the list of target column names
     */
    public SqlNodeList getTargetColumnList() {
        return targetColumnList;
    }


    /**
     * @return the list of source expressions
     */
    public SqlNodeList getSourceExpressionList() {
        return sourceExpressionList;
    }


    /**
     * Gets the filter condition for rows to be updated.
     *
     * @return the condition expression for the data to be updated, or null for all rows in the table
     */
    public SqlNode getCondition() {
        return condition;
    }


    /**
     * Gets the source SELECT expression for the data to be updated. Returns null before the statement has been expanded by {@code SqlValidatorImpl#performUnconditionalRewrites(SqlNode, boolean)}.
     *
     * @return the source SELECT for the data to be updated
     */
    public SqlSelect getSourceSelect() {
        return sourceSelect;
    }


    public void setSourceSelect( SqlSelect sourceSelect ) {
        this.sourceSelect = sourceSelect;
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.SELECT, "UPDATE", "" );
        final int opLeft = ((SqlOperator) getOperator()).getLeftPrec();
        final int opRight = ((SqlOperator) getOperator()).getRightPrec();
        targetTable.unparse( writer, opLeft, opRight );
        if ( alias != null ) {
            writer.keyword( "AS" );
            alias.unparse( writer, opLeft, opRight );
        }
        final SqlWriter.Frame setFrame = writer.startList( SqlWriter.FrameTypeEnum.UPDATE_SET_LIST, "SET", "" );
        for ( Pair<SqlNode, SqlNode> pair : Pair.zip( getTargetColumnList().getSqlList(), getSourceExpressionList().getSqlList() ) ) {
            writer.sep( "," );
            SqlIdentifier id = (SqlIdentifier) pair.left;
            id.unparse( writer, opLeft, opRight );
            writer.keyword( "=" );
            SqlNode sourceExp = pair.right;
            sourceExp.unparse( writer, opLeft, opRight );
        }
        writer.endList( setFrame );
        if ( condition != null ) {
            writer.sep( "WHERE" );
            condition.unparse( writer, opLeft, opRight );
        }
        writer.endList( frame );
    }


    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        validator.validateUpdate( this );
    }

}


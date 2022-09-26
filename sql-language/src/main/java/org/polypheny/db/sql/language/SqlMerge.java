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
 * A <code>SqlMerge</code> is a node of a parse tree which represents a MERGE statement.
 */
public class SqlMerge extends SqlCall {

    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator( "MERGE", Kind.MERGE );

    SqlNode targetTable;
    SqlNode condition;
    SqlNode source;
    SqlUpdate updateCall;
    SqlInsert insertCall;
    SqlSelect sourceSelect;
    SqlIdentifier alias;


    public SqlMerge( ParserPos pos, SqlNode targetTable, SqlNode condition, SqlNode source, SqlUpdate updateCall, SqlInsert insertCall, SqlSelect sourceSelect, SqlIdentifier alias ) {
        super( pos );
        this.targetTable = targetTable;
        this.condition = condition;
        this.source = source;
        this.updateCall = updateCall;
        this.insertCall = insertCall;
        this.sourceSelect = sourceSelect;
        this.alias = alias;
    }


    @Override
    public Operator getOperator() {
        return OPERATOR;
    }


    @Override
    public Kind getKind() {
        return Kind.MERGE;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( targetTable, condition, source, updateCall, insertCall, sourceSelect, alias );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( targetTable, condition, source, updateCall, insertCall, sourceSelect, alias );
    }


    @Override
    public void setOperand( int i, Node operand ) {
        switch ( i ) {
            case 0:
                assert operand instanceof SqlIdentifier;
                targetTable = (SqlNode) operand;
                break;
            case 1:
                condition = (SqlNode) operand;
                break;
            case 2:
                source = (SqlNode) operand;
                break;
            case 3:
                updateCall = (SqlUpdate) operand;
                break;
            case 4:
                insertCall = (SqlInsert) operand;
                break;
            case 5:
                sourceSelect = (SqlSelect) operand;
                break;
            case 6:
                alias = (SqlIdentifier) operand;
                break;
            default:
                throw new AssertionError( i );
        }
    }


    /**
     * @return the identifier for the target table of the merge
     */
    public SqlNode getTargetTable() {
        return targetTable;
    }


    /**
     * @return the alias for the target table of the merge
     */
    public SqlIdentifier getAlias() {
        return alias;
    }


    /**
     * @return the source for the merge
     */
    public SqlNode getSourceTableRef() {
        return source;
    }


    public void setSourceTableRef( SqlNode tableRef ) {
        this.source = tableRef;
    }


    /**
     * @return the update statement for the merge
     */
    public SqlUpdate getUpdateCall() {
        return updateCall;
    }


    /**
     * @return the insert statement for the merge
     */
    public SqlInsert getInsertCall() {
        return insertCall;
    }


    /**
     * @return the condition expression to determine whether to update or insert
     */
    public SqlNode getCondition() {
        return condition;
    }


    /**
     * Gets the source SELECT expression for the data to be updated/inserted.
     * Returns null before the statement has been expanded by {@code SqlValidatorImpl#performUnconditionalRewrites(SqlNode, boolean)}.
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
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.SELECT, "MERGE INTO", "" );
        final int opLeft = ((SqlOperator) getOperator()).getLeftPrec();
        final int opRight = ((SqlOperator) getOperator()).getRightPrec();
        targetTable.unparse( writer, opLeft, opRight );
        if ( alias != null ) {
            writer.keyword( "AS" );
            alias.unparse( writer, opLeft, opRight );
        }

        writer.newlineAndIndent();
        writer.keyword( "USING" );
        source.unparse( writer, opLeft, opRight );

        writer.newlineAndIndent();
        writer.keyword( "ON" );
        condition.unparse( writer, opLeft, opRight );

        if ( updateCall != null ) {
            writer.newlineAndIndent();
            writer.keyword( "WHEN MATCHED THEN UPDATE" );
            final SqlWriter.Frame setFrame =
                    writer.startList(
                            SqlWriter.FrameTypeEnum.UPDATE_SET_LIST,
                            "SET",
                            "" );

            for ( Pair<SqlNode, SqlNode> pair : Pair.zip( updateCall.targetColumnList.getSqlList(), updateCall.sourceExpressionList.getSqlList() ) ) {
                writer.sep( "," );
                SqlIdentifier id = (SqlIdentifier) pair.left;
                id.unparse( writer, opLeft, opRight );
                writer.keyword( "=" );
                SqlNode sourceExp = pair.right;
                sourceExp.unparse( writer, opLeft, opRight );
            }
            writer.endList( setFrame );
        }

        if ( insertCall != null ) {
            writer.newlineAndIndent();
            writer.keyword( "WHEN NOT MATCHED THEN INSERT" );
            if ( insertCall.getTargetColumnList() != null ) {
                insertCall.getTargetColumnList().unparse( writer, opLeft, opRight );
            }
            insertCall.getSource().unparse( writer, opLeft, opRight );

            writer.endList( frame );
        }
    }


    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        validator.validateMerge( this );
    }

}


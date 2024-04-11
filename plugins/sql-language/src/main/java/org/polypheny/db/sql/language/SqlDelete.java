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
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * A <code>SqlDelete</code> is a node of a parse tree which represents a DELETE statement.
 */
public class SqlDelete extends SqlCall {

    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator( "DELETE", Kind.DELETE );

    SqlNode targetTable;
    SqlNode condition;
    SqlSelect sourceSelect;
    SqlIdentifier alias;


    public SqlDelete( ParserPos pos, SqlNode targetTable, SqlNode condition, SqlSelect sourceSelect, SqlIdentifier alias ) {
        super( pos );
        this.targetTable = targetTable;
        this.condition = condition;
        this.sourceSelect = sourceSelect;
        this.alias = alias;
    }


    @Override
    public Kind getKind() {
        return Kind.DELETE;
    }


    @Override
    public @Nullable String getEntity() {
        return targetTable.getEntity();
    }


    @Override
    public Operator getOperator() {
        return OPERATOR;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( targetTable, condition, alias );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( targetTable, condition, alias );
    }


    @Override
    public void setOperand( int i, Node operand ) {
        switch ( i ) {
            case 0:
                targetTable = (SqlNode) operand;
                break;
            case 1:
                condition = (SqlNode) operand;
                break;
            case 2:
                sourceSelect = (SqlSelect) operand;
                break;
            case 3:
                alias = (SqlIdentifier) operand;
                break;
            default:
                throw new AssertionError( i );
        }
    }


    /**
     * @return the identifier for the target table of the deletion
     */
    public SqlNode getTargetTable() {
        return targetTable;
    }


    /**
     * @return the alias for the target table of the deletion
     */
    public SqlIdentifier getAlias() {
        return alias;
    }


    /**
     * Gets the filter condition for rows to be deleted.
     *
     * @return the condition expression for the data to be deleted, or null for all rows in the table
     */
    public SqlNode getCondition() {
        return condition;
    }


    /**
     * Gets the source SELECT expression for the data to be deleted. This returns null before the condition has been expanded by
     * {@code SqlValidatorImpl#performUnconditionalRewrites(SqlNode, boolean)}.
     *
     * @return the source SELECT for the data to be inserted
     */
    public SqlSelect getSourceSelect() {
        return sourceSelect;
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.SELECT, "DELETE FROM", "" );
        final int opLeft = ((SqlOperator) getOperator()).getLeftPrec();
        final int opRight = ((SqlOperator) getOperator()).getRightPrec();
        targetTable.unparse( writer, opLeft, opRight );
        if ( alias != null ) {
            writer.keyword( "AS" );
            alias.unparse( writer, opLeft, opRight );
        }
        if ( condition != null ) {
            writer.sep( "WHERE" );
            condition.unparse( writer, opLeft, opRight );
        }
        writer.endList( frame );
    }


    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        validator.validateDelete( this );
    }


    public void setSourceSelect( SqlSelect sourceSelect ) {
        this.sourceSelect = sourceSelect;
    }

}


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
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * A <code>SqlInsert</code> is a node of a parse tree which represents an INSERT statement.
 */
public class SqlInsert extends SqlCall {

    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator( "INSERT", Kind.INSERT );

    public enum OverrideMode {
        NONE,
        SYSTEM,
        USER
    }


    SqlNodeList keywords;
    @Getter
    SqlNode targetTable;
    @Getter
    SqlNode source;
    @Setter
    SqlNodeList columns;
    @Getter
    private final OverrideMode overrideMode;
    @Getter
    @Nullable
    private final SqlNodeList returningColumns;


    public SqlInsert( ParserPos pos, SqlNodeList keywords, SqlNode targetTable, SqlNode source, SqlNodeList columns ) {
        this( pos, keywords, targetTable, source, columns, OverrideMode.NONE, null );
    }


    public SqlInsert( ParserPos pos, SqlNodeList keywords, SqlNode targetTable, SqlNode source, SqlNodeList columns, OverrideMode overrideMode ) {
        this( pos, keywords, targetTable, source, columns, overrideMode, null );
    }


    public SqlInsert(
            ParserPos pos, SqlNodeList keywords, SqlNode targetTable, SqlNode source, SqlNodeList columns,
            OverrideMode overrideMode, @Nullable SqlNodeList returningColumns ) {
        super( pos );
        this.overrideMode = overrideMode;
        this.returningColumns = returningColumns;
        this.keywords = keywords;
        this.targetTable = targetTable;
        this.source = source;
        this.columns = columns;
        assert keywords != null;
    }


    @Override
    public SqlInsert clone( ParserPos pos ) {
        return new SqlInsert( pos, keywords, targetTable, source, columns, overrideMode, returningColumns );
    }


    public boolean isDefaultValues() {
        return SqlNodeList.isEmptyList( columns );
    }


    @Override
    public Kind getKind() {
        return Kind.INSERT;
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
        return ImmutableNullableList.of( keywords, targetTable, source, columns );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( keywords, targetTable, source, columns );
    }


    /**
     * Returns whether this is an UPSERT statement.
     *
     * In SQL, this is represented using the {@code UPSERT} keyword rather than {@code INSERT};
     * in the abstract syntax tree, an UPSERT is indicated by the presence of a {@link SqlInsertKeyword#UPSERT} keyword.
     */
    public final boolean isUpsert() {
        return getModifierNode( SqlInsertKeyword.UPSERT ) != null;
    }


    @Override
    public void setOperand( int i, Node operand ) {
        switch ( i ) {
            case 0:
                keywords = (SqlNodeList) operand;
                break;
            case 1:
                assert operand instanceof SqlIdentifier;
                targetTable = (SqlNode) operand;
                break;
            case 2:
                source = (SqlNode) operand;
                break;
            case 3:
                columns = (SqlNodeList) operand;
                break;
            default:
                throw new AssertionError( i );
        }
    }


    public void setSource( SqlSelect source ) {
        this.source = source;
    }


    /**
     * @return the list of target column names, or null for all columns in the target table
     */
    public SqlNodeList getTargetColumnList() {
        return columns;
    }


    public final SqlNode getModifierNode( SqlInsertKeyword modifier ) {
        for ( SqlNode keyword : keywords.getSqlList() ) {
            SqlInsertKeyword keyword2 = ((SqlLiteral) keyword).symbolValue( SqlInsertKeyword.class );
            if ( keyword2 == modifier ) {
                return keyword;
            }
        }
        return null;
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.startList( SqlWriter.FrameTypeEnum.SELECT );
        writer.sep( isUpsert()
                ? "UPSERT INTO"
                : "INSERT INTO" );
        final int opLeft = ((SqlOperator) getOperator()).getLeftPrec();
        final int opRight = ((SqlOperator) getOperator()).getRightPrec();
        targetTable.unparse( writer, opLeft, opRight );
        if ( columns != null && !isDefaultValues() ) {
            columns.unparse( writer, opLeft, opRight );
        }
        if ( overrideMode != OverrideMode.NONE ) {
            writer.keyword( overrideMode == OverrideMode.SYSTEM ? "OVERRIDING SYSTEM VALUE" : "OVERRIDING USER VALUE" );
        }
        writer.newlineAndIndent();
        if ( isDefaultValues() ) {
            writer.keyword( "DEFAULT VALUES" );
        } else {
            source.unparse( writer, 0, 0 );
        }
        if ( returningColumns != null ) {
            writer.keyword( "RETURNING" );
            returningColumns.commaList( writer );
        }
    }


    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        validator.validateInsert( this );
    }


    public DataModel getSchemaType() {
        return DataModel.RELATIONAL;
    }

}


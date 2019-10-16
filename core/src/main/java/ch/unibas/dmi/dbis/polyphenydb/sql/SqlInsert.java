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
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableNullableList;
import java.util.List;
import lombok.Setter;


/**
 * A <code>SqlInsert</code> is a node of a parse tree which represents an INSERT statement.
 */
public class SqlInsert extends SqlCall {

    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator( "INSERT", SqlKind.INSERT );

    SqlNodeList keywords;
    SqlNode targetTable;
    SqlNode source;
    @Setter
    SqlNodeList columnList;


    public SqlInsert( SqlParserPos pos, SqlNodeList keywords, SqlNode targetTable, SqlNode source, SqlNodeList columnList ) {
        super( pos );
        this.keywords = keywords;
        this.targetTable = targetTable;
        this.source = source;
        this.columnList = columnList;
        assert keywords != null;
    }


    @Override
    public SqlKind getKind() {
        return SqlKind.INSERT;
    }


    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( keywords, targetTable, source, columnList );
    }


    /**
     * Returns whether this is an UPSERT statement.
     *
     * In SQL, this is represented using the {@code UPSERT} keyword rather than {@code INSERT}; in the abstract syntax tree, an UPSERT is indicated by the presence of a {@link SqlInsertKeyword#UPSERT} keyword.
     */
    public final boolean isUpsert() {
        return getModifierNode( SqlInsertKeyword.UPSERT ) != null;
    }


    @Override
    public void setOperand( int i, SqlNode operand ) {
        switch ( i ) {
            case 0:
                keywords = (SqlNodeList) operand;
                break;
            case 1:
                assert operand instanceof SqlIdentifier;
                targetTable = operand;
                break;
            case 2:
                source = operand;
                break;
            case 3:
                columnList = (SqlNodeList) operand;
                break;
            default:
                throw new AssertionError( i );
        }
    }


    /**
     * @return the identifier for the target table of the insertion
     */
    public SqlNode getTargetTable() {
        return targetTable;
    }


    /**
     * @return the source expression for the data to be inserted
     */
    public SqlNode getSource() {
        return source;
    }


    public void setSource( SqlSelect source ) {
        this.source = source;
    }


    /**
     * @return the list of target column names, or null for all columns in the target table
     */
    public SqlNodeList getTargetColumnList() {
        return columnList;
    }


    public final SqlNode getModifierNode( SqlInsertKeyword modifier ) {
        for ( SqlNode keyword : keywords ) {
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
        final int opLeft = getOperator().getLeftPrec();
        final int opRight = getOperator().getRightPrec();
        targetTable.unparse( writer, opLeft, opRight );
        if ( columnList != null ) {
            columnList.unparse( writer, opLeft, opRight );
        }
        writer.newlineAndIndent();
        source.unparse( writer, 0, 0 );
    }


    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        validator.validateInsert( this );
    }
}


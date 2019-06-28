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

package ch.unibas.dmi.dbis.polyphenydb.sql.ddl;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCollation;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDataTypeSpec;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSpecialOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * Parse tree for SqlAttributeDefinition, which is part of a {@link SqlCreateType}.
 */
public class SqlAttributeDefinition extends SqlCall {

    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator( "ATTRIBUTE_DEF", SqlKind.ATTRIBUTE_DEF );

    final SqlIdentifier name;
    final SqlDataTypeSpec dataType;
    final SqlNode expression;
    final SqlCollation collation;


    /**
     * Creates a SqlAttributeDefinition; use {@link SqlDdlNodes#attribute}.
     */
    SqlAttributeDefinition( SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, SqlNode expression, SqlCollation collation ) {
        super( pos );
        this.name = name;
        this.dataType = dataType;
        this.expression = expression;
        this.collation = collation;
    }


    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of( name, dataType );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        name.unparse( writer, 0, 0 );
        dataType.unparse( writer, 0, 0 );
        if ( collation != null ) {
            writer.keyword( "COLLATE" );
            collation.unparse( writer, 0, 0 );
        }
        if ( dataType.getNullable() != null && !dataType.getNullable() ) {
            writer.keyword( "NOT NULL" );
        }
        if ( expression != null ) {
            writer.keyword( "DEFAULT" );
            exp( writer );
        }
    }


    // TODO: refactor this to a util class to share with SqlColumnDeclaration
    private void exp( SqlWriter writer ) {
        if ( writer.isAlwaysUseParentheses() ) {
            expression.unparse( writer, 0, 0 );
        } else {
            writer.sep( "(" );
            expression.unparse( writer, 0, 0 );
            writer.sep( ")" );
        }
    }
}

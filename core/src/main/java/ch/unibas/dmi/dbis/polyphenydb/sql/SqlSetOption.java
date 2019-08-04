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


import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableNullableList;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;


/**
 * SQL parse tree node to represent {@code SET} and {@code RESET} statements, optionally preceded by {@code ALTER SYSTEM} or {@code ALTER SESSION}.
 *
 * Syntax:
 *
 * <blockquote><code>
 * ALTER scope SET `option.name` = value;<br>
 * ALTER scope RESET `option`.`name`;<br>
 * ALTER scope RESET ALL;<br>
 * <br>
 * SET `option.name` = value;<br>
 * RESET `option`.`name`;<br>
 * RESET ALL;
 * </code></blockquote>
 *
 * If {@link #value} is null, assume RESET; if {@link #value} is not null, assume SET.
 *
 * Examples:
 *
 * <ul>
 * <li><code>ALTER SYSTEM SET `my`.`param1` = 1</code></li>
 * <li><code>SET `my.param2` = 1</code></li>
 * <li><code>SET `my.param3` = ON</code></li>
 * <li><code>ALTER SYSTEM RESET `my`.`param1`</code></li>
 * <li><code>RESET `my.param2`</code></li>
 * <li><code>ALTER SESSION RESET ALL</code></li>
 * </ul>
 */
public class SqlSetOption extends SqlAlter {

    public static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator( "SET_OPTION", SqlKind.SET_OPTION ) {
                @Override
                public SqlCall createCall( SqlLiteral functionQualifier, SqlParserPos pos, SqlNode... operands ) {
                    final SqlNode scopeNode = operands[0];
                    return new SqlSetOption(
                            pos,
                            scopeNode == null
                                    ? null
                                    : scopeNode.toString(),
                            (SqlIdentifier) operands[1],
                            operands[2] );
                }
            };

    /**
     * Name of the option as an {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier} with one or more parts.
     */
    SqlIdentifier name;

    /**
     * Value of the option. May be a {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral} or a {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier} with one part.
     * Reserved words (currently just 'ON') are converted to identifiers by the parser.
     */
    SqlNode value;

    @Getter
    String scope;


    /**
     * Creates a node.
     *
     * @param pos Parser position, must not be null.
     * @param scope Scope (generally "SYSTEM" or "SESSION"), may be null.
     * @param name Name of option, as an identifier, must not be null.
     * @param value Value of option, as an identifier or literal, may be null. If null, assume RESET command, else assume SET command.
     */
    public SqlSetOption( SqlParserPos pos, String scope, SqlIdentifier name, SqlNode value ) {
        super( OPERATOR, pos );
        this.name = name;
        this.value = value;
        this.scope = scope;
        assert name != null;
    }


    @Override
    public SqlKind getKind() {
        return SqlKind.SET_OPTION;
    }


    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }


    @Override
    public List<SqlNode> getOperandList() {
        final List<SqlNode> operandList = new ArrayList<>();
        if ( scope == null ) {
            operandList.add( null );
        } else {
            operandList.add( new SqlIdentifier( scope, SqlParserPos.ZERO ) );
        }
        operandList.add( name );
        operandList.add( value );
        return ImmutableNullableList.copyOf( operandList );
    }


    @Override
    public void setOperand( int i, SqlNode operand ) {
        switch ( i ) {
            case 0:
                if ( operand != null ) {
                    scope = ((SqlIdentifier) operand).getSimple();
                } else {
                    scope = null;
                }
                break;
            case 1:
                name = (SqlIdentifier) operand;
                break;
            case 2:
                value = operand;
                break;
            default:
                throw new AssertionError( i );
        }
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.literal( scope );
        if ( value != null ) {
            writer.keyword( "SET" );
        } else {
            writer.keyword( "RESET" );
        }
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.SIMPLE );
        name.unparse( writer, leftPrec, rightPrec );
        if ( value != null ) {
            writer.sep( "=" );
            value.unparse( writer, leftPrec, rightPrec );
        }
        writer.endList( frame );
    }


    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        validator.validate( value );
    }


    public SqlIdentifier getName() {
        return name;
    }


    public void setName( SqlIdentifier name ) {
        this.name = name;
    }


    public SqlNode getValue() {
        return value;
    }


    public void setValue( SqlNode value ) {
        this.value = value;
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        // TODO: Implement
    }
}


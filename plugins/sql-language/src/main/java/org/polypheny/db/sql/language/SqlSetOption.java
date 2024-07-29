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


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


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
            new SqlSpecialOperator( "SET_OPTION", Kind.SET_OPTION ) {
                @Override
                public Call createCall( Literal functionQualifier, ParserPos pos, Node... operands ) {
                    final SqlNode scopeNode = (SqlNode) operands[0];
                    return new SqlSetOption(
                            pos,
                            scopeNode == null
                                    ? null
                                    : scopeNode.toString(),
                            (SqlIdentifier) operands[1],
                            (SqlNode) operands[2] );
                }
            };

    /**
     * Name of the option as an {@link SqlIdentifier} with one or more parts.
     */
    SqlIdentifier name;

    /**
     * Value of the option. May be a {@link SqlLiteral} or a {@link SqlIdentifier} with one part.
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
    public SqlSetOption( ParserPos pos, String scope, SqlIdentifier name, SqlNode value ) {
        super( OPERATOR, pos );
        this.name = name;
        this.value = value;
        this.scope = scope;
        assert name != null;
    }


    @Override
    public Kind getKind() {
        return Kind.SET_OPTION;
    }


    @Override
    public Operator getOperator() {
        return OPERATOR;
    }


    @Override
    public List<Node> getOperandList() {
        final List<SqlNode> operandList = new ArrayList<>();
        if ( scope == null ) {
            operandList.add( null );
        } else {
            operandList.add( new SqlIdentifier( scope, ParserPos.ZERO ) );
        }
        operandList.add( name );
        operandList.add( value );
        return ImmutableNullableList.copyOf( operandList );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return getOperandList().stream().map( e -> (SqlNode) e ).collect( Collectors.toList() );
    }


    @Override
    public void setOperand( int i, Node operand ) {
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
                value = (SqlNode) operand;
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
        validator.validateSql( value );
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
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        DdlManager.getInstance().setOption();
    }

}


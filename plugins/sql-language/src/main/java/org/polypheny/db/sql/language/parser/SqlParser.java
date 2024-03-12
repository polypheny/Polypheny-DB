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

package org.polypheny.db.sql.language.parser;


import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.sql.language.SqlBinaryOperator;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlSelect;


/**
 * A <code>SqlParser</code> parses a SQL statement.
 */
public class SqlParser implements Parser {

    private final SqlAbstractParserImpl parser;


    public SqlParser( SqlAbstractParserImpl parser, ParserConfig sqlParserConfig ) {
        this.parser = parser;
        parser.setTabSize( 1 );
        parser.setQuotedCasing( sqlParserConfig.quotedCasing() );
        parser.setUnquotedCasing( sqlParserConfig.unquotedCasing() );
        parser.setIdentifierMaxLength( sqlParserConfig.identifierMaxLength() );
        parser.setConformance( sqlParserConfig.conformance() );
        switch ( sqlParserConfig.quoting() ) {
            case DOUBLE_QUOTE:
                parser.switchTo( "DQID" );
                break;
            case BACK_TICK:
                parser.switchTo( "BTID" );
                break;
            case BRACKET:
                parser.switchTo( "DEFAULT" );
                break;
        }
    }


    /**
     * Parses a SQL expression.
     *
     * @throws NodeParseException if there is a parse error
     */
    public SqlNode parseExpression() throws NodeParseException {
        try {
            return parser.parseSqlExpressionEof();
        } catch ( Throwable ex ) {
            if ( ex instanceof PolyphenyDbContextException ) {
                final String originalSql = parser.getOriginalSql();
                if ( originalSql != null ) {
                    ((PolyphenyDbContextException) ex).setOriginalStatement( originalSql );
                }
            }
            throw parser.normalizeException( ex );
        }
    }


    /**
     * Parses a <code>SELECT</code> statement.
     *
     * @return A {@link SqlSelect} for a regular <code>SELECT</code> statement; a {@link SqlBinaryOperator} for a <code>UNION</code>, <code>INTERSECT</code>, or <code>EXCEPT</code>.
     * @throws NodeParseException if there is a parse error
     */
    @Override
    public Node parseQuery() throws NodeParseException {
        try {
            return parser.parseSqlStmtEof();
        } catch ( Throwable ex ) {
            if ( ex instanceof PolyphenyDbContextException ) {
                final String originalSql = parser.getOriginalSql();
                if ( originalSql != null ) {
                    ((PolyphenyDbContextException) ex).setOriginalStatement( originalSql );
                }
            }
            throw parser.normalizeException( ex );
        }
    }


    /**
     * Parses an SQL statement.
     *
     * @return top-level SqlNode representing stmt
     * @throws NodeParseException if there is a parse error
     */
    @Override
    public Node parseStmt() throws NodeParseException {
        return parseQuery();
    }


    /**
     * Get the parser metadata.
     *
     * @return {@link SqlAbstractParserImpl.Metadata} implementation of underlying parser.
     */
    public SqlAbstractParserImpl.Metadata getMetadata() {
        return parser.getMetadata();
    }


}


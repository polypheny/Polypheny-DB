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

package ch.unibas.dmi.dbis.polyphenydb.sql.parser;


import ch.unibas.dmi.dbis.polyphenydb.runtime.PolyphenyDbContextException;
import ch.unibas.dmi.dbis.polyphenydb.sql.Lex;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelect;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.impl.SqlParserImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformance;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformanceEnum;
import ch.unibas.dmi.dbis.polyphenydb.util.SourceStringReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Objects;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;


/**
 * A <code>SqlParser</code> parses a SQL statement.
 */
public class SqlParser {

    public static final int DEFAULT_IDENTIFIER_MAX_LENGTH = 128;

    private final SqlAbstractParserImpl parser;


    private SqlParser( SqlAbstractParserImpl parser, SqlParserConfig sqlParserConfig ) {
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
     * Creates a <code>SqlParser</code> to parse the given string using Polypheny-DB's parser implementation.
     *
     * The default lexical policy is similar to Oracle.
     *
     * @param s An SQL statement or expression to parse.
     * @return A parser
     * @see Lex#ORACLE
     */
    public static SqlParser create( String s ) {
        return create( s, configBuilder().build() );
    }


    /**
     * Creates a <code>SqlParser</code> to parse the given string using the parser implementation created from given {@link SqlParserImplFactory} with given quoting syntax and casing policies for identifiers.
     *
     * @param sql A SQL statement or expression to parse
     * @param sqlParserConfig The parser configuration (identifier max length, etc.)
     * @return A parser
     */
    public static SqlParser create( String sql, SqlParserConfig sqlParserConfig ) {
        return create( new SourceStringReader( sql ), sqlParserConfig );
    }


    /**
     * Creates a <code>SqlParser</code> to parse the given string using the parser implementation created from given {@link SqlParserImplFactory} with given quoting syntax and casing policies for identifiers.
     *
     * Unlike {@link #create(java.lang.String, SqlParserConfig)}, the parser is not able to return the original query string, but will instead return "?".
     *
     * @param reader The source for the SQL statement or expression to parse
     * @param sqlParserConfig The parser configuration (identifier max length, etc.)
     * @return A parser
     */
    public static SqlParser create( Reader reader, SqlParserConfig sqlParserConfig ) {
        SqlAbstractParserImpl parser = sqlParserConfig.parserFactory().getParser( reader );

        return new SqlParser( parser, sqlParserConfig );
    }


    /**
     * Parses a SQL expression.
     *
     * @throws SqlParseException if there is a parse error
     */
    public SqlNode parseExpression() throws SqlParseException {
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
     * @return A {@link SqlSelect} for a regular <code>SELECT</code> statement; a {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlBinaryOperator} for a <code>UNION</code>, <code>INTERSECT</code>, or <code>EXCEPT</code>.
     * @throws SqlParseException if there is a parse error
     */
    public SqlNode parseQuery() throws SqlParseException {
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
     * Parses a <code>SELECT</code> statement and reuses parser.
     *
     * @param sql sql to parse
     * @return A {@link SqlSelect} for a regular <code>SELECT</code> statement; a {@link ch.unibas.dmi.dbis.polyphenydb.sql.SqlBinaryOperator} for a <code>UNION</code>, <code>INTERSECT</code>, or <code>EXCEPT</code>.
     * @throws SqlParseException if there is a parse error
     */
    public SqlNode parseQuery( String sql ) throws SqlParseException {
        parser.ReInit( new StringReader( sql ) );
        return parseQuery();
    }


    /**
     * Parses an SQL statement.
     *
     * @return top-level SqlNode representing stmt
     * @throws SqlParseException if there is a parse error
     */
    public SqlNode parseStmt() throws SqlParseException {
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


    /**
     * Builder for a {@link SqlParserConfig}.
     */
    public static ConfigBuilder configBuilder() {
        return new ConfigBuilder();
    }


    /**
     * Builder for a {@link SqlParserConfig} that starts with an existing {@code Config}.
     */
    public static ConfigBuilder configBuilder( SqlParserConfig sqlParserConfig ) {
        return new ConfigBuilder().setConfig( sqlParserConfig );
    }


    /**
     * Interface to define the configuration for a SQL parser.
     *
     * @see ConfigBuilder
     */
    public interface SqlParserConfig {

        /**
         * Default configuration.
         */
        SqlParserConfig DEFAULT = configBuilder().build();

        int identifierMaxLength();

        Casing quotedCasing();

        Casing unquotedCasing();

        Quoting quoting();

        boolean caseSensitive();

        SqlConformance conformance();

        SqlParserImplFactory parserFactory();
    }


    /**
     * Builder for a {@link SqlParserConfig}.
     */
    public static class ConfigBuilder {

        private Casing quotedCasing = Lex.ORACLE.quotedCasing;
        private Casing unquotedCasing = Lex.ORACLE.unquotedCasing;
        private Quoting quoting = Lex.ORACLE.quoting;
        private int identifierMaxLength = DEFAULT_IDENTIFIER_MAX_LENGTH;
        private boolean caseSensitive = Lex.ORACLE.caseSensitive;
        private SqlConformance conformance = SqlConformanceEnum.DEFAULT;
        private SqlParserImplFactory parserFactory = SqlParserImpl.FACTORY;


        private ConfigBuilder() {
        }


        /**
         * Sets configuration identical to a given {@link SqlParserConfig}.
         */
        public ConfigBuilder setConfig( SqlParserConfig sqlParserConfig ) {
            this.quotedCasing = sqlParserConfig.quotedCasing();
            this.unquotedCasing = sqlParserConfig.unquotedCasing();
            this.quoting = sqlParserConfig.quoting();
            this.identifierMaxLength = sqlParserConfig.identifierMaxLength();
            this.conformance = sqlParserConfig.conformance();
            this.parserFactory = sqlParserConfig.parserFactory();
            return this;
        }


        public ConfigBuilder setQuotedCasing( Casing quotedCasing ) {
            this.quotedCasing = Objects.requireNonNull( quotedCasing );
            return this;
        }


        public ConfigBuilder setUnquotedCasing( Casing unquotedCasing ) {
            this.unquotedCasing = Objects.requireNonNull( unquotedCasing );
            return this;
        }


        public ConfigBuilder setQuoting( Quoting quoting ) {
            this.quoting = Objects.requireNonNull( quoting );
            return this;
        }


        public ConfigBuilder setCaseSensitive( boolean caseSensitive ) {
            this.caseSensitive = caseSensitive;
            return this;
        }


        public ConfigBuilder setIdentifierMaxLength( int identifierMaxLength ) {
            this.identifierMaxLength = identifierMaxLength;
            return this;
        }


        public ConfigBuilder setConformance( SqlConformance conformance ) {
            this.conformance = conformance;
            return this;
        }


        public ConfigBuilder setParserFactory( SqlParserImplFactory factory ) {
            this.parserFactory = Objects.requireNonNull( factory );
            return this;
        }


        public ConfigBuilder setLex( Lex lex ) {
            setCaseSensitive( lex.caseSensitive );
            setUnquotedCasing( lex.unquotedCasing );
            setQuotedCasing( lex.quotedCasing );
            setQuoting( lex.quoting );
            return this;
        }


        /**
         * Builds a {@link SqlParserConfig}.
         */
        public SqlParserConfig build() {
            return new ConfigImpl( identifierMaxLength, quotedCasing, unquotedCasing, quoting, caseSensitive, conformance, parserFactory );
        }

    }


    /**
     * Implementation of {@link SqlParserConfig}.
     * Called by builder; all values are in private final fields.
     */
    private static class ConfigImpl implements SqlParserConfig {

        private final int identifierMaxLength;
        private final boolean caseSensitive;
        private final SqlConformance conformance;
        private final Casing quotedCasing;
        private final Casing unquotedCasing;
        private final Quoting quoting;
        private final SqlParserImplFactory parserFactory;


        private ConfigImpl( int identifierMaxLength, Casing quotedCasing, Casing unquotedCasing, Quoting quoting, boolean caseSensitive, SqlConformance conformance, SqlParserImplFactory parserFactory ) {
            this.identifierMaxLength = identifierMaxLength;
            this.caseSensitive = caseSensitive;
            this.conformance = Objects.requireNonNull( conformance );
            this.quotedCasing = Objects.requireNonNull( quotedCasing );
            this.unquotedCasing = Objects.requireNonNull( unquotedCasing );
            this.quoting = Objects.requireNonNull( quoting );
            this.parserFactory = Objects.requireNonNull( parserFactory );
        }


        @Override
        public int identifierMaxLength() {
            return identifierMaxLength;
        }


        @Override
        public Casing quotedCasing() {
            return quotedCasing;
        }


        @Override
        public Casing unquotedCasing() {
            return unquotedCasing;
        }


        @Override
        public Quoting quoting() {
            return quoting;
        }


        @Override
        public boolean caseSensitive() {
            return caseSensitive;
        }


        @Override
        public SqlConformance conformance() {
            return conformance;
        }


        @Override
        public SqlParserImplFactory parserFactory() {
            return parserFactory;
        }
    }
}


/*
 * Copyright 2019-2020 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.mql.parser;


import java.io.Reader;
import java.io.StringReader;
import java.util.Objects;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.mql.parser.impl.MqlParserImpl;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.sql.SqlSelect;
import org.polypheny.db.sql.parser.SqlParseException;
import org.polypheny.db.util.SourceStringReader;


/**
 * A <code>MqlParser</code> parses a MQL statement.
 */
public class MqlParser {

    public static final int DEFAULT_IDENTIFIER_MAX_LENGTH = 128;

    private final MqlAbstractParserImpl parser;


    private MqlParser( MqlAbstractParserImpl parser ) {
        this.parser = parser;
    }


    public static MqlParser create( String sql, MqlParserConfig mqlParserConfig ) {
        return create( new SourceStringReader( sql ), mqlParserConfig );
    }


    public static MqlParser create( Reader reader, MqlParserConfig mqlParserConfig ) {
        MqlAbstractParserImpl parser = mqlParserConfig.parserFactory().getParser( reader );

        return new MqlParser( parser );
    }


    /**
     * Parses a SQL expression.
     *
     * @throws SqlParseException if there is a parse error
     */
    public MqlNode parseExpression() throws MqlParseException {
        try {
            return parser.parseMqlExpressionEof();
        } catch ( Throwable ex ) {
            if ( ex instanceof PolyphenyDbContextException ) {
                final String originalMql = parser.getOriginalMql();
                if ( originalMql != null ) {
                    ((PolyphenyDbContextException) ex).setOriginalStatement( originalMql );
                }
            }
            throw parser.normalizeException( ex );
        }
    }


    /**
     * Parses a <code>SELECT</code> statement.
     *
     * @return A {@link SqlSelect} for a regular <code>SELECT</code> statement; a {@link org.polypheny.db.sql.SqlBinaryOperator} for a <code>UNION</code>, <code>INTERSECT</code>, or <code>EXCEPT</code>.
     * @throws SqlParseException if there is a parse error
     */
    public MqlNode parseQuery() throws MqlParseException {
        try {
            return parser.parseMqlStmtEof();
        } catch ( Throwable ex ) {
            if ( ex instanceof PolyphenyDbContextException ) {
                final String originalMql = parser.getOriginalMql();
                if ( originalMql != null ) {
                    ((PolyphenyDbContextException) ex).setOriginalStatement( originalMql );
                }
            }
            throw parser.normalizeException( ex );
        }
    }


    /**
     * Parses a <code>SELECT</code> statement and reuses parser.
     */
    public MqlNode parseQuery( String mql ) throws MqlParseException {
        parser.ReInit( new StringReader( mql ) );
        return parseQuery();
    }


    /**
     * Parses an SQL statement.
     *
     * @return top-level SqlNode representing stmt
     */
    public MqlNode parseStmt() throws MqlParseException {
        return parseQuery();
    }


    /**
     * Builder for a {@link MqlParserConfig}.
     */
    public static ConfigBuilder configBuilder() {
        return new ConfigBuilder();
    }


    /**
     * Builder for a {@link MqlParserConfig} that starts with an existing {@code Config}.
     */
    public static ConfigBuilder configBuilder( MqlParserConfig mqlParserConfig ) {
        return new ConfigBuilder().setConfig( mqlParserConfig );
    }


    /**
     * Interface to define the configuration for a SQL parser.
     *
     * @see ConfigBuilder
     */
    public interface MqlParserConfig {

        /**
         * Default configuration.
         */
        MqlParserConfig DEFAULT = configBuilder().build();


        MqlParserImplFactory parserFactory();

    }


    /**
     * Builder for a {@link MqlParserConfig}.
     */
    public static class ConfigBuilder {

        private MqlParserImplFactory parserFactory = MqlParserImpl.FACTORY;


        private ConfigBuilder() {
        }


        /**
         * Sets configuration identical to a given {@link MqlParserConfig}.
         */
        public ConfigBuilder setConfig( MqlParserConfig mqlParserConfig ) {
            this.parserFactory = mqlParserConfig.parserFactory();
            return this;
        }


        public ConfigBuilder setParserFactory( MqlParserImplFactory factory ) {
            this.parserFactory = Objects.requireNonNull( factory );
            return this;
        }


        /**
         * Builds a {@link MqlParserConfig}.
         */
        public MqlParserConfig build() {
            return new ConfigImpl( parserFactory );
        }

    }


    /**
     * Implementation of {@link MqlParserConfig}.
     * Called by builder; all values are in private final fields.
     */
    private static class ConfigImpl implements MqlParserConfig {

        private final MqlParserImplFactory parserFactory;


        private ConfigImpl( MqlParserImplFactory parserFactory ) {
            this.parserFactory = Objects.requireNonNull( parserFactory );
        }


        @Override
        public MqlParserImplFactory parserFactory() {
            return parserFactory;
        }

    }

}


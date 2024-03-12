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

package org.polypheny.db.languages;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.polypheny.db.algebra.constant.ConformanceEnum;
import org.polypheny.db.algebra.constant.Lex;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.util.Conformance;

public interface Parser {

    int DEFAULT_IDENTIFIER_MAX_LENGTH = 128;

    /**
     * Builder for a {@link ParserConfig}.
     */
    static ConfigBuilder configBuilder() {
        return new ConfigBuilder();
    }

    /**
     * Builder for a {@link ParserConfig} that starts with an existing {@code Config}.
     */
    static ConfigBuilder configBuilder( ParserConfig sqlParserConfig ) {
        return new ConfigBuilder().setConfig( sqlParserConfig );
    }

    Node parseQuery() throws NodeParseException;

    Node parseStmt() throws NodeParseException;

    default List<? extends Node> parseStmts() throws NodeParseException {
        throw new UnsupportedOperationException( "This operation is not supported by the used parser." );
    }

    /**
     * Interface to define the configuration for a SQL parser.
     *
     * @see ConfigBuilder
     */
    interface ParserConfig {

        /**
         * Default configuration.
         */
        ParserConfig DEFAULT = configBuilder().build();


        int identifierMaxLength();

        Casing quotedCasing();

        Casing unquotedCasing();

        Quoting quoting();

        Conformance conformance();

        ParserFactory parserFactory();

    }


    /**
     * Builder for a {@link ParserConfig}.
     */
    class ConfigBuilder {

        private Casing quotedCasing = Lex.POLYPHENY.quotedCasing;
        private Casing unquotedCasing = Lex.POLYPHENY.unquotedCasing;
        private Quoting quoting = Lex.POLYPHENY.quoting;
        private int identifierMaxLength = DEFAULT_IDENTIFIER_MAX_LENGTH;
        private Conformance conformance = ConformanceEnum.LENIENT;
        private ParserFactory parserFactory;


        private ConfigBuilder() {
            parserFactory = QueryLanguage.from( "sql" ).factory();
        }


        /**
         * Sets configuration identical to a given {@link ParserConfig}.
         */
        public ConfigBuilder setConfig( ParserConfig sqlParserConfig ) {
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


        public ConfigBuilder setIdentifierMaxLength( int identifierMaxLength ) {
            this.identifierMaxLength = identifierMaxLength;
            return this;
        }


        public ConfigBuilder setConformance( Conformance conformance ) {
            this.conformance = conformance;
            return this;
        }


        public ConfigBuilder setParserFactory( ParserFactory factory ) {
            this.parserFactory = Objects.requireNonNull( factory );
            return this;
        }


        public ConfigBuilder setLex( Lex lex ) {
            setUnquotedCasing( lex.unquotedCasing );
            setQuotedCasing( lex.quotedCasing );
            setQuoting( lex.quoting );
            return this;
        }


        /**
         * Builds a {@link ParserConfig}.
         */
        public ParserConfig build() {
            return new ConfigImpl( identifierMaxLength, quotedCasing, unquotedCasing, quoting, conformance, parserFactory );
        }

    }


    /**
     * Implementation of {@link ParserConfig}.
     * Called by builder; all values are in private final fields.
     */
    class ConfigImpl implements ParserConfig {

        private final int identifierMaxLength;
        private final Conformance conformance;
        private final Casing quotedCasing;
        private final Casing unquotedCasing;
        private final Quoting quoting;
        private final ParserFactory parserFactory;


        private ConfigImpl( int identifierMaxLength, Casing quotedCasing, Casing unquotedCasing, Quoting quoting, Conformance conformance, ParserFactory parserFactory ) {
            this.identifierMaxLength = identifierMaxLength;
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
        public Conformance conformance() {
            return conformance;
        }


        @Override
        public ParserFactory parserFactory() {
            return parserFactory;
        }

    }

}

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

package org.polypheny.db.cypher.parser;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.cypher.CypherStatement;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.ParserFactory;
import org.polypheny.db.languages.ParserImpl;
import org.polypheny.db.nodes.Node;

public class CypherParser implements Parser {

    private final CypherAbstractParserImpl parser;


    public CypherParser( CypherAbstractParserImpl parser ) {
        this.parser = parser;
    }


    public static CypherParser create( String query, CypherParserConfig parserConfig ) {
        ParserImpl parser = parserConfig.parserFactory().getParser( query );

        return new CypherParser( (CypherAbstractParserImpl) parser );
    }


    @Override
    public Node parseQuery() throws NodeParseException {
        return null;
    }


    @Override
    public Node parseStmt() throws NodeParseException {
        return null;
    }


    @Override
    public List<CypherStatement> parseStmts() throws NodeParseException {
        try {
            return parser.parseExpressionsEof();
        } catch ( Exception e ) {
            throw new NodeParseException( e.getMessage(), null, null, null, e );
        }
    }


    /**
     * Builder for a {@link CypherParserConfig}.
     */
    public static ConfigBuilder configBuilder() {
        return new ConfigBuilder();
    }


    /**
     * Builder for a {@link CypherParserConfig} that starts with an existing {@code Config}.
     */
    public static ConfigBuilder configBuilder( CypherParserConfig cypherParserConfig ) {
        return new ConfigBuilder().setConfig( cypherParserConfig );
    }


    /**
     * Interface to define the configuration for a SQL parser.
     *
     * @see ConfigBuilder
     */
    public interface CypherParserConfig {

        /**
         * Default configuration.
         */
        CypherParserConfig DEFAULT = configBuilder().build();


        ParserFactory parserFactory();

    }


    /**
     * Builder for a {@link CypherParserConfig}.
     */
    public static class ConfigBuilder {

        private ParserFactory parserFactory = CypherParserImpl.FACTORY;


        private ConfigBuilder() {
        }


        /**
         * Sets configuration identical to a given {@link CypherParserConfig}.
         */
        public ConfigBuilder setConfig( CypherParserConfig cypherParserConfig ) {
            this.parserFactory = cypherParserConfig.parserFactory();
            return this;
        }


        public ConfigBuilder setParserFactory( ParserFactory factory ) {
            this.parserFactory = Objects.requireNonNull( factory );
            return this;
        }


        /**
         * Builds a {@link CypherParserConfig}.
         */
        public CypherParserConfig build() {
            return new ConfigImpl( parserFactory );
        }

    }


    /**
     * Implementation of {@link CypherParserConfig}.
     * Called by builder; all values are in private final fields.
     */
    private static class ConfigImpl implements CypherParserConfig {

        private final ParserFactory parserFactory;


        private ConfigImpl( ParserFactory parserFactory ) {
            this.parserFactory = Objects.requireNonNull( parserFactory );
        }


        @Override
        public ParserFactory parserFactory() {
            return parserFactory;
        }

    }

}

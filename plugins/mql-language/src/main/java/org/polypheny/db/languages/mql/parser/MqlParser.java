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

package org.polypheny.db.languages.mql.parser;

import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.ParserFactory;
import org.polypheny.db.languages.ParserImpl;
import org.polypheny.db.languages.mql.MqlCollectionStatement;
import org.polypheny.db.languages.mql.MqlNode;
import org.polypheny.db.mql.parser.MqlParserImpl;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.util.SourceStringReader;


/**
 * A <code>MqlParser</code> parses a MQL statement.
 */
public class MqlParser implements Parser {

    public static final int DEFAULT_IDENTIFIER_MAX_LENGTH = 128;
    private static final Pattern pattern = Pattern.compile( "limit\\(\\s*[0-9]*\\s*\\)" );

    private final MqlAbstractParserImpl parser;
    private final Integer limit;


    public MqlParser( MqlAbstractParserImpl parser, Integer limit ) {
        this.limit = limit;
        this.parser = parser;
    }


    public static MqlParser create( String mql, MqlParserConfig mqlParserConfig ) {
        return create( new SourceStringReader( mql ), mqlParserConfig );
    }


    public static MqlParser create( Reader reader, MqlParserConfig mqlParserConfig ) {
        String mql = ((SourceStringReader) reader).getSourceString();
        List<String> splits = Arrays.asList( mql.split( "\\." ) );
        Integer limit = null;
        String last = splits.get( splits.size() - 1 );
        if ( pattern.matcher( last ).matches() ) {
            mql = mql.replace( "." + last, "" );
            limit = Integer.parseInt( last
                    .replace( "limit(", "" )
                    .replace( ")", "" )
                    .trim() );
        }
        ParserImpl parser = mqlParserConfig.parserFactory().getParser( new SourceStringReader( mql ) );

        return new MqlParser( (MqlAbstractParserImpl) parser, limit );
    }




    /**
     * Parses a <code>SELECT</code> statement.
     *
     * @throws NodeParseException if there is a parse error
     */
    public MqlNode parseQuery() throws NodeParseException {
        try {
            MqlNode node = parser.parseMqlStmtEof();
            if ( node instanceof MqlCollectionStatement && limit != null ) {
                ((MqlCollectionStatement) node).setLimit( limit );
            }
            return node;
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
     * Parses an SQL statement.
     *
     * @return top-level SqlNode representing stmt
     */
    public MqlNode parseStmt() throws NodeParseException {
        return parseQuery();
    }


    /**
     * Builder for a {@link MqlParserConfig}.
     */
    public static ConfigBuilder configBuilder() {
        return new ConfigBuilder();
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


        ParserFactory parserFactory();

    }


    /**
     * Builder for a {@link MqlParserConfig}.
     */
    public static class ConfigBuilder {

        private ParserFactory parserFactory = MqlParserImpl.FACTORY;


        private ConfigBuilder() {
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
    private record ConfigImpl(ParserFactory parserFactory) implements MqlParserConfig {

        private ConfigImpl( ParserFactory parserFactory ) {
            this.parserFactory = Objects.requireNonNull( parserFactory );
        }

    }

}


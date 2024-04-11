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

import com.google.common.collect.ImmutableList;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.ParserImpl;
import org.polypheny.db.languages.mql.MqlNode;


public abstract class MqlAbstractParserImpl implements ParserImpl {

    @Setter
    @Getter
    protected String originalMql;


    /**
     * Returns metadata about this parser: keywords, etc.
     */
    public abstract MqlAbstractParserImpl.Metadata getMetadata();

    /**
     * Reinitializes parser with new input.
     *
     * @param reader provides new input
     */
    public abstract void ReInit( Reader reader );


    /**
     * Removes or transforms misleading information from a parse exception or error, and converts to {@link NodeParseException}.
     *
     * @param ex dirty excn
     * @return clean excn
     */
    public abstract MqlParseException normalizeException( Throwable ex );


    /**
     * Parses a SQL expression ending with EOF and constructs a parse tree.
     *
     * @return constructed parse tree.
     */
    public abstract MqlNode parseMqlExpressionEof() throws Exception;

    /**
     * Parses a SQL statement ending with EOF and constructs a parse tree.
     *
     * @return constructed parse tree.
     */
    public abstract MqlNode parseMqlStmtEof() throws Exception;


    /**
     * Metadata about the parser. For example:
     *
     * <ul>
     * <li>"KEY" is a keyword: it is meaningful in certain contexts, such as "CREATE FOREIGN KEY", but can be used as an identifier, as in <code> "CREATE TABLE t (key INTEGER)"</code>.</li>
     * <li>"SELECT" is a reserved word. It can not be used as an identifier.</li>
     * <li>"CURRENT_USER" is the name of a context variable. It cannot be used as an identifier.</li>
     * <li>"ABS" is the name of a reserved function. It cannot be used as an identifier.</li>
     * <li>"DOMAIN" is a reserved word as specified by the SQL:92 standard.</li>
     * </ul>
     */
    public interface Metadata {

        /**
         * Returns true if token is a keyword but not a reserved word. For example, "KEY".
         */
        boolean isNonReservedKeyword( String token );

        /**
         * Returns whether token is the name of a context variable such as "CURRENT_USER".
         */
        boolean isContextVariableName( String token );

        /**
         * Returns whether token is a reserved function name such as "CURRENT_USER".
         */
        boolean isReservedFunctionName( String token );

        /**
         * Returns whether token is a keyword. (That is, a non-reserved keyword, a context variable, or a reserved function name.)
         */
        boolean isKeyword( String token );

        /**
         * Returns whether token is a reserved word.
         */
        boolean isReservedWord( String token );

        /**
         * Returns a list of all tokens in alphabetical order.
         */
        List<String> getTokens();

    }


    /**
     * Default implementation of the {@link MqlAbstractParserImpl.Metadata} interface.
     */
    public static class MetadataImpl implements MqlAbstractParserImpl.Metadata {

        private final Set<String> reservedFunctionNames = new HashSet<>();
        private final Set<String> contextVariableNames = new HashSet<>();
        private final Set<String> nonReservedKeyWordSet = new HashSet<>();

        /**
         * Set of all tokens.
         */
        private final SortedSet<String> tokenSet = new TreeSet<>();

        /**
         * Immutable list of all tokens, in alphabetical order.
         */
        private final List<String> tokenList;
        private final Set<String> reservedWords = new HashSet<>();


        /**
         * Creates a MetadataImpl.
         */
        public MetadataImpl( MqlAbstractParserImpl mqlParser ) {
            //initList( mqlParser, reservedFunctionNames, "ReservedFunctionName" );
            //initList( mqlParser, contextVariableNames, "ContextVariable" );
            //initList( mqlParser, nonReservedKeyWordSet, "NonReservedKeyWord" );
            tokenList = ImmutableList.copyOf( tokenSet );
            Set<String> reservedWordSet = new TreeSet<>();
            reservedWordSet.addAll( tokenSet );
            reservedWordSet.removeAll( nonReservedKeyWordSet );
            reservedWords.addAll( reservedWordSet );
        }


        /**
         * Initializes lists of keywords.
         */
        private void initList( MqlAbstractParserImpl parserImpl, Set<String> keywords, String name ) {
            parserImpl.ReInit( new StringReader( "1" ) );
            try {
                Object o = virtualCall( parserImpl, name );
                throw new AssertionError( "expected call to fail, got " + o );
            } catch ( NodeParseException parseException ) {
                // First time through, build the list of all tokens.
                final String[] tokenImages = parseException.getTokenImages();
                if ( tokenSet.isEmpty() ) {
                    for ( String token : tokenImages ) {
                        String tokenVal = MqlParserUtil.getTokenVal( token );
                        if ( tokenVal != null ) {
                            tokenSet.add( tokenVal );
                        }
                    }
                }

                // Add the tokens which would have been expected in this syntactic context to the list we're building.
                final int[][] expectedTokenSequences = parseException.getExpectedTokenSequences();
                for ( final int[] tokens : expectedTokenSequences ) {
                    assert tokens.length == 1;
                    final int tokenId = tokens[0];
                    String token = tokenImages[tokenId];
                    String tokenVal = MqlParserUtil.getTokenVal( token );
                    if ( tokenVal != null ) {
                        keywords.add( tokenVal );
                    }
                }
            } catch ( Throwable e ) {
                throw new GenericRuntimeException( "While building token lists", e );
            }
        }


        /**
         * Uses reflection to invoke a method on this parser. The method must be public and have no parameters.
         *
         * @param parserImpl Parser
         * @param name Name of method. For example "ReservedFunctionName".
         * @return Result of calling method
         */
        private Object virtualCall( MqlAbstractParserImpl parserImpl, String name ) throws Throwable {
            Class<?> clazz = parserImpl.getClass();
            try {
                final Method method = clazz.getMethod( name, (Class[]) null );
                return method.invoke( parserImpl, (Object[]) null );
            } catch ( InvocationTargetException e ) {
                Throwable cause = e.getCause();
                throw parserImpl.normalizeException( cause );
            }
        }


        @Override
        public List<String> getTokens() {
            return tokenList;
        }


        @Override
        public boolean isKeyword( String token ) {
            return isNonReservedKeyword( token )
                    || isReservedFunctionName( token )
                    || isContextVariableName( token )
                    || isReservedWord( token );
        }


        @Override
        public boolean isNonReservedKeyword( String token ) {
            return nonReservedKeyWordSet.contains( token );
        }


        @Override
        public boolean isReservedFunctionName( String token ) {
            return reservedFunctionNames.contains( token );
        }


        @Override
        public boolean isContextVariableName( String token ) {
            return contextVariableNames.contains( token );
        }


        @Override
        public boolean isReservedWord( String token ) {
            return reservedWords.contains( token );
        }

    }

}

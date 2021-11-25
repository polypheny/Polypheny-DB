/*
 * Copyright 2019-2021 The Polypheny Project
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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.sql.parser.SqlParser;
import org.polypheny.db.util.PolyphenyDbParserException;


/**
 * SqlParseException defines a checked exception corresponding to {@link SqlParser}.
 */
public class MqlParseException extends NodeParseException implements PolyphenyDbParserException {

    private final ParserPos pos;
    private final int[][] expectedTokenSequences;
    private final String[] tokenImages;

    /**
     * The original exception thrown by the generated parser. Unfortunately, each generated parser throws exceptions of a different class. So, we keep the exception for forensic purposes, but don't print it publicly.
     *
     * Also, make it transient, because it is a ParseException generated by JavaCC and contains a non-serializable Token.
     */
    private final transient Throwable parserException;


    /**
     * Creates a SqlParseException.
     *
     * @param message Message
     * @param pos Position
     * @param expectedTokenSequences Token sequences
     * @param tokenImages Token images
     * @param parserException Parser exception
     */
    public MqlParseException( String message, ParserPos pos, int[][] expectedTokenSequences, String[] tokenImages, Throwable parserException ) {
        super( message, pos, expectedTokenSequences, tokenImages, parserException );
        // Cause must be null because the exception generated by JavaCC contains a Token and is therefore not serializable (even though it
        // implements the Serializable interface). This is serious: one non-serializable object poisons the entire chain, so the stack cannot be transmitted over Java RMI.
        this.pos = pos;
        this.expectedTokenSequences = expectedTokenSequences;
        this.tokenImages = tokenImages;
        this.parserException = parserException;
    }


    /**
     * Returns the position where this error occurred.
     *
     * @return parser position
     */
    public ParserPos getPos() {
        return pos;
    }


    /**
     * Returns a list of the token names which could have legally occurred at this point.
     *
     * If some of the alternatives contain multiple tokens, returns the last token of only these longest sequences. (This occurs when the parser is maintaining more than the usual lookup.)
     * For instance, if the possible tokens are
     *
     * <blockquote>
     * <pre>
     * {"IN"}
     * {"BETWEEN"}
     * {"LIKE"}
     * {"=", "&lt;IDENTIFIER&gt;"}
     * {"=", "USER"}
     * </pre>
     * </blockquote>
     *
     * returns
     *
     * <blockquote>
     * <pre>
     * "&lt;IDENTIFIER&gt;"
     * "USER"
     * </pre>
     * </blockquote>
     *
     * @return list of token names which could have occurred at this point
     */
    public Collection<String> getExpectedTokenNames() {
        if ( expectedTokenSequences == null ) {
            return Collections.emptyList();
        }
        int maxLength = 0;
        for ( int[] expectedTokenSequence : expectedTokenSequences ) {
            maxLength = Math.max( expectedTokenSequence.length, maxLength );
        }
        final Set<String> set = new TreeSet<>();
        for ( int[] expectedTokenSequence : expectedTokenSequences ) {
            if ( expectedTokenSequence.length == maxLength ) {
                set.add( tokenImages[expectedTokenSequence[expectedTokenSequence.length - 1]] );
            }
        }
        return set;
    }


    /**
     * Returns the token images.
     *
     * @return token images
     */
    public String[] getTokenImages() {
        return tokenImages;
    }


    /**
     * Returns the expected token sequences.
     *
     * @return expected token sequences
     */
    public int[][] getExpectedTokenSequences() {
        return expectedTokenSequences;
    }


    // override Exception
    @Override
    public Throwable getCause() {
        return parserException;
    }


    /**
     * Per {@link java.io.Serializable} API, provides a replacement object to be written during serialization.
     *
     * SqlParseException is serializable but is not available on the client.
     * This implementation converts this SqlParseException into a vanilla {@link RuntimeException} with the same message.
     */
    private Object writeReplace() {
        return new RuntimeException( getClass().getName() + ": " + getMessage() );
    }

}

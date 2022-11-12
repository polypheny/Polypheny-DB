/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.cypher;

import org.polypheny.db.cypher.parser.ParseException;
import org.polypheny.db.cypher.parser.Token;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.ParserPos;

public class CypherParseException extends NodeParseException {

    /**
     * Creates a CypherParseException.
     *
     * @param message Message
     * @param pos Position
     * @param expectedTokenSequences Token sequences
     * @param tokenImages Token images
     * @param parserException Parser exception
     */
    public CypherParseException( String message, ParserPos pos, int[][] expectedTokenSequences, String[] tokenImages, Throwable parserException ) {
        super( message, pos, expectedTokenSequences, tokenImages, parserException );
    }


    public CypherParseException( ParseException e, Token t ) {
        // fix
        super( e.getMessage(), null, e.expectedTokenSequences, e.tokenImage, e );
    }


    public CypherParseException( String image, int[][] expectedTokenSequences, String[] tokenImage, ParseException e, int i, int endLine, int i1 ) {
        super( image, new ParserPos( i, endLine ), expectedTokenSequences, tokenImage, e );
    }

}

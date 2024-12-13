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

package org.polypheny.db.algebra.polyalg.parser;

import java.io.Reader;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.ParserImpl;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.util.SourceStringReader;

public class PolyAlgParser implements Parser {

    private final PolyAlgAbstractParserImpl parser;


    public PolyAlgParser( PolyAlgAbstractParserImpl parser ) {
        this.parser = parser;
    }


    public static PolyAlgParser create( String polyAlg ) {
        return create( new SourceStringReader( polyAlg ) );
    }


    public static PolyAlgParser create( Reader reader ) {
        ParserImpl parser = PolyAlgParserImpl.FACTORY.getParser( reader );
        return new PolyAlgParser( (PolyAlgAbstractParserImpl) parser );

    }


    @Override
    public Node parseQuery() throws NodeParseException {
        try {
            return parser.parsePolyAlgEof();

        } catch ( Throwable ex ) {
            throw parser.normalizeException( ex );
        }
    }


    @Override
    public Node parseStmt() throws NodeParseException {
        return parseQuery();
    }

}

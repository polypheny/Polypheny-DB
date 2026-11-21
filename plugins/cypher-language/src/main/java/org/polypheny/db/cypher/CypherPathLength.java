/*
 * Copyright 2019-2023 The Polypheny Project
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

import java.util.regex.Pattern;
import lombok.Getter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.util.Pair;

@Getter
public class CypherPathLength extends CypherNode {

    // The parser shares its numeric tokens with scalar expressions, which allow other spellings.
    private static final Pattern BOUND_LITERAL = Pattern.compile( "0|[1-9][0-9]*|0x[0-9a-fA-F]+|0o[0-7]+" );

    private final ParserPos fromPos;
    private final ParserPos toPos;
    private final String from;
    private final String to;
    private final Pair<Integer, Integer> lengthRange;


    public CypherPathLength( ParserPos pos, ParserPos fromPos, ParserPos toPos, String from, String to ) {
        super( pos );
        this.fromPos = fromPos;
        this.toPos = toPos;
        this.from = from;
        this.to = to;
        long min = from == null ? 1 : parseBound( from );
        Long max = to == null ? null : parseBound( to );
        // A path is backed by int-sized collections, and cannot contain more edges than the graph.
        this.lengthRange = min > Integer.MAX_VALUE
                ? Pair.of( 1, 0 )
                : Pair.of( (int) min, max == null ? null : (int) Math.min( max, Integer.MAX_VALUE ) );
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.PATH_LENGTH;
    }


    private static long parseBound( String bound ) {
        if ( !BOUND_LITERAL.matcher( bound ).matches() ) {
            throw new GenericRuntimeException( "Invalid Cypher relationship length: %s", bound );
        }
        int radix = bound.startsWith( "0x" ) ? 16 : bound.startsWith( "0o" ) ? 8 : 10;
        int offset = radix == 10 ? 0 : 2;
        try {
            return Long.parseLong( bound, offset, bound.length(), radix );
        } catch ( NumberFormatException e ) {
            throw new GenericRuntimeException( "Cypher relationship length is outside the supported integer range.", e );
        }
    }

}

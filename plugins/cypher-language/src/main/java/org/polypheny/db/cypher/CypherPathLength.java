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

import lombok.Getter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.util.Pair;

@Getter
public class CypherPathLength extends CypherNode {


    private ParserPos fromPos;
    private ParserPos toPos;
    private String from;
    private String to;


    public CypherPathLength( ParserPos pos, ParserPos fromPos, ParserPos toPos, String from, String to ) {
        super( pos );
        this.fromPos = fromPos;
        this.toPos = toPos;
        this.from = from;
        this.to = to;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.PATH_LENGTH;
    }


    public Pair<Integer, Integer> getLengthRange() {
        long min = from == null ? 1 : parseBound( from );
        Long max = to == null ? null : parseBound( to );
        // A path is backed by int-sized collections, and cannot contain more edges than the graph.
        if ( min > Integer.MAX_VALUE ) {
            return Pair.of( 1, 0 );
        }
        return Pair.of( (int) min, max == null ? null : (int) Math.min( max, Integer.MAX_VALUE ) );
    }


    private static long parseBound( String bound ) {
        int radix = bound.startsWith( "0x" ) || bound.startsWith( "0X" ) ? 16 : bound.startsWith( "0o" ) ? 8 : 10;
        int offset = radix == 10 ? 0 : 2;
        boolean maySeparate = offset > 0;
        long value = 0;
        try {
            for ( int i = offset; i < bound.length(); i++ ) {
                char character = bound.charAt( i );
                if ( character == '_' && maySeparate ) {
                    maySeparate = false;
                    continue;
                }
                int digit = character <= 127 ? Character.digit( character, radix ) : -1;
                if ( digit < 0 ) {
                    throw new GenericRuntimeException( "Invalid Cypher relationship length: %s", bound );
                }
                value = Math.addExact( Math.multiplyExact( value, radix ), digit );
                maySeparate = true;
            }
        } catch ( ArithmeticException e ) {
            throw new GenericRuntimeException( "Cypher relationship length is outside the supported integer range.", e );
        }
        if ( offset == bound.length() || !maySeparate ) {
            throw new GenericRuntimeException( "Invalid Cypher relationship length: %s", bound );
        }
        return value;
    }

}

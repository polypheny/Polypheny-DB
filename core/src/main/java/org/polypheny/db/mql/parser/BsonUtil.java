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

package org.polypheny.db.mql.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.polypheny.db.util.Pair;

public class BsonUtil {

    private final static List<Pair<String, String>> mappings = new ArrayList<>();


    static {
        mappings.add( new Pair<>( "\\+", "$add" ) );
        mappings.add( new Pair<>( "\\-", "$subtract" ) );
        mappings.add( new Pair<>( "\\*", "$multiply" ) );
        mappings.add( new Pair<>( "\\/", "$divide" ) );
    }


    public static List<BsonDocument> trySplit( String documents ) {
        int openedCount = 0;
        int lastEnd = 0;
        List<Pair<Integer, Integer>> intervals = new ArrayList<>();
        char now;
        for ( int i = 0; i < documents.length(); i++ ) {
            now = documents.charAt( i );

            if ( now == '{' ) {
                openedCount++;
            } else if ( now == '}' && i != documents.length() - 1 ) {
                openedCount--;
            } else if ( (now == ',' && openedCount == 0) ) {
                // we are either between two documents },{ or at the end of the last document }
                // we are between two documents "},{"
                intervals.add( new Pair<>( lastEnd, i - 1 ) );
                lastEnd = i + 1;
            } else if ( now == '}' && i == documents.length() - 1 && openedCount == 1 ) {
                intervals.add( new Pair<>( lastEnd, i ) );
            }

        }

        return intervals
                .stream()
                .map( interval -> BsonDocument.parse( BsonUtil.fixBson( documents.substring( interval.left, interval.right + 1 ) ) ) ) // +1 due to smaller than handling of substring
                .collect( Collectors.toList() );

    }


    /**
     * operations which include /*+_ cannot be parsed by the bsonDocument parser
     * so they need to be replace by a equivalent bson compatible operation
     * 1-3*10 -> {$subtract: [1, {$multiply:[3,10]}]}
     *
     * @param bson the full bson string
     * @return the initial bson string with the exchanged calculation
     *
     * TODO DL: edge-case in string is not handled properly
     */
    public static String fixBson( String bson ) {
        String reg = "[a-zA-B0-9$.\"]+(\\s*[*/+-]\\s*[a-zA-B0-9$.\"]+)+";

        if ( bson.split( reg ).length == 1 ) {
            return bson;
        }

        Pattern p = Pattern.compile( reg );
        Matcher m = p.matcher( bson );

        while ( m.find() ) {
            String match = m.group( 0 );
            String calculation = fixCalculation( match, 0 );
            bson = bson.replace( match, calculation );
        }

        return bson;
    }


    private static String fixCalculation( String calculation, int depth ) {
        if ( depth > mappings.size() - 1 ) {
            return calculation;
        }

        Pair<String, String> entry = mappings.get( depth );
        String[] splits = calculation.split( entry.getKey() );

        if ( splits.length > 1 ) {
            List<String> parts = Arrays.stream( splits ).map( s -> fixCalculation( s, depth + 1 ) ).collect( Collectors.toList() );
            return "{" + entry.getValue() + " : [" + String.join( ",", parts ) + "]}";
        } else {
            return fixCalculation( calculation, depth + 1 );
        }
    }

}

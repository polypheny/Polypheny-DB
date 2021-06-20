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
import java.util.List;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.polypheny.db.util.Pair;

public class BsonUtil {

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
                .map( interval -> BsonDocument.parse( documents.substring( interval.left, interval.right + 1 ) ) ) // +1 due to smaller than handling of substring
                .collect( Collectors.toList() );

    }

}

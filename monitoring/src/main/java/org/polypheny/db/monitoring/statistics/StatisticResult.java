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

package org.polypheny.db.monitoring.statistics;

import lombok.Getter;


/**
 * Contains statistics for multiple columns.
 */
public class StatisticResult {

    @Getter
    private StatisticQueryResult[] columns;


    public StatisticResult() {
    }


    public StatisticResult( StatisticQueryResult[] columns ) {
        this.columns = columns;
    }


    /**
     * Constructor which transforms an answer-array into multiple StatisticColumns
     *
     * @param data answer per stat as a two-dimensional array
     */
    public StatisticResult( QueryResult queryResult, Comparable<?>[][] data ) {
        if ( data.length == 0 || data[0].length == 0 ) {
            this.columns = new StatisticQueryResult[0];
        } else {
            this.columns = new StatisticQueryResult[data[0].length];

            Comparable<?>[][] rotated = rotate2dArray( data );

            for ( int i = 0; i < rotated.length; i++ ) {
                this.columns[i] = new StatisticQueryResult( queryResult, rotated[i] );
            }
        }
    }


    /**
     * Rotates a 2d array counterclockwise
     * Assumes 2d array is equally long in all "sub"arrays
     */
    private Comparable<?>[][] rotate2dArray( Comparable<?>[][] data ) {
        int width = data[0].length;
        int height = data.length;

        Comparable<?>[][] rotated = new Comparable<?>[width][height];

        for ( int x = 0; x < width; x++ ) {
            for ( int y = 0; y < height; y++ ) {
                rotated[x][y] = data[y][x];
            }
        }
        return rotated;
    }

//    /**
//     * Transforms an StatisticResult, which has to consist of <b>value</b> and <b>occurrence</b> of a column, into a map
//     *
//     * @return map with <b>value</b> as key and <b>occurrence</b> as value
//     */
//    public static <E> Map<E, Integer> toOccurrenceMap( StatisticResult stats ) {
//        HashMap<E, Integer> map = new HashMap<>();
//        String[] values = stats.getColumns()[0].getData();
//        String[] occurrences = stats.getColumns()[1].getData();
//        //TODO: handle mismatch
//        for ( int i = 0; i < values.length; i++ ) {
//            map.put( (E) values[i], Integer.parseInt( occurrences[i] ) );
//        }
//        return map;
//    }

}

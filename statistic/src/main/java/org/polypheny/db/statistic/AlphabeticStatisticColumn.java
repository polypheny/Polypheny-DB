/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.statistic;


import org.polypheny.db.PolySqlType;
import org.polypheny.db.config.RuntimeConfig;
import com.google.gson.annotations.Expose;
import lombok.extern.slf4j.Slf4j;


/**
 * Stores the available statistic data of a specific column
 * Responsible to validate if data should be changed
 */
@Slf4j
public class AlphabeticStatisticColumn<T extends Comparable<T>> extends StatisticColumn<T> {

    @Expose
    private final String columnType = "alphabetic";


    public AlphabeticStatisticColumn( String schema, String table, String column, PolySqlType type ) {
        super( schema, table, column, type );
    }


    public AlphabeticStatisticColumn( String[] splitColumn, PolySqlType type ) {
        super( splitColumn[0], splitColumn[1], splitColumn[2], type );
    }


    @Override
    public void insert( T val ) {
        if ( uniqueValues.size() < RuntimeConfig.STATISTIC_BUFFER.getInteger() ) {
            if ( !uniqueValues.contains( val ) ) {
                uniqueValues.add( val );
            }
        } else {
            isFull = true;
        }
    }


    @Override
    public String toString() {
        String statistics = "";
        statistics += "count: " + count;
        statistics += "unique Value: " + uniqueValues.toString();
        return statistics;
    }

}

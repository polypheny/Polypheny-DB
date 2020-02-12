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

package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import com.google.gson.annotations.Expose;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * Stores the available statistic data of a specific column
 * Responsible to validate if data should be changed
 */
@Slf4j
public class NumericalStatisticColumn<T extends Comparable<T>> extends StatisticColumn<T> {

    @Expose
    private final String columnType = "numeric";

    @Expose
    @Getter
    @Setter
    private T min;

    @Expose
    @Getter
    @Setter
    private T max;


    public NumericalStatisticColumn( String schema, String table, String column, PolySqlType type ) {
        super( schema, table, column, type );
    }


    public NumericalStatisticColumn( String[] splitColumn, PolySqlType type ) {
        super( splitColumn, type );
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
        if ( min == null ) {
            min = val;
            max = val;
        } else if ( val.compareTo( min ) < 0 ) {
            this.min = val;
        } else if ( val.compareTo( max ) > 0 ) {
            this.max = val;
        }
    }


    @Override
    public String toString() {
        String statistics = "";
        statistics += "min: " + min;
        statistics += "max: " + max;
        statistics += "count: " + count;
        statistics += "unique Value: " + uniqueValues.toString();
        return statistics;
    }

}

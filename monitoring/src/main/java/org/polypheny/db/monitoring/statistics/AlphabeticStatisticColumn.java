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


import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.RuntimeConfig;


/**
 * Stores the available statistic data of a specific column
 * Responsible to validate if data should be changed
 */
@Slf4j
public class AlphabeticStatisticColumn<T extends Comparable<T>> extends StatisticColumn<T> {

    @Getter
    public List<T> uniqueValuesCache = new ArrayList<>();
    boolean cacheFull;


    public AlphabeticStatisticColumn( QueryResult column ) {
        super( column.getSchemaId(), column.getTableId(), column.getColumnId(), column.getType() );
    }


    @Override
    public void insert( T val ) {
        if ( uniqueValues.size() < RuntimeConfig.STATISTIC_BUFFER.getInteger() ) {
            if ( !uniqueValues.contains( val ) ) {
                uniqueValues.add( val );
            }
        } else {
            full = true;
            if ( uniqueValuesCache.size() < (RuntimeConfig.STATISTIC_BUFFER.getInteger() * 2) ) {
                uniqueValuesCache.add( val );
            } else {
                cacheFull = true;
            }
        }
    }


    @Override
    public void insert( List<T> values ) {
        if ( values != null && !(values.get( 0 ) instanceof ArrayList) ) {
            for ( T val : values ) {
                insert( val );
            }
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

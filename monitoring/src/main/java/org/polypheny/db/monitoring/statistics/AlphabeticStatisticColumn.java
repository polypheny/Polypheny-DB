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

package org.polypheny.db.monitoring.statistics;


import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Stores the available statistic data of a specific column
 * Responsible to validate if data should be changed
 */
@Slf4j
public class AlphabeticStatisticColumn extends StatisticColumn {

    @Getter
    public List<PolyString> uniqueValuesCache = new ArrayList<>();
    boolean cacheFull;


    public AlphabeticStatisticColumn( QueryResult column ) {
        super( column.getColumn().id, column.getColumn().type );
    }


    @Override
    public void insert( PolyValue val ) {
        if ( uniqueValues.size() < RuntimeConfig.STATISTIC_BUFFER.getInteger() ) {
            if ( !uniqueValues.contains( val ) ) {
                uniqueValues.add( val );
            }
        } else {
            full = true;
            if ( uniqueValuesCache.size() < (RuntimeConfig.STATISTIC_BUFFER.getInteger() * 2) ) {
                uniqueValuesCache.add( val.asString() );
            } else {
                cacheFull = true;
            }
        }
    }


    @Override
    public void insert( List<PolyValue> values ) {
        if ( values == null ) {
            return;
        }

        for ( PolyValue val : values ) {
            insert( val == null ? null : val.asString() );
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

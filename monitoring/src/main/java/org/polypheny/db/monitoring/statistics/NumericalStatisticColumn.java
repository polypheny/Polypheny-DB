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

package org.polypheny.db.monitoring.statistics;


import com.google.gson.annotations.Expose;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.RuntimeConfig;


/**
 * Stores the available statistic data of a specific column
 * Responsible to validate if data should be changed
 */
@Slf4j
public class NumericalStatisticColumn extends StatisticColumn<Number> {

    @Expose
    @Getter
    @Setter
    private Number min;

    @Expose
    @Getter
    @Setter
    private Number max;

    @Getter
    private final TreeSet<Number> minCache = new TreeSet<>( Comparator.comparingDouble( Number::doubleValue ) );
    @Getter
    private final TreeSet<Number> maxCache = new TreeSet<>( Comparator.comparingDouble( Number::doubleValue ) );


    public NumericalStatisticColumn( QueryResult column ) {
        super( column.getColumn().schemaId, column.getEntity().id, column.getColumn().id, column.getColumn().type, StatisticType.NUMERICAL );
    }


    @Override
    public void insert( List<Number> values ) {
        if ( values != null && !(values.get( 0 ) instanceof List) ) {
            for ( Number val : values ) {
                if ( val != null ) {
                    insert( val );
                }
            }
        }
    }


    @Override
    public void insert( Number val ) {
        if ( uniqueValues.size() < RuntimeConfig.STATISTIC_BUFFER.getInteger() ) {
            if ( !uniqueValues.contains( val ) ) {
                if ( !uniqueValues.isEmpty() ) {
                    uniqueValues.add( val );
                }
                minCache.add( val );
                maxCache.add( val );
            }
        } else {
            full = true;
        }
        if ( val == null ) {
            return;
        }

        if ( min == null ) {
            min = val;
            max = val;
        } else if ( val.doubleValue() < min.doubleValue() ) {
            this.min = val;
        } else if ( val.doubleValue() > min.doubleValue() ) {
            this.max = val;
        }

        if ( minCache.last().doubleValue() > val.doubleValue() ) {
            if ( minCache.size() > RuntimeConfig.STATISTIC_BUFFER.getInteger() ) {
                minCache.remove( minCache.last() );
            }
            minCache.add( val );
        }

        if ( maxCache.first().doubleValue() < val.doubleValue() ) {
            if ( maxCache.size() > RuntimeConfig.STATISTIC_BUFFER.getInteger() ) {
                maxCache.remove( maxCache.first() );
            }
            maxCache.add( val );
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

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
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.TimestampString;


/**
 * Stores the available statistic data of a specific column.
 * Responsible to validate if data should be changed.
 */
@Slf4j
public class TemporalStatisticColumn<T extends Comparable<T>> extends StatisticColumn<T> {

    public void setMin( T min ) {
        this.min = min;
        this.minSinceEpoch = getSinceEpoch( min );
    }


    public void setMax( T max ) {
        this.max = max;
        this.maxSinceEpoch = getSinceEpoch( max );
    }


    @Expose
    @Getter
    private T min;

    private Long minSinceEpoch;

    @Expose
    @Getter
    private T max;

    private Long maxSinceEpoch;

    @Expose
    @Getter
    @Setter
    private String temporalType;

    @Getter
    public TreeSet<T> minCache = new TreeSet<>();
    @Getter
    public TreeSet<T> maxCache = new TreeSet<>();


    public TemporalStatisticColumn( QueryResult column ) {
        super( column.getSchemaId(), column.getTableId(), column.getColumnId(), column.getType(), StatisticType.TEMPORAL );
        temporalType = column.getType().getFamily().name();
    }


    @Override
    public void insert( T original ) {

        if ( uniqueValues.size() < RuntimeConfig.STATISTIC_BUFFER.getInteger() ) {
            if ( !uniqueValues.contains( original ) ) {
                uniqueValues.add( original );
                if ( original != null ) {
                    minCache.add( original );
                    maxCache.add( original );
                }
            }
        } else {
            full = true;
        }

        if ( original == null ) {
            return;
        }

        long val = getSinceEpoch( original );

        if ( min == null ) {
            setMin( original );
            setMax( original );
        } else if ( val < minSinceEpoch ) {
            setMin( original );
        } else if ( val > maxSinceEpoch ) {
            setMax( original );
        }

        if ( getSinceEpoch( minCache.last() ) > val ) {
            if ( minCache.size() > RuntimeConfig.STATISTIC_BUFFER.getInteger() ) {
                minCache.remove( minCache.last() );
            }
            minCache.add( original );
        }

        if ( getSinceEpoch( maxCache.first() ) < val ) {
            if ( maxCache.size() > RuntimeConfig.STATISTIC_BUFFER.getInteger() ) {
                maxCache.remove( maxCache.first() );
            }
            maxCache.add( original );
        }
    }


    private long getSinceEpoch( T val ) {
        if ( val instanceof Long ) {
            return (Long) val;
        } else if ( val instanceof Integer ) {
            return (Integer) val;
        } else if ( val instanceof Timestamp ) {
            return ((Timestamp) val).getTime();
        } else if ( val instanceof TimestampString ) {
            return ((TimestampString) val).getMillisSinceEpoch();
        } else if ( val instanceof Date ) {
            return ((Date) val).getTime();
        } else if ( val instanceof DateString ) {
            return ((DateString) val).getMillisSinceEpoch();
        }

        throw new RuntimeException();
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
        statistics += "min: " + min;
        statistics += "max: " + max;
        statistics += "count: " + count;
        statistics += "unique Value: " + uniqueValues.toString();
        return statistics;
    }

}

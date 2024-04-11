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


import com.google.gson.annotations.Expose;
import java.util.List;
import java.util.TreeSet;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyTemporal;


/**
 * Stores the available statistic data of a specific column.
 * Responsible to validate if data should be changed.
 */
@Getter
@Slf4j
public class TemporalStatisticColumn extends StatisticColumn {


    @Expose
    @Setter
    private PolyTemporal min;

    @Expose
    @Setter
    private PolyTemporal max;

    @Expose
    @Setter
    private String temporalType;


    public TreeSet<PolyTemporal> minCache = new TreeSet<>();
    public TreeSet<PolyTemporal> maxCache = new TreeSet<>();


    public TemporalStatisticColumn( QueryResult column ) {
        super( column.getColumn().namespaceId, column.getColumn().type );
        temporalType = column.getColumn().type.getFamily().name();
    }


    @Override
    public void insert( PolyValue val ) {
        if ( uniqueValues.size() < RuntimeConfig.STATISTIC_BUFFER.getInteger() ) {
            if ( !uniqueValues.contains( val ) ) {
                uniqueValues.add( val );
                if ( val != null ) {
                    minCache.add( val.asTemporal() );
                    maxCache.add( val.asTemporal() );
                }
            }
        } else {
            full = true;
        }

        if ( val == null ) {
            return;
        }

        if ( min == null ) {
            min = val.asTemporal();
            max = val.asTemporal();
        } else if ( val.compareTo( min ) < 0 ) {
            this.min = val.asTemporal();
        } else if ( val.compareTo( max ) > 0 ) {
            this.max = val.asTemporal();
        }

        if ( minCache.last().compareTo( val ) > 0 ) {
            if ( minCache.size() > RuntimeConfig.STATISTIC_BUFFER.getInteger() ) {
                minCache.remove( minCache.last() );
            }
            minCache.add( val.asTemporal() );
        }

        if ( maxCache.first().compareTo( val ) < 0 ) {
            if ( maxCache.size() > RuntimeConfig.STATISTIC_BUFFER.getInteger() ) {
                maxCache.remove( maxCache.first() );
            }
            maxCache.add( val.asTemporal() );
        }
    }


    @Override
    public void insert( List<PolyValue> values ) {
        if ( values != null ) {
            for ( PolyValue val : values ) {
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

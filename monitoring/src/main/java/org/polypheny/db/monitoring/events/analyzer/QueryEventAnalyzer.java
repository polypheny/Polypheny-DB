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

package org.polypheny.db.monitoring.events.analyzer;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.information.InformationDuration;
import org.polypheny.db.monitoring.events.QueryEvent;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;

@Slf4j
public class QueryEventAnalyzer {

    public static QueryDataPointImpl analyze( QueryEvent queryEvent ) {
        QueryDataPointImpl metric = QueryDataPointImpl
                .builder()
                .Id( queryEvent.getId() )
                .fieldNames( queryEvent.getFieldNames() )
                .executionTime( queryEvent.getExecutionTime() )
                .rowCount( queryEvent.getRowCount() )
                .isSubQuery( queryEvent.isSubQuery() )
                .recordedTimestamp( queryEvent.getRecordedTimestamp() )
                .relCompareString( queryEvent.getRelCompareString() )
                .accessedPartitions( queryEvent.getAccessedPartitions() )
                .queryId( queryEvent.getLogicalQueryInformation().getQueryId() )
                .monitoringType( "SELECT" )
                .physicalQueryId( queryEvent.getPhysicalQueryId() )
                .build();
        metric.getTables().addAll( queryEvent.getLogicalQueryInformation().getTables() );

        if ( queryEvent.isAnalyze() ) {
            processDurationInfo( queryEvent, metric );

        }

        return metric;
    }


    private static void processDurationInfo( QueryEvent queryEvent, QueryDataPointImpl metric ) {
        InformationDuration duration = new Gson().fromJson( queryEvent.getDurations(), InformationDuration.class );
        getDurationInfo( metric, "Plan Caching", duration );
        getDurationInfo( metric, "Index Lookup Rewrite", duration );
        getDurationInfo( metric, "Constraint Enforcement", duration );
        getDurationInfo( metric, "Implementation Caching", duration );
        getDurationInfo( metric, "Index Update", duration );
        getDurationInfo( metric, "Routing", duration );
        getDurationInfo( metric, "Planning & Optimization", duration );
        getDurationInfo( metric, "Implementation", duration );
        getDurationInfo( metric, "Locking", duration );
        getDurationInfo( metric, "Analyze", duration );
        getDurationInfo( metric, "Plan Selection", duration );
        getDurationInfo( metric, "Parameter validation", duration );
    }


    private static void getDurationInfo( QueryDataPointImpl queryMetric, String durationName, InformationDuration duration ) {
        long time = duration.getDuration( durationName );
        queryMetric.getDataElements().put( durationName, time );
    }

}

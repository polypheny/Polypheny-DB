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
import org.polypheny.db.monitoring.events.metrics.QueryDataPoint;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;


@Slf4j
public class QueryEventAnalyzer {

    public static QueryDataPoint analyze( QueryEvent queryEvent ) {
        QueryDataPoint metric = QueryDataPoint
                .builder()
                .description( queryEvent.getDescription() )
                .monitoringType( queryEvent.getMonitoringType() )
                .Id( queryEvent.getId() )
                .fieldNames( queryEvent.getFieldNames() )
                .executionTime( queryEvent.getExecutionTime() )
                .rowCount( queryEvent.getRowCount() )
                .isSubQuery( queryEvent.isSubQuery() )
                .recordedTimestamp( queryEvent.getRecordedTimestamp() )
                .accessedPartitions( queryEvent.getAccessedPartitions() )
                .build();

        AlgRoot relRoot = queryEvent.getRouted();
        if ( relRoot != null ) {
            AlgNode node = relRoot.alg;
            processRelNode( node, queryEvent, metric );
        }

        //if ( queryEvent.isAnalyze() ) {
        //    processDurationInfo( queryEvent, metric );
        //}

        return metric;
    }


    private static void processDurationInfo( QueryEvent queryEvent, QueryDataPoint metric ) {
        InformationDuration duration = new Gson().fromJson( queryEvent.getDurations(), InformationDuration.class );
        getDurationInfo( metric, "Index Lookup Rewrite", duration );
        getDurationInfo( metric, "Constraint Enforcement", duration );
        getDurationInfo( metric, "Implementation Caching", duration );
        getDurationInfo( metric, "Index Update", duration );
        getDurationInfo( metric, "Routing", duration );
        getDurationInfo( metric, "Planning & Optimization", duration );
        getDurationInfo( metric, "Implementation", duration );
        getDurationInfo( metric, "Locking", duration );
    }


    private static void getDurationInfo( QueryDataPoint queryMetric, String durationName, InformationDuration duration ) {
        long time = duration.getNanoDuration( durationName );
        queryMetric.getDataElements().put( durationName, time );
    }


    private static void processRelNode( AlgNode node, QueryEvent event, QueryDataPoint metric ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            processRelNode( node.getInput( i ), event, metric );
        }

        if ( node.getTable() != null ) {
            metric.getTables().addAll( node.getTable().getQualifiedName() );
        }
    }

}

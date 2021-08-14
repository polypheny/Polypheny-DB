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
import org.polypheny.db.monitoring.events.DmlEvent;
import org.polypheny.db.monitoring.events.metrics.DmlDataPoint;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;


@Slf4j
public class DmlEventAnalyzer {

    public static DmlDataPoint analyze( DmlEvent dmlEvent ) {
        DmlDataPoint metric = DmlDataPoint
                .builder()
                .description( dmlEvent.getDescription() )
                .Id( dmlEvent.getId() )
                .fieldNames( dmlEvent.getFieldNames() )
                .executionTime( dmlEvent.getExecutionTime() )
                .rowCount( dmlEvent.getRowCount() )
                .isSubQuery( dmlEvent.isSubQuery() )
                .recordedTimestamp( dmlEvent.getRecordedTimestamp() )
                .build();

        RelRoot relRoot = dmlEvent.getRouted();
        if ( relRoot != null ) {
            RelNode node = relRoot.rel;
            processRelNode( node, dmlEvent, metric );
        }

        if ( dmlEvent.isAnalyze() ) {
            processDurationInfo( dmlEvent, metric );
        }

        return metric;
    }


    private static void processDurationInfo( DmlEvent dmlEvent, DmlDataPoint metric ) {
        InformationDuration duration = new Gson().fromJson( dmlEvent.getDurations(), InformationDuration.class );
        getDurationInfo( metric, "Plan Caching", duration );
        getDurationInfo( metric, "Index Lookup Rewrite", duration );
        getDurationInfo( metric, "Constraint Enforcement", duration );
        getDurationInfo( metric, "Implementation Caching", duration );
        getDurationInfo( metric, "Index Update", duration );
        getDurationInfo( metric, "Routing", duration );
        getDurationInfo( metric, "Planning & Optimization", duration );
        getDurationInfo( metric, "Implementation", duration );
        getDurationInfo( metric, "Locking", duration );
    }


    private static void getDurationInfo( DmlDataPoint dmlMetric, String durationName, InformationDuration duration ) {
        long time = duration.getNanoDuration( durationName );
        dmlMetric.getDataElements().put( durationName, time );
    }


    private static void processRelNode( RelNode node, DmlEvent event, DmlDataPoint metric ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            processRelNode( node.getInput( i ), event, metric );
        }

        if ( node.getTable() != null ) {
            metric.getTables().addAll( node.getTable().getQualifiedName() );
        }
    }

}

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


import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.monitoring.events.DmlEvent;
import org.polypheny.db.monitoring.events.metrics.DmlDataPoint;


@Slf4j
public class DmlEventAnalyzer {

    public static DmlDataPoint analyze( DmlEvent dmlEvent ) {
        DmlDataPoint metric = DmlDataPoint
                .builder()
                .Id( dmlEvent.getId() )
                .fieldNames( dmlEvent.getFieldNames() )
                .executionTime( dmlEvent.getExecutionTime() )
                .rowCount( dmlEvent.getRowCount() )
                .isSubQuery( dmlEvent.isSubQuery() )
                .recordedTimestamp( dmlEvent.getRecordedTimestamp() )
                .accessedPartitions( dmlEvent.getAccessedPartitions().values().stream().flatMap( Set::stream ).collect( Collectors.toList() ) )
                .queryClass( dmlEvent.getLogicalQueryInformation().getQueryClass() )
                .monitoringType( "DML" )
                .physicalQueryClass( dmlEvent.getPhysicalQueryClass() )
                .build();
        metric.getTables().addAll( dmlEvent.getLogicalQueryInformation().getTables() );

        return metric;
    }

}


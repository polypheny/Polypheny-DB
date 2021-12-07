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
                .algCompareString( queryEvent.getAlgCompareString() )
                .accessedPartitions( queryEvent.getAccessedPartitions().values().stream().flatMap( Set::stream ).collect( Collectors.toList() ) )
                .queryClass( queryEvent.getLogicalQueryInformation().getQueryClass() )
                .monitoringType( "SELECT" )
                .physicalQueryClass( queryEvent.getPhysicalQueryClass() )
                .build();
        metric.getTables().addAll( queryEvent.getLogicalQueryInformation().getTables() );

        return metric;
    }

}

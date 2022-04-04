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

package org.polypheny.db.monitoring.workloadAnalysis;

import java.sql.Timestamp;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adaptiveness.WorkloadInformation;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.metrics.WorkloadDataPoint;
import org.polypheny.db.monitoring.repository.MonitoringRepository;
import org.polypheny.db.monitoring.workloadAnalysis.Shuttle.AlgNodeAnalyzeShuttle;


@Slf4j
@Getter
public class WorkloadRepository implements MonitoringRepository {


    @Override
    public void dataPoint( MonitoringDataPoint dataPoint ) {
        WorkloadManager workloadManager = WorkloadManager.getInstance();
        // Analyze logical query
        if ( ((WorkloadDataPoint) dataPoint).getAlgNode() != null && dataPoint.isCommitted() ) {
            // Shuttle to analyze processed nodes to figure
            AlgNodeAnalyzeShuttle analyzeRelShuttle = new AlgNodeAnalyzeShuttle();
            ((WorkloadDataPoint) dataPoint).getAlgNode().accept( analyzeRelShuttle );

            WorkloadInformation workloadInformation = new WorkloadInformationImpl(
                    ((WorkloadDataPoint) dataPoint).getExecutionTime(),
                    analyzeRelShuttle.getAggregateInformation(),
                    analyzeRelShuttle.getJoinInformation(),
                    analyzeRelShuttle.getTableScanInformation(),
                    analyzeRelShuttle.getProjectCount(),
                    analyzeRelShuttle.getSortCount(),
                    analyzeRelShuttle.getFilterCount() );

            // Check if self-adaptive is active, check if the queries is complex (long) enough, check that it is not a parameterized query
            if ( RuntimeConfig.SELF_ADAPTIVE.getBoolean() &&
                    ((WorkloadDataPoint) dataPoint).getAlgNode().algCompareString().length() > 150 &&
                    !analyzeRelShuttle.getRexShuttle().isUsesDynamicParam() &&
                    !analyzeRelShuttle.isModify() ) {
                workloadManager.findOftenUsedComplexQueries( ((WorkloadDataPoint) dataPoint).getAlgNode(), workloadInformation);
            }

            Timestamp timestamp = ((WorkloadDataPoint) dataPoint).getRecordedTimestamp();

            workloadManager.updateWorkloadTimeline(
                    timestamp, workloadInformation );

        }


    }

}

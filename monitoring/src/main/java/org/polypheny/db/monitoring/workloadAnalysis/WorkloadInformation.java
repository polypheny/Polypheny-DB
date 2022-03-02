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
import org.polypheny.db.monitoring.workloadAnalysis.InformationObjects.AggregateInformation;
import org.polypheny.db.monitoring.workloadAnalysis.InformationObjects.JoinInformation;
import org.polypheny.db.monitoring.workloadAnalysis.InformationObjects.TableScanInformation;
import org.polypheny.db.util.Pair;

@Getter
public class WorkloadInformation {

    private long executionTime;
    private AggregateInformation aggregateInformation;
    private JoinInformation joinInformation;
    private TableScanInformation tableScanInformation;
    private int projectCount;
    private int sortCount;
    private int filterCount;


    public WorkloadInformation(
            long executionTime,
            AggregateInformation aggregateInformation,
            JoinInformation joinInformation,
            TableScanInformation tableScanInformation,
            int projectCount,
            int sortCount,
            int filterCount ) {
        this.executionTime = executionTime;
        this.aggregateInformation = aggregateInformation;
        this.joinInformation = joinInformation;
        this.tableScanInformation = tableScanInformation;
        this.projectCount = projectCount;
        this.sortCount = sortCount;
        this.filterCount = filterCount;
    }


    public WorkloadInformation updateInfo(
            WorkloadInformation workloadInfo ) {
        this.aggregateInformation.updateAggregateInformation( workloadInfo.getAggregateInformation() );

        for ( Pair<Long, Long>tableId : workloadInfo.getJoinInformation().getJointTableIds()) {
            this.joinInformation.updateJoinInformation( tableId.left, tableId.right );
        }

        for ( Long tableId : workloadInfo.getTableScanInformation().getEntityIds()) {
            this.tableScanInformation.updateTableScanInfo( tableId );
        }
        this.projectCount += workloadInfo.getProjectCount();
        this.sortCount += workloadInfo.getSortCount();
        this.filterCount += workloadInfo.getFilterCount();
        this.executionTime = (this.executionTime + workloadInfo.executionTime)/2;

        return this;
    }

}

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
import org.polypheny.db.monitoring.workloadAnalysis.InformationObjects.AggregateInformation;
import org.polypheny.db.monitoring.workloadAnalysis.InformationObjects.JoinInformation;
import org.polypheny.db.monitoring.workloadAnalysis.InformationObjects.TableScanInformation;

public class WorkloadInformation {

    private final Timestamp timestamp;
    private final AggregateInformation aggregateInformation;
    private final JoinInformation joinInformation;
    private final TableScanInformation tableScanInformation;
    private final int projectCount;
    private final int sortCount;
    private final int filterCount;


    public WorkloadInformation(
            Timestamp timestamp,
            AggregateInformation aggregateInformation,
            JoinInformation joinInformation,
            TableScanInformation tableScanInformation,
            int projectCount,
            int sortCount,
            int filterCount ) {
        this.timestamp = timestamp;
        this.aggregateInformation = aggregateInformation;
        this.joinInformation = joinInformation;
        this.tableScanInformation = tableScanInformation;
        this.projectCount = projectCount;
        this.sortCount = sortCount;
        this.filterCount = filterCount;
    }

}

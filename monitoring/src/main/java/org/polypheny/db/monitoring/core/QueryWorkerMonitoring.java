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

package org.polypheny.db.monitoring.core;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.monitoring.dtos.MonitoringJob;
import org.polypheny.db.monitoring.persistence.WriteMonitoringRepository;
import org.polypheny.db.monitoring.dtos.QueryData;
import org.polypheny.db.monitoring.persistence.QueryPersistentData;
import org.polypheny.db.rel.RelNode;

@Slf4j
public class QueryWorkerMonitoring implements MonitoringQueueWorker<QueryData, QueryPersistentData> {
    private WriteMonitoringRepository repository;

    public QueryWorkerMonitoring(WriteMonitoringRepository repository) {
        if (repository == null)
            throw new IllegalArgumentException("repository is null");

        this.repository = repository;
    }

    @Override
    public void handleJob(MonitoringJob<QueryData, QueryPersistentData> job) {
        QueryData queryData = job.getEventData();
        QueryPersistentData dbEntity = QueryPersistentData
                .builder()
                .description(queryData.getDescription())
                .monitoringType(queryData.monitoringType)
                .Id(job.Id())
                .fieldNames(queryData.getFieldNames())
                .recordedTimestamp(queryData.getRecordedTimestamp())
                .build();

        job.setPersistentData(dbEntity);

        RelNode node = queryData.getRouted().rel;
        job = processRelNode(node, job);

        this.repository.writeEvent(job);
    }

    private MonitoringJob<QueryData, QueryPersistentData> processRelNode(RelNode node, MonitoringJob<QueryData, QueryPersistentData> currentJob) {

        for (int i = 0; i < node.getInputs().size(); i++) {
            processRelNode(node.getInput(i), currentJob);
        }
        // System.out.println(node);
        if (node.getTable() != null) {
            //System.out.println("FOUND TABLE : " + node.getTable());
            currentJob.getPersistentData().getTables().addAll(node.getTable().getQualifiedName());
        }

        currentJob.getPersistentData().getDataElements().put("val1", 5);
        currentJob.getPersistentData().getDataElements().put("val2", 8);
        currentJob.getPersistentData().getDataElements().put("val3", "test");

        return currentJob;
    }
}

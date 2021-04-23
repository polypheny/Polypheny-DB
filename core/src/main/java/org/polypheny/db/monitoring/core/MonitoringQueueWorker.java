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

import org.polypheny.db.monitoring.dtos.MonitoringData;
import org.polypheny.db.monitoring.dtos.MonitoringJob;
import org.polypheny.db.monitoring.dtos.MonitoringPersistentData;

/**
 * MonitoringQueueWorker is responsible to handle certain type of MonitoringJobs with type <TEvent>
 * and <TPersistent>. Core idea is that the worker will inject the <MonitoringRepository> and will persist the data.
 * But all in all, the worker has the flexibility to decide what will happen with the MonitoringJobs.
 *
 * @param <TEvent> Worker input type, which will be processed to TPersistent and may get stored based on defined repository.
 * @param <TPersistent> Transformed TEvent which might  be persisted in repository and can later be queried.
 */
public interface MonitoringQueueWorker<TEvent extends MonitoringData, TPersistent extends MonitoringPersistentData> {

    /**
     * @param job worker handle the given job.
     */
    MonitoringJob<TEvent, TPersistent> handleJob( MonitoringJob<TEvent, TPersistent> job );

}

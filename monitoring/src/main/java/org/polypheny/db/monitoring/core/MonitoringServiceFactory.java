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

package org.polypheny.db.monitoring.core;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.monitoring.persistence.MapDbRepository;
import org.polypheny.db.monitoring.statistics.StatisticRepository;


@Slf4j
public class MonitoringServiceFactory {

    public static MonitoringServiceImpl createMonitoringService( boolean resetRepository ) {
        // Create mapDB repository
        MapDbRepository persistentRepo = new MapDbRepository();
        StatisticRepository statisticRepo = new StatisticRepository();

        // Initialize the mapDB repo and open connection
        persistentRepo.initialize( resetRepository );

        // Create monitoring service with dependencies
        MonitoringQueue queueWriteService = new MonitoringQueueImpl( persistentRepo, statisticRepo );

        // Initialize the monitoringService
        MonitoringServiceImpl monitoringService = new MonitoringServiceImpl( queueWriteService, persistentRepo );

        return monitoringService;
    }

}

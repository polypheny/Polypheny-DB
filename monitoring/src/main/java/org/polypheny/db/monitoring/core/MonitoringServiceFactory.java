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
import org.polypheny.db.monitoring.dtos.QueryData;
import org.polypheny.db.monitoring.persistence.MapDbRepository;
import org.polypheny.db.monitoring.persistence.QueryPersistentData;
import org.polypheny.db.monitoring.ui.MonitoringServiceUi;
import org.polypheny.db.monitoring.ui.MonitoringServiceUiImpl;

@Slf4j
public class MonitoringServiceFactory {

    public static MonitoringServiceImpl CreateMonitoringService() {

        // create mapDB repository
        MapDbRepository repo = new MapDbRepository();
        // initialize the mapDB repo and open connection
        repo.initialize();

        // create monitoring service with dependencies
        MonitoringQueue writeService = new MonitoringQueueImpl();
        MonitoringServiceUi uiService = new MonitoringServiceUiImpl( repo );

        // initialize ui
        uiService.initializeInformationPage();

        // initialize the monitoringService
        MonitoringServiceImpl monitoringService = new MonitoringServiceImpl( writeService, repo, uiService );

        // configure query monitoring event as system wide monitoring
        MonitoringQueueWorker worker = new QueryWorker( repo );
        monitoringService.registerEventType( QueryData.class, QueryPersistentData.class, worker );

        return monitoringService;
    }

}

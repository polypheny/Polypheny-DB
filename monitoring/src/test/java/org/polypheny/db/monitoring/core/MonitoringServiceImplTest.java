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
import org.junit.Test;
import org.polypheny.db.monitoring.dtos.QueryData;
import org.polypheny.db.monitoring.persistence.QueryPersistentData;
import org.polypheny.db.monitoring.persistent.ReadOnlyMonitoringRepository;
import org.polypheny.db.monitoring.persistent.MonitoringRepository;
import org.polypheny.db.monitoring.ui.MonitoringServiceUi;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

@Slf4j
public class MonitoringServiceImplTest {

    @Test
    public void TestIt() {
        MonitoringQueue doc1 = mock(MonitoringQueue.class);
        ReadOnlyMonitoringRepository doc2 = mock(ReadOnlyMonitoringRepository.class);
        MonitoringServiceUi doc3 = mock(MonitoringServiceUi.class);

        MonitoringRepository doc4 = mock( MonitoringRepository.class );


        MonitoringQueue writeQueueService = new MonitoringQueueImpl();


        MonitoringService sut = new MonitoringServiceImpl(writeQueueService, doc2, doc3);
        QueryData eventData = mock(QueryData.class);
        sut.registerEventType(QueryData.class, QueryPersistentData.class);

        sut.monitorEvent(eventData);

        assertNotNull(sut);

    }


}
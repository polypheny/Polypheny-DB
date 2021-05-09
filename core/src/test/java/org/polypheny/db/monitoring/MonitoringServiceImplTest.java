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

package org.polypheny.db.monitoring;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.mockito.Mockito;
import org.polypheny.db.monitoring.core.MonitoringQueue;
import org.polypheny.db.monitoring.core.MonitoringQueueImpl;
import org.polypheny.db.monitoring.core.MonitoringService;
import org.polypheny.db.monitoring.core.MonitoringServiceImpl;
import org.polypheny.db.monitoring.events.QueryEvent;
import org.polypheny.db.monitoring.persistence.MonitoringRepository;
import org.polypheny.db.monitoring.ui.MonitoringServiceUi;

@Slf4j
public class MonitoringServiceImplTest {

    @Test
    public void TestIt() {
        MonitoringQueue doc1 = Mockito.mock( MonitoringQueue.class );
        MonitoringRepository doc2 = Mockito.mock( MonitoringRepository.class );
        MonitoringServiceUi doc3 = Mockito.mock( MonitoringServiceUi.class );

        MonitoringRepository doc4 = Mockito.mock( MonitoringRepository.class );

        MonitoringQueue writeQueueService = new MonitoringQueueImpl( doc2 );

        MonitoringService sut = new MonitoringServiceImpl( writeQueueService, doc2, doc3 );
        QueryEvent eventData = Mockito.mock( QueryEvent.class );

        sut.monitorEvent( eventData );

        assertNotNull( sut );

    }


}
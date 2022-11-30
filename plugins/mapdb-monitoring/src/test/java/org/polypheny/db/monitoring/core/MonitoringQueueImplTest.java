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

import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.polypheny.db.monitoring.events.MonitoringEvent;
import org.polypheny.db.monitoring.events.QueryEvent;
import org.polypheny.db.monitoring.repository.MonitoringRepository;
import org.polypheny.db.monitoring.repository.PersistentMonitoringRepository;


class MonitoringQueueImplTest {

    @Test
    public void ctor_validParameters_instanceNotNull() {
        // arrange
        PersistentMonitoringRepository persistentRepo = Mockito.mock( PersistentMonitoringRepository.class );
        MonitoringRepository statisticRepo = Mockito.mock( MonitoringRepository.class );

        // act
        MonitoringQueue sut = new MonitoringQueueImpl( false, persistentRepo, statisticRepo );

        // assert
        Assertions.assertNotNull( sut );
    }


    @Test
    public void queueEvent_validEvent_QueueConsistsElements() {
        // arrange
        PersistentMonitoringRepository persistentRepo = Mockito.mock( PersistentMonitoringRepository.class );
        MonitoringRepository statisticRepo = Mockito.mock( MonitoringRepository.class );
        MonitoringQueue sut = new MonitoringQueueImpl( false, persistentRepo, statisticRepo );
        MonitoringEvent event = Mockito.mock( MonitoringEvent.class );

        // act
        sut.queueEvent( event );

        // assert
        long elementsInQueue = sut.getNumberOfElementsInQueue();
        Assertions.assertEquals( 1L, elementsInQueue );
    }


    @Test
    public void queueEvent_validEvent2Times_QueueConsistsElementOnce() {
        // arrange
        PersistentMonitoringRepository persistentRepo = Mockito.mock( PersistentMonitoringRepository.class );
        MonitoringRepository statisticRepo = Mockito.mock( MonitoringRepository.class );
        MonitoringQueue sut = new MonitoringQueueImpl( false, persistentRepo, statisticRepo );
        QueryEvent event = new QueryEvent();

        // act
        sut.queueEvent( event );
        sut.queueEvent( event );

        // assert
        long elementsInQueue = sut.getNumberOfElementsInQueue();
        Assertions.assertEquals( 1L, elementsInQueue );
    }


    @Test
    public void queueEvent_validEvents_QueueConsistsElements() {
        // arrange
        PersistentMonitoringRepository persistentRepo = Mockito.mock( PersistentMonitoringRepository.class );
        MonitoringRepository statisticRepo = Mockito.mock( MonitoringRepository.class );
        MonitoringQueue sut = new MonitoringQueueImpl( false, persistentRepo, statisticRepo );
        long numberOfEvents = 100L;

        // act
        for ( int i = 0; i < numberOfEvents; i++ ) {
            QueryEvent event = new QueryEvent();
            sut.queueEvent( event );
        }

        // assert
        long elementsInQueue = sut.getNumberOfElementsInQueue();
        Assertions.assertEquals( numberOfEvents, elementsInQueue );

        List<HashMap<String, String>> infoStrings = sut.getInformationOnElementsInQueue();
        Assertions.assertEquals( (int) numberOfEvents, infoStrings.size() );

        HashMap<String, String> infoString = infoStrings.get( 0 );
        Assertions.assertSame( 3, infoString.size() );
        Assertions.assertEquals( QueryEvent.class.toString(), infoString.get( "type" ) );
    }

}

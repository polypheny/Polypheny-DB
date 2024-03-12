/*
 * Copyright 2019-2024 The Polypheny Project
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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Timestamp;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.polypheny.db.monitoring.events.MonitoringEvent;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;
import org.polypheny.db.monitoring.repository.PersistentMonitoringRepository;


class MonitoringServiceImplTest {

    @Test
    public void ctor_invalidParameters_ThrowsException() {
        // arrange
        MonitoringQueue monitoringQueue = Mockito.mock( MonitoringQueue.class );
        PersistentMonitoringRepository repository = Mockito.mock( PersistentMonitoringRepository.class );

        // act - assert
        assertThrows( NullPointerException.class, () -> new MonitoringServiceImpl( null, repository ) );
        assertThrows( NullPointerException.class, () -> new MonitoringServiceImpl( monitoringQueue, null ) );
    }


    @Test
    void ctor_validParameters_instanceNotNull() {
        // arrange
        MonitoringQueue monitoringQueue = Mockito.mock( MonitoringQueue.class );
        PersistentMonitoringRepository repository = Mockito.mock( PersistentMonitoringRepository.class );

        // act
        MonitoringService sut = new MonitoringServiceImpl( monitoringQueue, repository );

        // assert
        assertNotNull( sut );
    }


    @Test
    void monitorEvent_provideNullEvent_throwsException() {
        // arrange
        MonitoringQueue monitoringQueue = Mockito.mock( MonitoringQueue.class );
        PersistentMonitoringRepository repository = Mockito.mock( PersistentMonitoringRepository.class );
        MonitoringService sut = new MonitoringServiceImpl( monitoringQueue, repository );

        // act - assert
        assertThrows( NullPointerException.class, () -> sut.monitorEvent( null ) );
    }


    @Test
    void monitorEvent_provideEvent_queueCalled() {
        // arrange
        MonitoringQueue monitoringQueue = Mockito.mock( MonitoringQueue.class );
        PersistentMonitoringRepository repository = Mockito.mock( PersistentMonitoringRepository.class );
        MonitoringEvent event = Mockito.mock( MonitoringEvent.class );
        MonitoringService sut = new MonitoringServiceImpl( monitoringQueue, repository );

        // act
        sut.monitorEvent( event );

        // assert
        Mockito.verify( monitoringQueue, Mockito.times( 1 ) ).queueEvent( event );
    }


    @Test
    void getAllDataPoints_providePointClass_repositoryCalled() {
        // arrange
        MonitoringQueue monitoringQueue = Mockito.mock( MonitoringQueue.class );
        PersistentMonitoringRepository repository = Mockito.mock( PersistentMonitoringRepository.class );
        MonitoringService sut = new MonitoringServiceImpl( monitoringQueue, repository );

        // act
        sut.getAllDataPoints( QueryDataPointImpl.class );

        // assert
        Mockito.verify( repository, Mockito.times( 1 ) ).getAllDataPoints( QueryDataPointImpl.class );
    }


    @Test
    void getDataPointsBefore_providePointClass_repositoryCalled() {
        // arrange
        MonitoringQueue monitoringQueue = Mockito.mock( MonitoringQueue.class );
        PersistentMonitoringRepository repository = Mockito.mock( PersistentMonitoringRepository.class );
        MonitoringService sut = new MonitoringServiceImpl( monitoringQueue, repository );

        // act
        Timestamp time = new Timestamp( System.currentTimeMillis() );
        sut.getDataPointsBefore( QueryDataPointImpl.class, time );

        // assert
        Mockito.verify( repository, Mockito.times( 1 ) ).getDataPointsBefore( QueryDataPointImpl.class, time );
    }


    @Test
    void getDataPointsAfter_providePointClass_repositoryCalled() {
        // arrange
        MonitoringQueue monitoringQueue = Mockito.mock( MonitoringQueue.class );
        PersistentMonitoringRepository repository = Mockito.mock( PersistentMonitoringRepository.class );
        MonitoringService sut = new MonitoringServiceImpl( monitoringQueue, repository );

        // act
        Timestamp time = new Timestamp( System.currentTimeMillis() );
        sut.getDataPointsAfter( QueryDataPointImpl.class, time );

        // assert
        Mockito.verify( repository, Mockito.times( 1 ) ).getDataPointsAfter( QueryDataPointImpl.class, time );
    }

}

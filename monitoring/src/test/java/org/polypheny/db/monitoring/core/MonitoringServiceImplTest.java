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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;

import java.sql.Timestamp;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.polypheny.db.monitoring.events.MonitoringEvent;
import org.polypheny.db.monitoring.events.metrics.QueryDataPoint;
import org.polypheny.db.monitoring.persistence.MonitoringRepository;
import org.polypheny.db.monitoring.ui.MonitoringServiceUi;

class MonitoringServiceImplTest {

    @Test
    public void ctor_invalidParameters_ThrowsException() {
        // arrange
        val monitoringQueue = Mockito.mock( MonitoringQueue.class );
        val repository = Mockito.mock( MonitoringRepository.class );
        val monitoringServiceUi = Mockito.mock( MonitoringServiceUi.class );

        // act - assert
        assertThrows( NullPointerException.class, () -> new MonitoringServiceImpl( null, repository, monitoringServiceUi ) );
        assertThrows( NullPointerException.class, () -> new MonitoringServiceImpl( monitoringQueue, null, monitoringServiceUi ) );
        assertThrows( NullPointerException.class, () -> new MonitoringServiceImpl( monitoringQueue, repository, null ) );
    }


    @Test
    void ctor_validParameters_instanceNotNull() {
        // arrange
        val monitoringQueue = Mockito.mock( MonitoringQueue.class );
        val repository = Mockito.mock( MonitoringRepository.class );
        val monitoringServiceUi = Mockito.mock( MonitoringServiceUi.class );

        // act
        val sut = new MonitoringServiceImpl( monitoringQueue, repository, monitoringServiceUi );

        // assert
        Assertions.assertNotNull( sut );
    }


    @Test
    void monitorEvent_provideNullEvent_throwsException() {
        // arrange
        val monitoringQueue = Mockito.mock( MonitoringQueue.class );
        val repository = Mockito.mock( MonitoringRepository.class );
        val monitoringServiceUi = Mockito.mock( MonitoringServiceUi.class );
        val sut = new MonitoringServiceImpl( monitoringQueue, repository, monitoringServiceUi );

        // act - assert
        assertThrows( NullPointerException.class, () -> sut.monitorEvent( null ) );
    }


    @Test
    void monitorEvent_provideEvent_queueCalled() {
        // arrange
        val monitoringQueue = Mockito.mock( MonitoringQueue.class );
        val repository = Mockito.mock( MonitoringRepository.class );
        val monitoringServiceUi = Mockito.mock( MonitoringServiceUi.class );
        val event = Mockito.mock( MonitoringEvent.class );
        val sut = new MonitoringServiceImpl( monitoringQueue, repository, monitoringServiceUi );

        // act
        sut.monitorEvent( event );

        // assert
        Mockito.verify( monitoringQueue, times( 1 ) ).queueEvent( event );
    }


    @Test
    void getAllDataPoints_providePointClass_repositoryCalled() {
        // arrange
        val monitoringQueue = Mockito.mock( MonitoringQueue.class );
        val repository = Mockito.mock( MonitoringRepository.class );
        val monitoringServiceUi = Mockito.mock( MonitoringServiceUi.class );
        val sut = new MonitoringServiceImpl( monitoringQueue, repository, monitoringServiceUi );

        // act
        sut.getAllDataPoints( QueryDataPoint.class );

        // assert
        Mockito.verify( repository, times( 1 ) ).getAllDataPoints( QueryDataPoint.class );
    }


    @Test
    void getDataPointsBefore_providePointClass_repositoryCalled() {
        // arrange
        val monitoringQueue = Mockito.mock( MonitoringQueue.class );
        val repository = Mockito.mock( MonitoringRepository.class );
        val monitoringServiceUi = Mockito.mock( MonitoringServiceUi.class );
        val sut = new MonitoringServiceImpl( monitoringQueue, repository, monitoringServiceUi );

        // act
        val time = new Timestamp( System.currentTimeMillis() );
        sut.getDataPointsBefore( QueryDataPoint.class, time );

        // assert
        Mockito.verify( repository, times( 1 ) ).getDataPointsBefore( QueryDataPoint.class, time );
    }


    @Test
    void getDataPointsAfter_providePointClass_repositoryCalled() {
        // arrange
        val monitoringQueue = Mockito.mock( MonitoringQueue.class );
        val repository = Mockito.mock( MonitoringRepository.class );
        val monitoringServiceUi = Mockito.mock( MonitoringServiceUi.class );
        val sut = new MonitoringServiceImpl( monitoringQueue, repository, monitoringServiceUi );

        // act
        val time = new Timestamp( System.currentTimeMillis() );
        sut.getDataPointsAfter( QueryDataPoint.class, time );

        // assert
        Mockito.verify( repository, times( 1 ) ).getDataPointsAfter( QueryDataPoint.class, time );
    }

}
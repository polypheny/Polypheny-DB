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


import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.monitoring.events.QueryEvent;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;
import org.polypheny.db.monitoring.ui.MonitoringServiceUi;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;


class MonitoringQueueImplIntegrationTest {

    @Test
    public void queuedEventsAreProcessed() {
        //  -- Arrange --
        // Set background task timer
        RuntimeConfig.QUEUE_PROCESSING_INTERVAL.setEnum( TaskSchedulingType.EVERY_SECOND );

        // Initialize mock repository
        TestMapDbRepository repo = new TestMapDbRepository();
        repo.initialize( true ); // will delete the file

        // Mock ui service, not really needed for testing
        MonitoringServiceUi uiService = Mockito.mock( MonitoringServiceUi.class );

        // Create monitoring service with dependencies
        MonitoringQueueImpl queueWriteService = new MonitoringQueueImpl( repo );

        // Initialize the monitoringService
        MonitoringService sut = new MonitoringServiceImpl( queueWriteService, repo, uiService );

        Assertions.assertNotNull( sut );

        // -- Act --
        List<QueryEvent> events = createQueryEvent( 15 );
        events.forEach( sut::monitorEvent );

        try {
            Thread.sleep( 2000L );
        } catch ( InterruptedException e ) {
            e.printStackTrace();
        }

        // -- Assert --
        List<QueryDataPointImpl> result = sut.getAllDataPoints( QueryDataPointImpl.class );
        Assertions.assertEquals( 15, result.size() );
    }


    private List<QueryEvent> createQueryEvent( int number ) {
        List<QueryEvent> result = new ArrayList<>();

        for ( int i = 0; i < number; i++ ) {
            QueryEvent event = new QueryEvent();
            event.setRouted( null );
            event.setResult( null );
            event.setStatement( Mockito.mock( Statement.class ) );
            event.setDescription( UUID.randomUUID().toString() );
            event.setExecutionTime( (long) (Math.random() * 1000L) );
            event.setFieldNames( Lists.newArrayList( "T1", "T2", "T3" ) );
            event.setRowCount( 15 );
            event.setAnalyze( true );
            event.setSubQuery( false );

            result.add( event );
        }

        return result;
    }

}

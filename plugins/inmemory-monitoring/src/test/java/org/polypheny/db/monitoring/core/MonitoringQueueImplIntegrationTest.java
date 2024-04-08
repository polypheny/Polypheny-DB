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


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.polypheny.db.TestHelper;
import org.polypheny.db.monitoring.events.QueryEvent;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.transaction.Statement;


@Slf4j
class MonitoringQueueImplIntegrationTest {

    @BeforeAll
    public static void setUp() {
        TestHelper.getInstance();
    }


    @Test
    public void queuedEventsAreProcessed() throws InterruptedException {
        //  -- Arrange --

        // Initialize mock repository
        TestInMemoryRepository persistentRepo = new TestInMemoryRepository();
        TestInMemoryRepository statisticRepo = new TestInMemoryRepository();
        persistentRepo.initialize( true ); // will delete the file

        // Create monitoring service with dependencies
        MonitoringQueueImpl queueWriteService = new MonitoringQueueImpl( persistentRepo, statisticRepo );

        // Initialize the monitoringService
        MonitoringService sut = new MonitoringServiceImpl( queueWriteService, persistentRepo );

        assertNotNull( sut );

        // -- Act --
        List<QueryEvent> events = createQueryEvent( 10 );
        events.forEach( sut::monitorEvent );

        Thread.sleep( 10000L );

        for ( int i = 0; i < 8; i++ ) {
            if ( statisticRepo.count.get() != 10 ) {
                Thread.sleep( 8000L );
            }
        }

        // -- Assert --

        assertEquals( 10, statisticRepo.count.get() );
        assertEquals( 10, persistentRepo.count.get() );
    }


    private List<QueryEvent> createQueryEvent( int number ) {
        List<QueryEvent> result = new ArrayList<>();

        LogicalQueryInformation mockedQueryInformation = Mockito.mock( LogicalQueryInformation.class );
        Mockito.when( mockedQueryInformation.getAllScannedEntities() ).thenReturn( ImmutableSet.of( 0L ) );

        for ( int i = 0; i < number; i++ ) {
            QueryEvent event = new QueryEvent();
            event.setRouted( null );
            event.setResult( null );
            event.setStatement( Mockito.mock( Statement.class ) );
            event.setLogicalQueryInformation( mockedQueryInformation );
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

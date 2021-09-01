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
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.monitoring.events.QueryEvent;
import org.polypheny.db.monitoring.events.metrics.QueryDataPoint;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.transaction.Statement;

class MonitoringQueueImplIntegrationTest {

    @Test
    public void monitoringImplWithBackgroundTask() {
        val monitoringService = MonitoringServiceProvider.getInstance();
        Assertions.assertNotNull( monitoringService );

        //RuntimeConfig.QUEUE_PROCESSING_INTERVAL = TaskSchedulingType.EVERY_SECOND.getMillis() ;

        val events = createQueryEvent( 15 );
        events.forEach( event -> monitoringService.monitorEvent( event ) );

        try {
            Thread.sleep( 5000L );
        } catch ( InterruptedException e ) {
            e.printStackTrace();
        }

        val result = monitoringService.getAllDataPoints( QueryDataPoint.class );

    }


    private List<QueryEvent> createQueryEvent( int number ) {
        val result = new ArrayList<QueryEvent>();

        for ( int i = 0; i < number; i++ ) {
            val event = new QueryEvent();
            event.setRouted( Mockito.mock( RelRoot.class ) );
            event.setSignature( Mockito.mock( PolyphenyDbSignature.class ) );
            event.setStatement( Mockito.mock( Statement.class ) );
            event.setDescription( UUID.randomUUID().toString() );
            event.setExecutionTime( (long) Math.random() * 1000L );
            event.setFieldNames( Lists.newArrayList( "T1", "T2", "T3" ) );
            event.setRowCount( 15 );
            event.setAnalyze( true );
            event.setSubQuery( false );

            result.add( event );
        }

        return result;
    }

}
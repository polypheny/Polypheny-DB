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

package org.polypheny.db.monitoring.ui;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.monitoring.core.MonitoringQueue;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.metrics.DmlDataPoint;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;
import org.polypheny.db.monitoring.persistence.MonitoringRepository;


@Slf4j
public class MonitoringServiceUiImpl implements MonitoringServiceUi {

    private final MonitoringRepository repo;
    private final MonitoringQueue queue;
    private InformationPage informationPage;


    public MonitoringServiceUiImpl( @NonNull MonitoringRepository repo, @NonNull MonitoringQueue queue ) {
        this.repo = repo;
        this.queue = queue;

        initializeInformationPage();
    }


    @Override
    public void initializeInformationPage() {
        // Initialize Information Page
        informationPage = new InformationPage( "Workload Monitoring" );
        informationPage.fullWidth();
        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );

        initializeWorkloadInformationTable();
        //initializeQueueInformationTable();
    }

    /**
     * Universal method to add arbitrary new information Groups to UI.
     */
    private void addInformationGroupTUi( @NonNull InformationGroup informationGroup, @NonNull List<InformationTable> informationTables ) {
        InformationManager im = InformationManager.getInstance();
        im.addGroup( informationGroup );

        for ( InformationTable informationTable : informationTables ) {
            im.registerInformation( informationTable );
        }
    }

    private void initializeWorkloadInformationTable() {
        val informationGroup = new InformationGroup( informationPage, "Workload Overview" );
        val informationTable = new InformationTable( informationGroup,
                Arrays.asList( "Attribute", "Value" ) );
        informationGroup.setOrder( 1 );

        informationGroup.setRefreshFunction( () -> this.updateWorkloadInformationTable( informationTable ) );

        addInformationGroupTUi( informationGroup, Arrays.asList( informationTable ) );
    }


    private void initializeQueueInformationTable() {
        //On first subscriber also add
        //Also build active subscription table Metric to subscribers
        //or which subscribers, exist and to which metrics they are subscribed

        val informationGroup = new InformationGroup( informationPage, "Monitoring Queue" ).setOrder( 2 );
        val informationTable = new InformationTable( informationGroup,
                Arrays.asList( "Event Type", "UUID", "Timestamp" ) );

        informationGroup.setRefreshFunction( () -> this.updateQueueInformationTable( informationTable ) );

        addInformationGroupTUi( informationGroup, Arrays.asList( informationTable ) );
    }


    private void updateQueueInformationTable( InformationTable table ) {
        List<HashMap<String, String>> queueInfoElements = this.queue.getInformationOnElementsInQueue();
        table.reset();

        for ( HashMap<String, String> infoRow : queueInfoElements ) {
            List<String> row = new ArrayList<>();
            row.add( infoRow.get( "type" ) );
            row.add( infoRow.get( "id" ) );
            row.add( infoRow.get( "timestamp" ) );

            table.addRow( row );
        }
    }


    private void updateWorkloadInformationTable( InformationTable table ) {
        table.reset();

        table.addRow( "Number of processed events since restart", queue.getNumberOfProcessedEvents() );
        table.addRow( "Number of events in queue", queue.getNumberOfElementsInQueue());
        //table.addRow( "# Data Points", queue.getElementsInQueue().size() );
        table.addRow( "# SELECT", MonitoringServiceProvider.getInstance().getAllDataPoints( QueryDataPointImpl.class ).size() );
        table.addRow( "# DML", MonitoringServiceProvider.getInstance().getAllDataPoints( DmlDataPoint.class ).size() );
    }

}

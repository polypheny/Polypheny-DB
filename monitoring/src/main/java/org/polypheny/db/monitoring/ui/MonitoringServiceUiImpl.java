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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.monitoring.core.MonitoringQueue;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.metrics.DmlDataPoint;
import org.polypheny.db.monitoring.events.metrics.QueryDataPoint;
import org.polypheny.db.monitoring.persistence.MonitoringRepository;


@Slf4j
public class MonitoringServiceUiImpl implements MonitoringServiceUi {

    private final MonitoringRepository repo;
    private final MonitoringQueue queue;
    private InformationPage informationPage;


    // TODO: die Abhänigkeit zur Queue ist nicht wirklich optimal, aber lassen wir vielleicht mal so stehen
    public MonitoringServiceUiImpl( @NonNull MonitoringRepository repo, @NonNull MonitoringQueue queue ) {
        this.repo = repo;
        this.queue = queue;

        initializeInformationPage();
    }


    @Override
    public void initializeInformationPage() {
        //Initialize Information Page
        informationPage = new InformationPage( "Workload Monitoring" );
        informationPage.fullWidth();
        InformationManager im = InformationManager.getInstance();
        im.addPage( informationPage );

        initializeWorkloadInformationTable();
        initializeQueueInformationTable();
    }


    @Override
    public <T extends MonitoringDataPoint> void registerDataPointForUi( @NonNull Class<T> metricClass ) {
        String className = metricClass.getName();
        val informationGroup = new InformationGroup( informationPage, className );

        // TODO: see todo below
        val fieldAsString = Arrays.stream( metricClass.getDeclaredFields() )
                .map( Field::getName )
                .filter( str -> !str.equals( "serialVersionUID" ) )
                .collect( Collectors.toList() );
        val informationTable = new InformationTable( informationGroup, fieldAsString );

        informationGroup.setRefreshFunction( () -> this.updateMetricInformationTable( informationTable, metricClass ) );

        addInformationGroupTUi( informationGroup, Arrays.asList( informationTable ) );
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


    private <T extends MonitoringDataPoint> void updateMetricInformationTable( InformationTable table, Class<T> metricClass ) {
        List<T> elements = this.repo.getAllDataPoints( metricClass );
        table.reset();

        Field[] fields = metricClass.getDeclaredFields();
        Method[] methods = metricClass.getMethods();
        for ( T element : elements ) {
            List<String> row = new LinkedList<>();

            for ( Field field : fields ) {
                // TODO: get declared fields and fine corresponding Lombok getter to execute
                // Therefore, nothing need to be done for serialVersionID
                // and neither do we need to hacky set the setAccessible flag for the fields
                if ( field.getName().equals( "serialVersionUID" ) ) {
                    continue;
                }

                try {
                    field.setAccessible( true );
                    val value = field.get( element );
                    row.add( value.toString() );
                } catch ( IllegalAccessException e ) {
                    e.printStackTrace();
                }
            }

            table.addRow( row );
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
        table.addRow( "Number of events in queue", queue.getNumberOfElementsInQueue() );
        //table.addRow( "# Data Points", queue.getElementsInQueue().size() );
        table.addRow( "# SELECT", MonitoringServiceProvider.getInstance().getAllDataPoints( QueryDataPoint.class ).size() );
        table.addRow( "# DML", MonitoringServiceProvider.getInstance().getAllDataPoints( DmlDataPoint.class ).size() );
    }

}

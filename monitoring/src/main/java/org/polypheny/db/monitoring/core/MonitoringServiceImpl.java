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

import java.sql.Timestamp;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.MonitoringEvent;
import org.polypheny.db.monitoring.events.QueryPostCost;
import org.polypheny.db.monitoring.repository.PersistentMonitoringRepository;


@Slf4j
public class MonitoringServiceImpl implements MonitoringService {

    private final MonitoringQueue monitoringQueue;
    private final PersistentMonitoringRepository repository;


    public MonitoringServiceImpl(
            @NonNull MonitoringQueue monitoringQueue,
            @NonNull PersistentMonitoringRepository repository ) {

        this.monitoringQueue = monitoringQueue;
        this.repository = repository;
    }


    @Override
    public void monitorEvent( @NonNull MonitoringEvent eventData ) {
        if ( RuntimeConfig.MONITORING_QUEUE_ACTIVE.getBoolean() ) {
            this.monitoringQueue.queueEvent( eventData );
        }
    }


    @Override
    public <T extends MonitoringDataPoint> List<T> getAllDataPoints( @NonNull Class<T> dataPointClass ) {
        return this.repository.getAllDataPoints( dataPointClass );
    }


    @Override
    public <T extends MonitoringDataPoint> long getNumberOfDataPoints( @NonNull Class<T> dataPointClass ) {
        return this.repository.getNumberOfDataPoints( dataPointClass );
    }


    @Override
    public <T extends MonitoringDataPoint> List<T> getDataPointsBefore( @NonNull Class<T> dataPointClass, @NonNull Timestamp timestamp ) {
        return this.repository.getDataPointsBefore( dataPointClass, timestamp );
    }


    @Override
    public <T extends MonitoringDataPoint> List<T> getDataPointsAfter( @NonNull Class<T> dataPointClass, @NonNull Timestamp timestamp ) {
        return this.repository.getDataPointsAfter( dataPointClass, timestamp );
    }


    @Override
    public long getNumberOfElementsInQueue() {
        return monitoringQueue.getNumberOfElementsInQueue();
    }


    /**
     * Removes all data points for given monitoring persistent type.
     *
     * @param dataPointClass specific datapoint class of interest to remove
     */
    @Override
    public <T extends MonitoringDataPoint> void removeAllDataPointsOfSpecificClass( Class<T> dataPointClass ) {
        this.repository.removeAllDataPointsOfSpecificClass( dataPointClass );
    }


    /**
     * Removes all aggregated dataPoints.
     */
    @Override
    public void resetAllDataPoints() {
        this.repository.resetAllDataPoints();
    }


    @Override
    public QueryPostCost getQueryPostCosts( @NotNull String physicalQueryClass ) {
        return this.repository.getQueryPostCosts( physicalQueryClass );
    }


    @Override
    public List<QueryPostCost> getAllQueryPostCosts() {
        return this.repository.getAllQueryPostCosts();
    }


    @Override
    public void updateQueryPostCosts( @NonNull String physicalQueryClass, long executionTime ) {
        this.repository.updateQueryPostCosts( physicalQueryClass, executionTime );
    }


    @Override
    public void resetQueryPostCosts() {
        this.repository.resetQueryPostCosts();
    }


    @Override
    public long getSize() {
        return monitoringQueue.getNumberOfElementsInQueue();
    }


}

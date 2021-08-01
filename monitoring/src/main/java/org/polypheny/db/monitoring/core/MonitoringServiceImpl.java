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

import java.sql.Timestamp;
import java.util.List;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.MonitoringEvent;
import org.polypheny.db.monitoring.events.QueryDataPoint;
import org.polypheny.db.monitoring.persistence.MonitoringRepository;
import org.polypheny.db.monitoring.ui.MonitoringServiceUi;

@Slf4j
public class MonitoringServiceImpl implements MonitoringService {

    // region private fields

    private final MonitoringQueue monitoringQueue;
    private final MonitoringRepository repository;
    private final MonitoringServiceUi monitoringServiceUi;

    // endregion

    // region ctors


    public MonitoringServiceImpl(
            @NonNull MonitoringQueue monitoringQueue,
            @NonNull MonitoringRepository repository,
            @NonNull MonitoringServiceUi monitoringServiceUi ) {

        this.monitoringQueue = monitoringQueue;
        this.repository = repository;
        this.monitoringServiceUi = monitoringServiceUi;
    }

    // endregion

    // region public methods


    @Override
    public void monitorEvent( @NonNull MonitoringEvent eventData ) {
        this.monitoringQueue.queueEvent( eventData );
    }


    @Override
    public <T extends MonitoringDataPoint> List<T> getAllDataPoints( @NonNull Class<T> dataPointClass ) {
        return this.repository.getAllDataPoints( dataPointClass );
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
    public List<QueryDataPoint> getQueryDataPoints( String queryClassString ) {
        return this.repository.getQueryDataPoints( queryClassString );
    }


    // endregion
}

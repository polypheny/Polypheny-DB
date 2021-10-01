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

package org.polypheny.db.monitoring.persistence;

import java.sql.Timestamp;
import java.util.List;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;


/**
 * Interface for writing monitoring jobs to repository.
 */
public interface MonitoringRepository {

    /**
     * Initialized the repository, might need some configuration beforehand.
     */
    void initialize();

    /**
     * Persist given monitoring metric.
     *
     * @param dataPoint to be persisted in repository backend
     */
    void persistDataPoint( MonitoringDataPoint dataPoint );

    /**
     * Get all data for given monitoring persistent type.
     *
     * @param dataPointClass DatapointClass of interest
     * @return Returns List of all datapoints from teh specified dataPointClass
     */
    <T extends MonitoringDataPoint> List<T> getAllDataPoints( Class<T> dataPointClass );

    /**
     * Get data before specified timestamp for given monitoring persistent type.
     *
     * @param dataPointClass datapointclass of interest to look for
     * @param timestamp youngest timestamp t return datapoints from
     * @return Returns List of all datapoints from the specified dataPointClass
     */
    <T extends MonitoringDataPoint> List<T> getDataPointsBefore( Class<T> dataPointClass, Timestamp timestamp );

    /**
     * Get data after specified timestamp for given monitoring persistent type.
     *
     * @param dataPointClass datapointclass of interest to look for
     * @param timestamp oldest timestamp t return datapoints from
     * @return Returns List of all datapoints from teh specified dataPointClass
     */
    <T extends MonitoringDataPoint> List<T> getDataPointsAfter( Class<T> dataPointClass, Timestamp timestamp );

}

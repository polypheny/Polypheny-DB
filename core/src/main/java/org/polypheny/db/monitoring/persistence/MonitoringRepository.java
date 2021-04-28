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
import org.polypheny.db.monitoring.events.MonitoringMetric;

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
     * @param metric
     */
    void persistMetric( MonitoringMetric metric );

    /**
     * Get all data for given monitoring persistent type.
     *
     * @param metricClass
     * @param <T>
     * @return
     */
    <T extends MonitoringMetric> List<T> getAllMetrics( Class<T> metricClass );

    /**
     * Get data before specified timestamp for given monitoring persistent type.
     *
     * @param metricClass
     * @param <T>
     * @return
     */
    <T extends MonitoringMetric> List<T> getMetricsBefore( Class<T> metricClass, Timestamp timestamp );

    /**
     * Get data after specified timestamp for given monitoring persistent type.
     *
     * @param metricClass
     * @param <T>
     * @return
     */
    <T extends MonitoringMetric> List<T> getMetricsAfter( Class<T> metricClass, Timestamp timestamp );

}

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

package org.polypheny.db.monitoring.events;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.polypheny.db.monitoring.events.metrics.RoutingDataPoint;

@AllArgsConstructor()
public class RoutingEvent extends BaseEvent {

    private  String queryClassString;
    private Set<Integer> adapterId;
    private double nanoTime;

    @Override
    public <T extends MonitoringDataPoint> List<Class<T>> getMetrics() {
        return Arrays.asList( (Class<T>) RoutingDataPoint.class );
    }


    @Override
    public <T extends MonitoringDataPoint> List<Class<T>> getOptionalMetrics() {
        return Collections.emptyList();
    }


    @Override
    public List<MonitoringDataPoint> analyze() {
        // nothing to analyze, just create the datapoint
        return Arrays.asList(
                RoutingDataPoint.builder()
                        .Id( this.getId() )
                        .recordedTimestamp( this.getRecordedTimestamp() )
                        .adapterId( this.adapterId )
                        .queryClassString( this.queryClassString )
                        .nanoTime( this.nanoTime )
                        .build() );
    }

}

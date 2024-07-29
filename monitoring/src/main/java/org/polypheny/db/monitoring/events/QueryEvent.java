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

package org.polypheny.db.monitoring.events;


import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.analyzer.QueryEventAnalyzer;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;


@Getter
@Setter
@Slf4j
public class QueryEvent extends StatementEvent {

    protected boolean updatePostCosts = false;


    @Override
    public <T extends MonitoringDataPoint> List<Class<T>> getMetrics() {
        return List.of( (Class<T>) QueryDataPointImpl.class );
    }


    @Override
    public List<MonitoringDataPoint> analyze() {
        final QueryDataPoint queryDataPoint = QueryEventAnalyzer.analyze( this );
        if ( updatePostCosts ) {
            MonitoringServiceProvider.getInstance().updateQueryPostCosts( this.physicalQueryClass, this.executionTime );
        }
        return Collections.singletonList( queryDataPoint );
    }

}

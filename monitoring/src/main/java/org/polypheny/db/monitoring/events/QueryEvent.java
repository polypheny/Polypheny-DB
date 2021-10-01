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
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.monitoring.events.analyzer.QueryEventAnalyzer;
import org.polypheny.db.monitoring.events.metrics.QueryDataPoint;
import org.polypheny.db.monitoring.exceptions.GenericEventAnalyzeRuntimeException;


@Getter
@Setter
public class QueryEvent extends StatementEvent {


    private String eventType = "QUERY EVENT";


    @Override
    public <T extends MonitoringDataPoint> List<Class<T>> getMetrics() {
        return Arrays.asList( (Class<T>) QueryDataPoint.class );
    }


    @Override
    public List<MonitoringDataPoint> analyze() {
        try {
            return Arrays.asList( QueryEventAnalyzer.analyze( this ) );
        } catch ( Exception e ) {
            throw new GenericEventAnalyzeRuntimeException( "Could not analyze query event:" );
        }
    }

}

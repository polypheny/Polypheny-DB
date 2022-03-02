/*
 * Copyright 2019-2022 The Polypheny Project
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
import org.polypheny.db.monitoring.events.analyzer.DdlEventAnalyzer;
import org.polypheny.db.monitoring.events.metrics.DdlDataPoint;


public class DdlEvent extends StatementEvent {

    @Override
    public <T extends MonitoringDataPoint> List<Class<T>> getMetrics() {
        return Arrays.asList( (Class<T>) DdlDataPoint.class );
    }


    @Override
    public List<MonitoringDataPoint> analyze() {
        return Arrays.asList( DdlEventAnalyzer.analyze( this ) );
    }

}

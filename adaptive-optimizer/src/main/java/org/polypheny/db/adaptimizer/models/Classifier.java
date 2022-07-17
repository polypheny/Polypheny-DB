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

package org.polypheny.db.adaptimizer.models;

import java.util.HashSet;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adaptimizer.AdaptiveOptimizerImpl;
import org.polypheny.db.monitoring.core.MonitoringService;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;
import org.polypheny.db.plan.PhysicalPlan;

/**
 * Interface class for adaptive operator cost model.
 */
@Slf4j
public abstract class Classifier {
    @Getter(AccessLevel.PRIVATE)
    private static final HashSet<UUID> processed;

    @Getter(AccessLevel.PRIVATE)
    private static final Model model;

    @Setter(AccessLevel.PUBLIC)
    @Getter(AccessLevel.PRIVATE)
    private static MonitoringService monitoringService;

    public static long estimate( PhysicalPlan physicalPlan ) {
        return getModel().estimate( physicalPlan );
    }

    public static void update() {
        UUID dataPointId;
        PhysicalPlan physicalPlan;
        for ( QueryDataPointImpl dataPoint : getMonitoringService().getAllDataPoints( QueryDataPointImpl.class ) ) {
            dataPointId = dataPoint.id();
            if ( ! getProcessed().contains( dataPointId ) ) {
                physicalPlan = dataPoint.getPhysicalPlan();
                physicalPlan.setActualExecutionTime( dataPoint.getExecutionTime() );
                getModel().process( physicalPlan );
                getProcessed().add( dataPoint.getId() );
            }
        }
    }

    // ----- Initializing Classifier ----
    static {
        monitoringService = AdaptiveOptimizerImpl.getMonitoringService();
        model = new StatsModel();
        processed = new HashSet<>();
    }

}

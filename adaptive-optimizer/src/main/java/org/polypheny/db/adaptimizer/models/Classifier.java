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
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.monitoring.core.MonitoringService;
import org.polypheny.db.monitoring.events.metrics.QueryDataPointImpl;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.PhysicalPlan;

/**
 * Interface class for adaptive operator cost model.
 */
@Slf4j
public abstract class Classifier {
    private static int hi = 30;
    private static int lo = 0;

    @Getter(AccessLevel.PRIVATE)
    private static final Model model;

    public static long estimate( PhysicalPlan physicalPlan ) {
        return estimateOrProcess( physicalPlan );
    }

    private static long estimateOrProcess( PhysicalPlan physicalPlan ) {
        try {
            lo++;
            if ( lo <= hi ) {
                InformationQueryPlan queryPlan = new InformationQueryPlan("g", AlgOptUtil.dumpPlan( "Physical Plan", physicalPlan.getRoot(), ExplainFormat.TEXT, ExplainLevel.NON_COST_ATTRIBUTES ) );
                InformationManager.getInstance().getGroup( "g" ).addInformation( queryPlan );
                InformationManager.getInstance().registerInformation( queryPlan );
            }
        } catch ( Exception exception ) {
            lo--;
        }

        if ( physicalPlan.isEstimated() && physicalPlan.isMeasured() ) {
            getModel().process( physicalPlan );
            return physicalPlan.getEstimatedExecutionTime();
        }

        return getModel().estimate( physicalPlan );
    }

    public static void update() {
        getModel().update();
    }

    // ----- Initializing Model ----
    static {
        model = new MachineLearningModel();
    }

}

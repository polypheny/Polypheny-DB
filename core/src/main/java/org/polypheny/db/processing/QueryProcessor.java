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

package org.polypheny.db.processing;


import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptTable.ViewExpander;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.type.RelDataType;


public interface QueryProcessor extends ViewExpander {

    /**
     * @param logicalRoot Logical query plan.
     * @param withMonitoring Activates or deactivates the monitoring.
     * @return prepared PolyphenyDbSignature
     */
    PolyphenyDbSignature<?> prepareQuery( RelRoot logicalRoot, boolean withMonitoring );

    /**
     * @param logicalRoot Logical query plan.
     * @param parameters Row type (required with prepared statements).
     * @param withMonitoring Activates or deactivates the monitoring.
     * @return prepared PolyphenyDbSignature
     */
    PolyphenyDbSignature<?> prepareQuery( RelRoot logicalRoot, RelDataType parameters, boolean withMonitoring );

    /**
     * @param logicalRoot Logical query plan.
     * @param parameters Row type (required with prepared statements).
     * @param isRouted Indicated whether query already routed.
     * @param isSubquery Indicates whether the query is a subquery (used with constraint enforcement)
     * @param withMonitoring Activates or deactivates the monitoring.
     * @return prepared PolyphenyDbSignature
     */
    PolyphenyDbSignature<?> prepareQuery( RelRoot logicalRoot, RelDataType parameters, boolean isRouted, boolean isSubquery, boolean withMonitoring );

    /**
     * @return Gets the planner.
     */
    RelOptPlanner getPlanner();

    /**
     * Resets caches Implementation, QueryPlan, RoutingPlan and Router caches.
     */
    void resetCaches();

}

/*
 * Copyright 2019-2025 The Polypheny Project
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


import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;


public interface QueryProcessor {

    /**
     * @param logicalRoot Logical query plan.
     * @param withMonitoring Activates or deactivates the monitoring.
     * @return prepared PolyphenyDbSignature
     */
    PolyImplementation prepareQuery( AlgRoot logicalRoot, boolean withMonitoring );

    /**
     * @param logicalRoot Logical query plan.
     * @param isRouted Indicated whether query already routed.
     * @param withMonitoring Activates or deactivates the monitoring.
     * @return prepared PolyphenyDbSignature
     */
    PolyImplementation prepareQuery( AlgRoot logicalRoot, boolean isRouted, boolean withMonitoring );

    /**
     * @param root Logical query plan.
     * @param isRouted Indicated whether query already routed.
     * @param isPhysical Indicates whether root is a physical query plan
     * @param withMonitoring Activates or deactivates the monitoring.
     * @return prepared PolyphenyDbSignature
     */
    PolyImplementation prepareQuery( AlgRoot root, boolean isRouted, boolean isPhysical, boolean withMonitoring );

    /**
     * @param logicalRoot Logical query plan.
     * @param parameters Row type (required with prepared statements).
     * @param withMonitoring Activates or deactivates the monitoring.
     * @return prepared PolyphenyDbSignature
     */
    PolyImplementation prepareQuery( AlgRoot logicalRoot, AlgDataType parameters, boolean withMonitoring );

    /**
     * @param logicalRoot Logical query plan.
     * @param parameters Row type (required with prepared statements).
     * @param isRouted Indicated whether query already routed.
     * @param isSubquery Indicates whether the query is a subquery (used with constraint enforcement)
     * @param withMonitoring Activates or deactivates the monitoring.
     * @return prepared PolyphenyDbSignature
     */
    PolyImplementation prepareQuery( AlgRoot logicalRoot, AlgDataType parameters, boolean isRouted, boolean isSubquery, boolean withMonitoring );

    /**
     * @param root Logical or physical query plan
     * @param parameters Row type (required with prepared statements).
     * @param isRouted Indicates whether query already routed.
     * @param isPhysical Indicates whether root is a physical query plan
     * @param isSubquery Indicates whether the query is a subquery (used with constraint enforcement)
     * @param withMonitoring Activates or deactivates the monitoring.
     * @return prepared PolyphenyDbSignature
     */
    PolyImplementation prepareQuery( AlgRoot root, AlgDataType parameters, boolean isRouted, boolean isPhysical, boolean isSubquery, boolean withMonitoring );

    /**
     * @return Gets the planner.
     */
    AlgPlanner getPlanner();

    /**
     * Resets caches Implementation, QueryPlan, RoutingPlan and Router caches.
     */
    void resetCaches();

    /**
     * To acquire a global shared lock for a statement.
     * This method is used before the statistics are updated to make sure nothing changes during the updating process.
     */
    void lock( Statement statement );

    void lock( Transaction transaction );

}

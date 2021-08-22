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

package org.polypheny.db.routing;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.util.Pair;

public interface ProposedRoutingPlan extends RoutingPlan {


    /**
     * @return the relRoot of the proposed routing plan.
     */
    RelRoot getRoutedRoot();

    /**
     * Sets the routed relRoot
     *
     * @param relRoot the routed rel root.
     */
    void setRoutedRoot( RelRoot relRoot );

    /**
     * @return the query class.
     */
    String getQueryClass();

    /**
     * @param queryClass the query class string.
     */
    void setQueryClass( String queryClass );

    /**
     * @return gets the physical query class as Optional.
     */
    Optional<String> getOptionalPhysicalQueryClass();

    /**
     * @param physicalQueryClass the physical queryClass
     */
    void setOptionalPhysicalQueryId( Optional<String> physicalQueryClass );

    /**
     * @return gets the router class which proposed the plan.
     */
    Optional<Class<? extends Router>> getRouter();

    /**
     * @param routerClass the router which proposed this plan.
     */
    void setRouter( Optional<Class<? extends Router>> routerClass );

    /**
     * @return the physical placements of the necessary partitions: partitionId, list<CatalogPlacementIds>
     */
    Optional<Map<Long, List<Pair<Integer, Long>>>> getPhysicalPlacementsOfPartitions(); // partitionId, list<CatalogPlacementIds>

    /**
     * @return Optional pre costs.
     */
    Optional<RelOptCost> getPreCosts();

    /**
     * Sets the pre costs.
     *
     * @param preCosts the approximated rel costs.
     */
    void setPreCosts( Optional<RelOptCost> preCosts );

    /**
     * @return true if routing plan is cacheable. Currenty, dml and ddls will not be cached.
     */
    boolean isCacheable();

}

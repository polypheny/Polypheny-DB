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
    @Override
    String getQueryClass();

    /**
     * @param queryClass the query class string.
     */
    void setQueryClass( String queryClass );

    /**
     * @return gets the physical query class as Optional.
     */
    String getOptionalPhysicalQueryClass();

    /**
     * @param physicalQueryClass the physical queryClass
     */
    void setOptionalPhysicalQueryId( String physicalQueryClass );

    /**
     * @return gets the router class which proposed the plan.
     */
    @Override
    Class<? extends Router> getRouter();

    /**
     * @param routerClass the router which proposed this plan.
     */
    void setRouter( Class<? extends Router> routerClass );

    /**
     * @return the physical placements of the necessary partitions: partitionId, list<CatalogPlacementIds>
     */
    Map<Long, List<Pair<Integer, Long>>> getPhysicalPlacementsOfPartitions(); // partitionId -> List<CatalogPlacementIds>

    /**
     * @return Optional pre costs.
     */
    RelOptCost getPreCosts();

    /**
     * Sets the pre costs.
     *
     * @param preCosts the approximated rel costs.
     */
    void setPreCosts( RelOptCost preCosts );

    /**
     * @return true if routing plan is cacheable. Currently, DMLs and DDLs will not be cached.
     */
    boolean isCacheable();

}

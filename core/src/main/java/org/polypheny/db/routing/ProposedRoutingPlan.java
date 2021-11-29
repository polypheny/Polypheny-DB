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
import org.polypheny.db.tools.RoutedRelBuilder.SelectedAdapterInfo;
import org.polypheny.db.util.Pair;


public interface ProposedRoutingPlan extends RoutingPlan {

    /**
     * @return The relRoot of the proposed routing plan.
     */
    RelRoot getRoutedRoot();

    /**
     * Sets the routed relRoot
     *
     * @param relRoot The routed rel root.
     */
    void setRoutedRoot( RelRoot relRoot );

    /**
     * @return The query class.
     */
    @Override
    String getQueryClass();

    /**
     * @param queryClass The query class string.
     */
    void setQueryClass( String queryClass );

    /**
     * @return Gets the router class which proposed the plan.
     */
    @Override
    Class<? extends Router> getRouter();

    /**
     * @param routerClass the router which proposed this plan.
     */
    void setRouter( Class<? extends Router> routerClass );

    /**
     * @return The physical placements of the necessary partitions: PartitionId -> List<AdapterId, CatalogColumnPlacementId>
     */
    @Override
    Map<Long, List<Pair<Integer, Long>>> getPhysicalPlacementsOfPartitions(); // PartitionId -> List<AdapterId, CatalogColumnPlacementId>

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

    /**
     * For reporting in the UI
     */
    Map<Long, SelectedAdapterInfo> getSelectedAdaptersInfo();

}

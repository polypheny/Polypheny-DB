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

public interface ProposedRoutingPlan extends RoutingPlan{


    RelRoot getRoutedRoot();
    void setRoutedRoot(RelRoot relRoot);

    String getQueryId();
    void setQueryId(String queryId);

    Optional<String> getOptionalPhysicalQueryId();
    void setOptionalPhysicalQueryId( Optional<String> physicalQueryId);

    Optional<Class<? extends Router>> getRouter();
    void setRouter(Optional<Class<? extends Router>> routerClass);

    Optional<Map<Long, List<Pair<Integer, Long>>>>  getPhysicalPlacementsOfPartitions(); // partitionId, list<CatalogPlacementIds>
    void setPhysicalPlacementsOfPartitions(Optional<Map<Long, List<Pair<Integer, Long>>>> physicalPartitionPlacement);

    Optional<RelOptCost> getPreCosts();
    void setPreCosts(Optional<RelOptCost> preCosts);

    boolean isCachable();

    //CachedProposedRoutingPlan convert( RelOptCost approximatedCosts );
}

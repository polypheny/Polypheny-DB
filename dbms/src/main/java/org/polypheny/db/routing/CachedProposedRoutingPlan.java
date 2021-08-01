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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.util.Pair;

@Getter
@Setter
@Slf4j
public class CachedProposedRoutingPlan implements RoutingPlan{
    protected String queryId;
    protected String physicalQueryId;
    protected RelOptCost preCosts;
    protected Optional<Class<? extends Router>> routingClass;
    protected Map<Long, List<Pair<Integer, Long>>> physicalPlacementsOfPartitions; // partitionId, list<CatalogPlacementIds>


    public CachedProposedRoutingPlan( ProposedRoutingPlan routingPlan, RelOptCost approximatedCosts){
        this.queryId = routingPlan.getQueryId();
        this.preCosts = approximatedCosts;
        this.routingClass = routingPlan.getRouter();
        this.physicalPlacementsOfPartitions = ImmutableMap.copyOf( routingPlan.getPhysicalPlacementsOfPartitions().get() );
        this.physicalQueryId = routingPlan.getPhysicalQueryId();
    }

}

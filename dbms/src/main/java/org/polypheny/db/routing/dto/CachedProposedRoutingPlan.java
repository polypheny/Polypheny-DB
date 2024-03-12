/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.routing.dto;

import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.routing.ColumnDistribution.RoutedDistribution;
import org.polypheny.db.routing.FieldDistribution;
import org.polypheny.db.routing.ProposedRoutingPlan;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.RoutingPlan;


/**
 * Cached version of the routing plan.
 */
@Getter
@Setter
public class CachedProposedRoutingPlan implements RoutingPlan {

    @Getter
    public RoutedDistribution routedDistribution; // PartitionId -> List<Pair<PlacementId, ColumnId>>
    @Getter
    public FieldDistribution fieldDistribution; // PartitionId -> List<AllocationColumn>

    protected String queryClass;
    protected String physicalQueryClass;
    protected AlgOptCost preCosts;
    protected Class<? extends Router> router;


    public CachedProposedRoutingPlan( ProposedRoutingPlan routingPlan, AlgOptCost approximatedCosts ) {
        this.queryClass = routingPlan.getQueryClass();
        this.preCosts = approximatedCosts;
        this.router = routingPlan.getRouter();
        this.routedDistribution = routingPlan.getRoutedDistribution();
        this.fieldDistribution = getFieldDistribution();
        this.physicalQueryClass = routingPlan.getPhysicalQueryClass();
    }


}

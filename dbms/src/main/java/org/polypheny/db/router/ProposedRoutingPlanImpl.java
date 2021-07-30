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

package org.polypheny.db.router;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.routing.ProposedRoutingPlan;
import org.polypheny.db.routing.Router;
import org.polypheny.db.tools.RoutedRelBuilder;

@Setter
@Getter
public class ProposedRoutingPlanImpl implements ProposedRoutingPlan {
    protected RelRoot routedRoot;
    protected String queryId;
    protected String physicalQueryId;
    protected Optional<Class<? extends Router>> router = Optional.empty();
    protected Optional<Map<Long, List<Long>>>  physicalPlacementsOfPartitions = Optional.empty(); // partitionId, list<CatalogPlacementIds>

    public ProposedRoutingPlanImpl( RoutedRelBuilder routedRelBuilder, RelRoot logicalRoot, String queryId){
        this.physicalPlacementsOfPartitions = Optional.of( routedRelBuilder.getPhysicalPlacementsOfPartitions() );
        this.queryId = queryId;
        val rel = routedRelBuilder.build();
        this.routedRoot = new RelRoot( rel , logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
    }

    public ProposedRoutingPlanImpl( RoutedRelBuilder routedRelBuilder, RelRoot logicalRoot, String queryId, Class<? extends Router> routerClass){
        this.physicalPlacementsOfPartitions = Optional.of( routedRelBuilder.getPhysicalPlacementsOfPartitions() );
        this.queryId = queryId;
        this.router = Optional.of( routerClass );
        val rel = routedRelBuilder.build();
        this.routedRoot = new RelRoot( rel , logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
    }

    public ProposedRoutingPlanImpl( RelNode routedConditional, RelRoot logicalRoot, String queryId){
        this.queryId = queryId;
        this.routedRoot = new RelRoot( routedConditional , logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
    }


}

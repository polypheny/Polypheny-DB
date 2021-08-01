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
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.util.Pair;

@Setter
@Getter
public class ProposedRoutingPlanImpl implements ProposedRoutingPlan {
    protected RelRoot routedRoot;
    protected String queryId;
    protected Optional<String> physicalQueryId = Optional.empty();
    protected Optional<Class<? extends Router>> router = Optional.empty();
    protected Optional<Map<Long, List<Pair<Integer, Long>>>>  physicalPlacementsOfPartitions = Optional.empty(); // partitionId, list<CatalogPlacementIds>
    protected Optional<RelOptCost> preCosts = Optional.empty();

    public ProposedRoutingPlanImpl( RoutedRelBuilder routedRelBuilder, RelRoot logicalRoot, String queryId){
        this.physicalPlacementsOfPartitions = Optional.of( routedRelBuilder.getPhysicalPlacementsOfPartitions() );
        this.queryId = queryId;
        this.physicalQueryId = Optional.of( queryId + this.physicalPlacementsOfPartitions.get() );
        val rel = routedRelBuilder.build();
        this.routedRoot = new RelRoot( rel , logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
    }

    public ProposedRoutingPlanImpl( RoutedRelBuilder routedRelBuilder, RelRoot logicalRoot, String queryId, Class<? extends Router> routerClass){
        this.physicalPlacementsOfPartitions = Optional.of( routedRelBuilder.getPhysicalPlacementsOfPartitions() );
        this.queryId = queryId;
        this.physicalQueryId = Optional.of( queryId + this.physicalPlacementsOfPartitions.get() );
        this.router = Optional.of( routerClass );
        val rel = routedRelBuilder.build();
        this.routedRoot = new RelRoot( rel , logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
    }

    public ProposedRoutingPlanImpl( RoutedRelBuilder routedRelBuilder, RelRoot logicalRoot, String queryId, CachedProposedRoutingPlan cachedPlan){
        this.physicalPlacementsOfPartitions = Optional.of( cachedPlan.getPhysicalPlacementsOfPartitions() );
        this.queryId = queryId;
        this.physicalQueryId = Optional.of( queryId + this.physicalPlacementsOfPartitions.get() );
        this.router = cachedPlan.getRoutingClass();
        val rel = routedRelBuilder.build();
        this.routedRoot = new RelRoot( rel , logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
    }



    public ProposedRoutingPlanImpl( RelNode routedConditional, RelRoot logicalRoot, String queryId){
        this.queryId = queryId;
        this.routedRoot = new RelRoot( routedConditional , logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
    }

    public ProposedRoutingPlanImpl( RelRoot routedRoot, String queryId){
        this.queryId = queryId;
        this.routedRoot = routedRoot;
    }

    public CachedProposedRoutingPlan convert(ProposedRoutingPlan routingPlan, RelOptCost approximatedCosts ){
        return new CachedProposedRoutingPlan( routingPlan, approximatedCosts );
    }


    @Override
    public Optional<String> getOptionalPhysicalQueryId() {
        return physicalQueryId;
    }


    @Override
    public void setOptionalPhysicalQueryId( Optional<String> physicalQueryId ) {
        this.physicalQueryId = physicalQueryId;
    }

    @Override
    public String getPhysicalQueryId() {
        return physicalQueryId.isPresent() ? physicalQueryId.get() : "";
    }

    @Override
    public boolean isCachable() {
        return this.physicalPlacementsOfPartitions.isPresent() && this.getOptionalPhysicalQueryId().isPresent() && !this.routedRoot.kind.belongsTo( SqlKind.DML );
    }

}

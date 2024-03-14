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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.routing.ColumnDistribution.RoutedDistribution;
import org.polypheny.db.routing.ProposedRoutingPlan;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.RoutingContext;
import org.polypheny.db.tools.RoutedAlgBuilder;


/**
 * Some information are not available during instantiation and therefore will be added later on.
 */
@Setter
@Getter
public class ProposedRoutingPlanImpl implements ProposedRoutingPlan {

    protected AlgRoot routedRoot;
    protected String queryClass;
    protected String physicalQueryClass;
    protected Class<? extends Router> router;
    protected RoutedDistribution routedDistribution; // PartitionId -> List<AdapterId, CatalogColumnPlacementId>
    protected AlgOptCost preCosts;


    public ProposedRoutingPlanImpl( RoutingContext context, RoutedAlgBuilder routedAlgBuilder, AlgRoot logicalRoot, String queryClass, Class<? extends Router> routerClass ) {
        this.routedDistribution = context.getRoutedDistribution();
        this.queryClass = queryClass;
        this.physicalQueryClass = queryClass + (context.getFieldDistribution() == null ? "" : context.getFieldDistribution().hashCode());
        this.router = routerClass;
        AlgNode alg = routedAlgBuilder.build();
        this.routedRoot = new AlgRoot( alg, logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
    }


    public ProposedRoutingPlanImpl( RoutingContext context, RoutedAlgBuilder routedAlgBuilder, AlgRoot logicalRoot, String queryClass, CachedProposedRoutingPlan cachedPlan ) {
        this( context, routedAlgBuilder, logicalRoot, queryClass, cachedPlan.getRouter() );
    }


    public ProposedRoutingPlanImpl( AlgNode routedConditional, AlgRoot logicalRoot, String queryClass ) {
        this.queryClass = queryClass;
        this.routedRoot = new AlgRoot( routedConditional, logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
    }


    public ProposedRoutingPlanImpl( AlgRoot routedRoot, String queryClass ) {
        this.queryClass = queryClass;
        this.routedRoot = routedRoot;
    }


    @Override
    public String getPhysicalQueryClass() {
        return physicalQueryClass != null ? physicalQueryClass : "";
    }


    @Override
    public boolean isCacheable() {
        return this.routedDistribution != null
                && this.getPhysicalQueryClass() != null
                && !this.routedRoot.kind.belongsTo( Kind.DML );
    }


    /**
     * Two proposed routing plans are considered equals, when the physicalPlacements are the same.
     *
     * @param obj other proposedRoutingPlan
     * @return true if physicalPlacements are equal.
     */
    @Override
    public boolean equals( Object obj ) {
        final ProposedRoutingPlanImpl other = (ProposedRoutingPlanImpl) obj;
        if ( other == null ) {
            return false;
        }

        if ( routedDistribution == null && other.routedDistribution == null ) {
            return true;
        }

        if ( this.routedDistribution == null || other.routedDistribution == null ) {
            return true;
        }

        return this.routedDistribution.equals( other.routedDistribution );
    }


    /**
     * Two proposed routing plans are considered equals, when the physicalPlacements are the same.
     * The same is valid for their hashCode.
     *
     * @return hashCode of the proposedRoutingPlan
     */
    @Override
    public int hashCode() {
        if ( this.routedDistribution != null ) {
            return this.routedDistribution.hashCode();
        }
        return super.hashCode();
    }

}

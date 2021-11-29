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

package org.polypheny.db.routing.dto;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.routing.ProposedRoutingPlan;
import org.polypheny.db.routing.Router;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.tools.RoutedRelBuilder.SelectedAdapterInfo;
import org.polypheny.db.util.Pair;


/**
 * Some information are not available during instantiation and therefore will be added later on.
 */
@Setter
@Getter
public class ProposedRoutingPlanImpl implements ProposedRoutingPlan {

    protected RelRoot routedRoot;
    protected String queryClass;
    protected String physicalQueryClass;
    protected Class<? extends Router> router;
    protected Map<Long, List<Pair<Integer, Long>>> physicalPlacementsOfPartitions; // PartitionId -> List<AdapterId, CatalogColumnPlacementId>
    protected Map<Long, SelectedAdapterInfo> selectedAdaptersInfo = new HashMap<>(); // For reporting in the UI
    protected RelOptCost preCosts;


    public ProposedRoutingPlanImpl( RoutedRelBuilder routedRelBuilder, RelRoot logicalRoot, String queryClass, Class<? extends Router> routerClass ) {
        this.physicalPlacementsOfPartitions = routedRelBuilder.getPhysicalPlacementsOfPartitions();
        this.queryClass = queryClass;
        this.physicalQueryClass = queryClass + this.physicalPlacementsOfPartitions;
        this.router = routerClass;
        RelNode rel = routedRelBuilder.build();
        this.routedRoot = new RelRoot( rel, logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
        this.selectedAdaptersInfo = routedRelBuilder.getSelectedAdaptersInfo();
    }


    public ProposedRoutingPlanImpl( RoutedRelBuilder routedRelBuilder, RelRoot logicalRoot, String queryClass, CachedProposedRoutingPlan cachedPlan ) {
        this.physicalPlacementsOfPartitions = cachedPlan.getPhysicalPlacementsOfPartitions();
        this.queryClass = queryClass;
        this.physicalQueryClass = queryClass + this.physicalPlacementsOfPartitions;
        this.router = cachedPlan.getRouter();
        RelNode rel = routedRelBuilder.build();
        this.routedRoot = new RelRoot( rel, logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
        this.selectedAdaptersInfo = cachedPlan.getSelectedAdaptersInfo();
    }


    public ProposedRoutingPlanImpl( RelNode routedConditional, RelRoot logicalRoot, String queryClass ) {
        this.queryClass = queryClass;
        this.routedRoot = new RelRoot( routedConditional, logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
    }


    public ProposedRoutingPlanImpl( RelRoot routedRoot, String queryClass ) {
        this.queryClass = queryClass;
        this.routedRoot = routedRoot;
    }


    @Override
    public String getPhysicalQueryClass() {
        return physicalQueryClass != null ? physicalQueryClass : "";
    }


    @Override
    public boolean isCacheable() {
        return this.physicalPlacementsOfPartitions != null
                && this.getPhysicalQueryClass() != null
                && !this.routedRoot.kind.belongsTo( SqlKind.DML );
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

        if ( physicalPlacementsOfPartitions == null && other.physicalPlacementsOfPartitions == null ) {
            return true;
        }

        if ( this.physicalPlacementsOfPartitions == null || other.physicalPlacementsOfPartitions == null ) {
            return true;
        }

        for ( Map.Entry<Long, List<Pair<Integer, Long>>> entry : this.physicalPlacementsOfPartitions.entrySet() ) {
            final Long id = entry.getKey();
            List<Pair<Integer, Long>> values = entry.getValue();

            if ( !other.physicalPlacementsOfPartitions.containsKey( id ) ) {
                return false;
            } else {
                if ( !values.containsAll( other.physicalPlacementsOfPartitions.get( id ) ) ) {
                    return false;
                }
            }
        }
        return true;
    }


    /**
     * Two proposed routing plans are considered equals, when the physicalPlacements are the same.
     * The same is valid for their hashCode.
     *
     * @return hashCode of the proposedRoutingPlan
     */
    @Override
    public int hashCode() {
        if ( this.physicalPlacementsOfPartitions != null && !this.physicalPlacementsOfPartitions.isEmpty() ) {
            return this.physicalPlacementsOfPartitions.values()
                    .stream().flatMap( Collection::stream )
                    .map( elem -> elem.right.hashCode() * elem.left.hashCode() )
                    .reduce( ( a, b ) -> a + b )
                    .get();
        }
        return super.hashCode();
    }

}

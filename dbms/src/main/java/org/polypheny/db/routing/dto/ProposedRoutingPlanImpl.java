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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.routing.ProposedRoutingPlan;
import org.polypheny.db.routing.Router;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.util.Pair;

/**
 * Some information are not available during instantiation and therefore will be added later on.
 * These values are all marked as Optional<>.
 */
@Setter
@Getter
public class ProposedRoutingPlanImpl implements ProposedRoutingPlan {

    protected RelRoot routedRoot;
    protected String queryClass;
    protected Optional<String> physicalQueryClass = Optional.empty();
    protected Optional<Class<? extends Router>> router = Optional.empty();
    protected Optional<Map<Long, List<Pair<Integer, Long>>>> physicalPlacementsOfPartitions = Optional.empty(); // partitionId, list<CatalogPlacementIds>
    protected Optional<RelOptCost> preCosts = Optional.empty();


    public ProposedRoutingPlanImpl( RoutedRelBuilder routedRelBuilder, RelRoot logicalRoot, String queryClass ) {
        this.physicalPlacementsOfPartitions = Optional.of( routedRelBuilder.getPhysicalPlacementsOfPartitions() );
        this.queryClass = queryClass;
        this.physicalQueryClass = Optional.of( queryClass + this.physicalPlacementsOfPartitions.get() );
        RelNode rel = routedRelBuilder.build();
        this.routedRoot = new RelRoot( rel, logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
    }


    public ProposedRoutingPlanImpl( RoutedRelBuilder routedRelBuilder, RelRoot logicalRoot, String queryClass, Class<? extends Router> routerClass ) {
        this.physicalPlacementsOfPartitions = Optional.of( routedRelBuilder.getPhysicalPlacementsOfPartitions() );
        this.queryClass = queryClass;
        this.physicalQueryClass = Optional.of( queryClass + this.physicalPlacementsOfPartitions.get() );
        this.router = Optional.of( routerClass );
        RelNode rel = routedRelBuilder.build();
        this.routedRoot = new RelRoot( rel, logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
    }


    public ProposedRoutingPlanImpl( RoutedRelBuilder routedRelBuilder, RelRoot logicalRoot, String queryClass, CachedProposedRoutingPlan cachedPlan ) {
        this.physicalPlacementsOfPartitions = Optional.of( cachedPlan.getPhysicalPlacementsOfPartitions() );
        this.queryClass = queryClass;
        this.physicalQueryClass = Optional.of( queryClass + this.physicalPlacementsOfPartitions.get() );
        this.router = cachedPlan.getRouter();
        RelNode rel = routedRelBuilder.build();
        this.routedRoot = new RelRoot( rel, logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
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
    public Optional<String> getOptionalPhysicalQueryClass() {
        return physicalQueryClass;
    }


    @Override
    public void setOptionalPhysicalQueryId( Optional<String> physicalQueryClass ) {
        this.physicalQueryClass = physicalQueryClass;
    }


    @Override
    public String getPhysicalQueryClass() {
        return physicalQueryClass.isPresent() ? physicalQueryClass.get() : "";
    }


    @Override
    public Optional<Map<Long, List<Pair<Integer, Long>>>> getOptionalPhysicalPlacementsOfPartitions() {
        return this.getPhysicalPlacementsOfPartitions();
    }


    @Override
    public boolean isCacheable() {
        return this.physicalPlacementsOfPartitions.isPresent() && this.getOptionalPhysicalQueryClass().isPresent() && !this.routedRoot.kind.belongsTo( SqlKind.DML );
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

        if ( !this.physicalPlacementsOfPartitions.isPresent() && !other.physicalPlacementsOfPartitions.isPresent() ) {
            return true;
        }

        if ( !this.physicalPlacementsOfPartitions.isPresent() || !other.physicalPlacementsOfPartitions.isPresent() ) {
            return true;
        }

        for ( Map.Entry<Long, List<Pair<Integer, Long>>> entry : this.physicalPlacementsOfPartitions.get().entrySet() ) {
            final Long id = entry.getKey();
            List<Pair<Integer, Long>> values = entry.getValue();

            if ( !other.physicalPlacementsOfPartitions.get().containsKey( id ) ) {
                return false;
            } else {
                if ( !values.containsAll( other.physicalPlacementsOfPartitions.get().get( id ) ) ) {
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
        if ( this.physicalPlacementsOfPartitions.isPresent() && !this.physicalPlacementsOfPartitions.get().isEmpty() ) {
            return this.physicalPlacementsOfPartitions.get().values()
                    .stream().flatMap( Collection::stream )
                    .map( elem -> elem.right.hashCode() * elem.left.hashCode() )
                    .reduce( ( a, b ) -> a + b ).get();
        }
        return super.hashCode();
    }

}

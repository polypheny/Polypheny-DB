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

package org.polypheny.db.routing.routers;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.core.document.DocumentScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.routing.ColumnDistribution;
import org.polypheny.db.routing.RoutingContext;
import org.polypheny.db.routing.dto.CachedProposedRoutingPlan;
import org.polypheny.db.tools.RoutedAlgBuilder;


/**
 * Routing of cached plan. Nothing needs to be found, everything included in the routed plan.
 */
@Slf4j
public class CachedPlanRouter extends BaseRouter {


    public RoutedAlgBuilder routeCached( AlgRoot logicalRoot, CachedProposedRoutingPlan routingPlanCached, RoutingContext context ) {
        final RoutedAlgBuilder builder = context.getRoutedAlgBuilder();
        return buildCachedSelect( logicalRoot.alg, builder, routingPlanCached, context );
    }


    private RoutedAlgBuilder buildCachedSelect( AlgNode node, RoutedAlgBuilder builder, CachedProposedRoutingPlan cachedPlan, RoutingContext context ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            builder = buildCachedSelect( node.getInput( i ), builder, cachedPlan, context );
        }

        if ( node.unwrap( DocumentScan.class ).isPresent() ) {
            return builder.push( super.handleDocScan( (DocumentScan<?>) node, context.getStatement(), null ) );
        }

        if ( node.unwrap( LogicalRelScan.class ).isPresent() && node.getEntity() != null ) {
            return builder.push( super.buildJoinedScan( (ColumnDistribution) cachedPlan.fieldDistribution, context ) );
        } else if ( node instanceof LogicalRelValues ) {
            return super.handleValues( (LogicalRelValues) node, builder );
        } else {
            return super.handleGeneric( node, builder );
        }
    }

}

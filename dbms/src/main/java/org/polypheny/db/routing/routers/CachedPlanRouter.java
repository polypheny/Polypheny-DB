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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.SetOp;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.routing.RoutingContext;
import org.polypheny.db.routing.dto.CachedProposedRoutingPlan;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.RoutedAlgBuilder;


/**
 * Replays the allocation selected for a direct scan in the current statement's algebra cluster.
 */
public class CachedPlanRouter extends BaseRouter {


    public boolean supports( AlgRoot logicalRoot, Snapshot snapshot ) {
        if ( logicalRoot.getModel() != ModelTrait.RELATIONAL || !logicalRoot.kind.belongsTo( Kind.QUERY ) ) {
            return false;
        }
        AlgNode node = logicalRoot.alg;
        Set<AlgNode> visited = Collections.newSetFromMap( new IdentityHashMap<>() );
        while ( node.getInputs().size() == 1 && !(node instanceof SetOp) ) {
            if ( !visited.add( node ) ) {
                return false;
            }
            node = node.getInput( 0 );
        }
        if ( node.getClass() != LogicalRelScan.class || !(node.getEntity() instanceof LogicalTable table) ) {
            return false;
        }
        return snapshot.alloc().getPlacementsFromLogical( table.id ).size() == 1
                && snapshot.alloc().getPartitionsFromLogical( table.id ).size() == 1
                && snapshot.alloc().getFromLogical( table.id ).size() == 1;
    }


    public RoutedAlgBuilder routeCached( AlgRoot logicalRoot, CachedProposedRoutingPlan cachedPlan, RoutingContext context ) {
        return buildCachedSelect( logicalRoot.alg, context.getRoutedAlgBuilder(), cachedPlan, context );
    }


    private RoutedAlgBuilder buildCachedSelect( AlgNode node, RoutedAlgBuilder builder, CachedProposedRoutingPlan cachedPlan, RoutingContext context ) {
        if ( node.getInputs().isEmpty() ) {
            return handleRelScan( builder, context.getStatement(), cachedPlan.allocation() );
        }
        return handleGeneric( node, buildCachedSelect( node.getInput( 0 ), builder, cachedPlan, context ) );
    }

}

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

package org.polypheny.db.routing;

import java.util.List;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.transaction.Statement;


/**
 * Interface for routing DML queries.
 */
public interface DmlRouter {

    /**
     * Routes DML queries and returns a RelNode.
     */
    AlgNode routeDml( LogicalRelModify node, Statement statement );

    /**
     * Routes conditional executes and directly returns a RelNode.
     */
    AlgNode handleConditionalExecute( AlgNode node, RoutingContext context );

    AlgNode handleConstraintEnforcer( AlgNode alg, RoutingContext context );

    AlgNode handleBatchIterator( AlgNode alg, RoutingContext context );

    AlgNode routeDocumentDml( LogicalDocumentModify alg, Statement statement, @Nullable AllocationEntity target, @Nullable List<Long> excludedPlacements );

    AlgNode routeGraphDml( LogicalLpgModify alg, Statement statement, @Nullable AllocationEntity target, @Nullable List<Long> excludedPlacements );

}

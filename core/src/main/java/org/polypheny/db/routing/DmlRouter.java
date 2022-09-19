/*
 * Copyright 2019-2022 The Polypheny Project
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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.transaction.Statement;


/**
 * Interface for routing DML queries.
 */
public interface DmlRouter {

    /**
     * Routes DML queries and returns a RelNode.
     */
    AlgNode routeDml( LogicalModify node, Statement statement );

    /**
     * Routes conditional executes and directly returns a RelNode.
     */
    AlgNode handleConditionalExecute( AlgNode node, Statement statement, LogicalQueryInformation queryInformation );

    AlgNode routeGraphDml( LogicalLpgModify alg, Statement statement );

    AlgNode handleConstraintEnforcer( AlgNode alg, Statement statement, LogicalQueryInformation queryInformation );

    AlgNode handleBatchIterator( AlgNode alg, Statement statement, LogicalQueryInformation queryInformation );

    AlgNode routeDocumentDml( LogicalDocumentModify alg, Statement statement, LogicalQueryInformation queryInformation, Integer adapterId );

    AlgNode routeGraphDml( LogicalLpgModify alg, Statement statement, CatalogGraphDatabase catalogGraph, List<Integer> placements );

}

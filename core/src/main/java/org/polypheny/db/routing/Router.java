/*
 * Copyright 2019-2020 The Polypheny Project
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
import org.polypheny.db.adapter.Store;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.transaction.Statement;

public interface Router {

    RelRoot route( RelRoot relRoot, Statement statement, ExecutionTimeMonitor executionTimeMonitor );

    List<Store> createTable( long schemaId, Statement statement );

    List<Store> addColumn( CatalogTable catalogTable, Statement statement );

    void dropPlacements( List<CatalogColumnPlacement> placements );

    RelNode buildJoinedTableScan( Statement statement, RelOptCluster cluster, List<CatalogColumnPlacement> placements );
}

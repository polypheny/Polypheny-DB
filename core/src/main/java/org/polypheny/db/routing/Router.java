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
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.transaction.Statement;

public interface Router {

    AlgRoot route( AlgRoot relRoot, Statement statement, ExecutionTimeMonitor executionTimeMonitor );

    List<DataStore> createTable( long schemaId, Statement statement );

    List<DataStore> addColumn( CatalogTable catalogTable, Statement statement );

    void dropPlacements( List<CatalogColumnPlacement> placements );

    AlgNode buildJoinedTableScan( Statement statement, AlgOptCluster cluster, Map<Long, List<CatalogColumnPlacement>> placements );

    void resetCaches();

}

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

package org.polypheny.db.router;

import java.util.List;
import java.util.Set;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.transaction.Statement;

public class UnifiedRouting extends AbstractRouter {

    @Override
    public List<DataStore> createTable( long schemaId, Statement statement ) {
        return null;
    }


    @Override
    public List<DataStore> addColumn( CatalogTable catalogTable, Statement statement ) {
        return null;
    }


    @Override
    public void dropPlacements( List<CatalogColumnPlacement> placements ) {

    }


    @Override
    protected void analyze( Statement statement, RelRoot logicalRoot ) {

    }


    @Override
    protected void wrapUp( Statement statement, RelNode routed ) {

    }


    @Override
    protected Set<List<CatalogColumnPlacement>> selectPlacement( RelNode node, CatalogTable catalogTable ) {
        return null;
    }

}

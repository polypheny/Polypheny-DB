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

package org.polypheny.db.catalog.entity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.logical.LogicalJoin;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.sql.SqlKind;

public class CatalogView extends CatalogTable{

    public CatalogView( long id, @NonNull String name, ImmutableList<Long> columnIds, long schemaId, long databaseId, int ownerId, @NonNull String ownerName, @NonNull Catalog.TableType type, RelNode definition, Long primaryKey, @NonNull ImmutableMap<Integer, ImmutableList<Long>> placementsByAdapter, boolean modifiable ) {
        super( id, name, columnIds, schemaId, databaseId, ownerId, ownerName, type, definition, primaryKey, placementsByAdapter, modifiable );
    }

    public RelRoot prepareRelRoot(RelRoot logicalRoot){
        RelRoot viewLogicalRoot = RelRoot.of( definition, SqlKind.SELECT );
        RelOptCluster relOptCluster = logicalRoot.rel.getCluster();
        repareViewCluster( viewLogicalRoot.rel, relOptCluster );
        return viewLogicalRoot;
    }

    public void repareViewCluster (RelNode viewLogicalRoot, RelOptCluster relOptCluster){
        if(viewLogicalRoot instanceof LogicalProject ){
            ((LogicalProject) viewLogicalRoot).setCluster( relOptCluster );
            repareViewCluster( ((LogicalProject) viewLogicalRoot).getInput(), relOptCluster );
        }else if(viewLogicalRoot instanceof LogicalJoin ){
            ((LogicalJoin) viewLogicalRoot).setCluster( relOptCluster );
            repareViewCluster(((LogicalJoin) viewLogicalRoot).getLeft(), relOptCluster );
            repareViewCluster(((LogicalJoin) viewLogicalRoot).getRight(), relOptCluster );
        }else if(viewLogicalRoot instanceof LogicalTableScan ){
            ((LogicalTableScan)viewLogicalRoot).setCluster( relOptCluster );
        }
    }

}

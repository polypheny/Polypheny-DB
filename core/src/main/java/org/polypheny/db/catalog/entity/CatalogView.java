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
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PartitionType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.BiRel;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rel.logical.LogicalViewTableScan;
import org.polypheny.db.view.ViewManager.ViewVisitor;

public class CatalogView extends CatalogTable {

    private static final long serialVersionUID = -4453089531698670528L;

    @Getter
    private final Map<Long, List<Long>> underlyingTables;
    @Getter
    private final QueryLanguage language;
    @Getter
    private final RelCollation relCollation;
    @Getter
    String query;


    public CatalogView(
            long id,
            @NonNull String name,
            ImmutableList<Long> columnIds,
            long schemaId,
            long databaseId,
            int ownerId,
            @NonNull String ownerName,
            @NonNull Catalog.TableType type,
            String query,
            Long primaryKey,
            @NonNull ImmutableMap<Integer, ImmutableList<Long>> placementsByAdapter,
            boolean modifiable,
            RelCollation relCollation,
            Map<Long, List<Long>> underlyingTables,
            QueryLanguage language ) {
        super( id, name, columnIds, schemaId, databaseId, ownerId, ownerName, type, primaryKey, placementsByAdapter, modifiable );
        this.query = query;
        this.relCollation = relCollation;
        this.underlyingTables = underlyingTables;
        this.language = language;
    }


    public CatalogView(
            long id,
            String name,
            ImmutableList<Long> columnIds,
            long schemaId,
            long databaseId,
            int ownerId,
            String ownerName,
            TableType tableType,
            String query,
            Long primaryKey,
            ImmutableMap<Integer, ImmutableList<Long>> placementsByAdapter,
            boolean modifiable,
            long numPartitions,
            PartitionType partitionType,
            ImmutableList<Long> partitionIds,
            long partitionColumnId,
            boolean isPartitioned,
            RelCollation relCollation,
            ImmutableList<Long> connectedViews,
            Map<Long, List<Long>> underlyingTables,
            QueryLanguage language ) {
        super( id, name, columnIds, schemaId, databaseId, ownerId, ownerName, tableType, primaryKey, placementsByAdapter, modifiable, numPartitions, partitionType, partitionIds, partitionColumnId, isPartitioned, connectedViews );
        this.query = query;
        this.relCollation = relCollation;
        this.underlyingTables = underlyingTables;
        this.language = language;
    }


    @Override
    public CatalogTable getConnectedViews( ImmutableList<Long> newConnectedViews ) {
        return new CatalogView(
                id,
                name,
                columnIds,
                schemaId,
                databaseId,
                ownerId,
                ownerName,
                tableType,
                query,
                primaryKey,
                placementsByAdapter,
                modifiable,
                numPartitions,
                partitionType,
                partitionIds,
                partitionColumnId,
                isPartitioned,
                relCollation,
                newConnectedViews,
                underlyingTables,
                language );
    }


    @Override
    public CatalogTable getRenamed( String newName ) {
        return new CatalogView(
                id,
                newName,
                columnIds,
                schemaId,
                databaseId,
                ownerId,
                ownerName,
                tableType,
                query,
                primaryKey,
                placementsByAdapter,
                modifiable,
                numPartitions,
                partitionType,
                partitionIds,
                partitionColumnId,
                isPartitioned,
                relCollation,
                connectedViews,
                underlyingTables,
                language );
    }


    @Override
    public CatalogTable getTableWithColumns( ImmutableList<Long> newColumnIds ) {
        return new CatalogView(
                id,
                name,
                newColumnIds,
                schemaId,
                databaseId,
                ownerId,
                ownerName,
                tableType,
                query,
                primaryKey,
                placementsByAdapter,
                modifiable,
                relCollation,
                underlyingTables,
                language );
    }


    public RelNode prepareView( RelOptCluster cluster ) {
        RelNode viewLogicalRoot = getDefinition();
        prepareView( viewLogicalRoot, cluster );

        ViewVisitor materializedVisitor = new ViewVisitor( false );
        viewLogicalRoot.accept( materializedVisitor );

        return viewLogicalRoot;
    }


    public void prepareView( RelNode viewLogicalRoot, RelOptCluster relOptCluster ) {
        if ( viewLogicalRoot instanceof AbstractRelNode ) {
            ((AbstractRelNode) viewLogicalRoot).setCluster( relOptCluster );
        }
        if ( viewLogicalRoot instanceof BiRel ) {
            prepareView( ((BiRel) viewLogicalRoot).getLeft(), relOptCluster );
            prepareView( ((BiRel) viewLogicalRoot).getRight(), relOptCluster );
        } else if ( viewLogicalRoot instanceof SingleRel ) {
            prepareView( ((SingleRel) viewLogicalRoot).getInput(), relOptCluster );
        }
        if ( viewLogicalRoot instanceof LogicalViewTableScan ) {
            prepareView( ((LogicalViewTableScan) viewLogicalRoot).getRelNode(), relOptCluster );
        }
    }

    public RelNode getDefinition() {
        return Catalog.getInstance().getNodeInfo().get( id );
    }


    public enum QueryLanguage {
        SQL, MONGOQL, RELALG
    }

}

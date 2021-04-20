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
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PartitionType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.BiRel;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rel.logical.ViewTableScan;
import org.polypheny.db.rel.type.RelDataType;

public class CatalogView extends CatalogTable {

    private static final long serialVersionUID = -4453089531698670528L;

    @Getter
    private final ImmutableList<Long> underlyingTables;
    @Getter
    private final RelDataType fieldList;


    public CatalogView(
            long id,
            @NonNull String name,
            ImmutableList<Long> columnIds,
            long schemaId,
            long databaseId,
            int ownerId,
            @NonNull String ownerName,
            @NonNull Catalog.TableType type,
            RelRoot definition,
            Long primaryKey,
            @NonNull ImmutableMap<Integer, ImmutableList<Long>> placementsByAdapter,
            boolean modifiable,
            ImmutableList<Long> underlyingTables,
            RelDataType fieldList ) {
        super( id, name, columnIds, schemaId, databaseId, ownerId, ownerName, type, definition, primaryKey, placementsByAdapter, modifiable );
        this.underlyingTables = underlyingTables;
        this.fieldList = fieldList;
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
            RelRoot definition,
            Long primaryKey,
            ImmutableMap<Integer, ImmutableList<Long>> placementsByAdapter,
            boolean modifiable,
            long numPartitions,
            PartitionType partitionType,
            ImmutableList<Long> partitionIds,
            long partitionColumnId,
            boolean isPartitioned,
            ImmutableList<Long> connectedViews,
            ImmutableList<Long> underlyingTables,
            RelDataType fieldList ) {
        super( id, name, columnIds, schemaId, databaseId, ownerId, ownerName, tableType, definition, primaryKey, placementsByAdapter, modifiable, numPartitions, partitionType, partitionIds, partitionColumnId, isPartitioned, connectedViews );
        this.underlyingTables = underlyingTables;
        this.fieldList = fieldList;
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
                definition,
                primaryKey,
                placementsByAdapter,
                modifiable,
                numPartitions,
                partitionType,
                partitionIds,
                partitionColumnId,
                isPartitioned,
                connectedViews,
                underlyingTables,
                fieldList );
    }


    public RelRoot prepareView( RelOptCluster cluster, RelTraitSet traitSet ) {
        RelRoot viewLogicalRoot = definition;
        prepareView( viewLogicalRoot.rel, cluster, traitSet );
        return viewLogicalRoot;
    }


    public static CatalogView generateView( CatalogTable table, ImmutableList<Long> underlyingTables, RelDataType fieldList ) {
        return new CatalogView( table.id, table.name, table.columnIds, table.schemaId, table.databaseId, table.ownerId, table.ownerName, table.tableType, table.definition, table.primaryKey, table.placementsByAdapter, table.modifiable, underlyingTables, fieldList );
    }


    public void prepareView( RelNode viewLogicalRoot, RelOptCluster relOptCluster, RelTraitSet traitSet ) {
        if ( viewLogicalRoot instanceof AbstractRelNode ) {
            ((AbstractRelNode) viewLogicalRoot).setCluster( relOptCluster );
            ((AbstractRelNode) viewLogicalRoot).setTraitSet( traitSet );
        }
        if ( viewLogicalRoot instanceof BiRel ) {
            prepareView( ((BiRel) viewLogicalRoot).getLeft(), relOptCluster, traitSet );
            prepareView( ((BiRel) viewLogicalRoot).getRight(), relOptCluster, traitSet );
        } else if ( viewLogicalRoot instanceof SingleRel ) {
            prepareView( ((SingleRel) viewLogicalRoot).getInput(), relOptCluster, traitSet );
        }
        if ( viewLogicalRoot instanceof ViewTableScan ) {
            prepareView( ((ViewTableScan) viewLogicalRoot).getRelRoot().rel, relOptCluster, traitSet );
        }
    }

}

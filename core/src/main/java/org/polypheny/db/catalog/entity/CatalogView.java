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

package org.polypheny.db.catalog.entity;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.logical.LogicalViewScan;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.view.ViewManager.ViewVisitor;

public class CatalogView extends CatalogTable {

    private static final long serialVersionUID = -4453089531698670528L;

    @Getter
    protected final ImmutableMap<Long, ImmutableList<Long>> underlyingTables;
    @Getter
    private final QueryLanguage language;
    @Getter
    private final AlgCollation algCollation;
    @Getter
    private final String query;


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
            ImmutableList<Integer> dataPlacements,
            boolean modifiable,
            PartitionProperty partitionProperty,
            AlgCollation algCollation,
            ImmutableList<Long> connectedViews,
            ImmutableMap<Long, ImmutableList<Long>> underlyingTables,
            QueryLanguage language ) {
        super( id, name, columnIds, schemaId, databaseId, ownerId, ownerName, tableType, primaryKey, dataPlacements,
                modifiable, partitionProperty, connectedViews );
        this.query = query;
        this.algCollation = algCollation;
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
                dataPlacements,
                modifiable,
                partitionProperty,
                algCollation,
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
                dataPlacements,
                modifiable,
                partitionProperty,
                algCollation,
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
                dataPlacements,
                modifiable,
                partitionProperty,
                algCollation,
                connectedViews,
                underlyingTables,
                language );
    }


    public AlgNode prepareView( AlgOptCluster cluster ) {
        AlgNode viewLogicalRoot = getDefinition();
        prepareView( viewLogicalRoot, cluster );

        ViewVisitor materializedVisitor = new ViewVisitor( false );
        viewLogicalRoot.accept( materializedVisitor );

        return viewLogicalRoot;
    }


    public void prepareView( AlgNode viewLogicalRoot, AlgOptCluster algOptCluster ) {
        if ( viewLogicalRoot instanceof AbstractAlgNode ) {
            ((AbstractAlgNode) viewLogicalRoot).setCluster( algOptCluster );
        }
        if ( viewLogicalRoot instanceof BiAlg ) {
            prepareView( ((BiAlg) viewLogicalRoot).getLeft(), algOptCluster );
            prepareView( ((BiAlg) viewLogicalRoot).getRight(), algOptCluster );
        } else if ( viewLogicalRoot instanceof SingleAlg ) {
            prepareView( ((SingleAlg) viewLogicalRoot).getInput(), algOptCluster );
        }
        if ( viewLogicalRoot instanceof LogicalViewScan ) {
            prepareView( ((LogicalViewScan) viewLogicalRoot).getAlgNode(), algOptCluster );
        }
    }


    public AlgNode getDefinition() {
        return Catalog.getInstance().getNodeInfo().get( id );
    }

}

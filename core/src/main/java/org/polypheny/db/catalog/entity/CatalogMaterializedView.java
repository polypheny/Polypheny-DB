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
import lombok.NonNull;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.partition.properties.PartitionProperty;


public class CatalogMaterializedView extends CatalogView {

    private static final long serialVersionUID = -303234050987260484L;

    @Getter
    private final QueryLanguage language;

    @Getter
    private final AlgCollation algCollation;

    @Getter
    private final String query;

    @Getter
    private final MaterializedCriteria materializedCriteria;

    @Getter
    private final boolean ordered;


    public CatalogMaterializedView(
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
            @NonNull ImmutableList<Integer> dataPlacements,
            boolean modifiable,
            PartitionProperty partitionProperty,
            AlgCollation algCollation,
            ImmutableList<Long> connectedViews,
            ImmutableMap<Long, ImmutableList<Long>> underlyingTables,
            QueryLanguage language,
            MaterializedCriteria materializedCriteria,
            boolean ordered
    ) {
        super(
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
                connectedViews,
                underlyingTables,
                language );
        this.query = query;
        this.algCollation = algCollation;
        this.language = language;
        this.materializedCriteria = materializedCriteria;
        this.ordered = ordered;
    }


    @Override
    public CatalogTable getTableWithColumns( ImmutableList<Long> newColumnIds ) {
        return new CatalogMaterializedView(
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
                language,
                materializedCriteria,
                ordered );
    }


    @Override
    public CatalogTable getConnectedViews( ImmutableList<Long> newConnectedViews ) {
        return new CatalogMaterializedView(
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
                language,
                materializedCriteria,
                ordered );
    }


    @Override
    public CatalogTable getRenamed( String newName ) {
        return new CatalogMaterializedView(
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
                language,
                materializedCriteria,
                ordered );
    }


    @Override
    public AlgNode getDefinition() {
        return Catalog.getInstance().getNodeInfo().get( id );
    }

}

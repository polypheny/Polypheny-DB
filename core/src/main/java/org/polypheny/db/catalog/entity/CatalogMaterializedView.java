/*
 * Copyright 2019-2023 The Polypheny Project
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

import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.Value;
import lombok.With;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.partition.properties.PartitionProperty;

@EqualsAndHashCode(callSuper = true)
@With
@Value
public class CatalogMaterializedView extends CatalogView {

    private static final long serialVersionUID = 4728996184367206274L;

    public String language;

    public AlgCollation algCollation;

    public String query;

    public MaterializedCriteria materializedCriteria;

    public boolean ordered;


    public CatalogMaterializedView(
            long id,
            String name,
            List<LogicalColumn> columns,
            long namespaceId,
            String namespaceName,
            EntityType entityType,
            String query,
            Long primaryKey,
            @NonNull List<Long> dataPlacements,
            boolean modifiable,
            PartitionProperty partitionProperty,
            AlgCollation algCollation,
            List<Long> connectedViews,
            Map<Long, List<Long>> underlyingTables,
            String language,
            MaterializedCriteria materializedCriteria,
            boolean ordered
    ) {
        super(
                id,
                name,
                namespaceName,
                columns,
                namespaceId,
                entityType,
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
    public AlgNode getDefinition() {
        return Catalog.getInstance().getNodeInfo().get( id );
    }


    public QueryLanguage getLanguage() {
        return QueryLanguage.from( language );
    }

}

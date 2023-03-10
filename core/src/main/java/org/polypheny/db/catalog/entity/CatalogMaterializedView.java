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

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.languages.QueryLanguage;

@EqualsAndHashCode(callSuper = true)
@SuperBuilder(toBuilder = true)
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
            long namespaceId,
            EntityType entityType,
            String query,
            Long primaryKey,
            boolean modifiable,
            AlgCollation algCollation,
            String language,
            MaterializedCriteria materializedCriteria,
            boolean ordered
    ) {
        super(
                id,
                name,
                namespaceId,
                entityType,
                query,
                primaryKey,
                modifiable,
                algCollation,
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

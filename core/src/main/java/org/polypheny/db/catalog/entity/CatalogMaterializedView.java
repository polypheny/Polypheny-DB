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
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.type.RelDataType;

public class CatalogMaterializedView extends CatalogTable{

    private static final long serialVersionUID = -303234050987260484L;
    @Getter
    private final Map<Long, List<Long>> underlyingTables;
    @Getter
    private final RelDataType fieldList;
    @Getter
    private final RelCollation relCollation;
    @Getter
    RelNode definition;


    public CatalogMaterializedView(
            long id,
            @NonNull String name,
            ImmutableList<Long> columnIds,
            long schemaId,
            long databaseId,
            int ownerId,
            @NonNull String ownerName,
            @NonNull Catalog.TableType type,
            RelNode definition,
            Long primaryKey,
            @NonNull ImmutableMap<Integer, ImmutableList<Long>> placementsByAdapter,
            boolean modifiable,
            RelCollation relCollation,
            Map<Long, List<Long>> underlyingTables,
            RelDataType fieldList ) {
        super( id, name, columnIds, schemaId, databaseId, ownerId, ownerName, type, primaryKey, placementsByAdapter, modifiable );
        this.definition = definition;
        this.relCollation = relCollation;
        this.underlyingTables = underlyingTables;
        this.fieldList = fieldList;
    }
    
    
    
}

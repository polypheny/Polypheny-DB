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

package org.polypheny.db.webui.models.catalog;

import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;

public class TableModel extends EntityModel {


    public TableModel( @Nullable Long id, @Nullable String name, Long namespaceId, boolean modifiable, NamespaceType namespaceType, EntityType entityType ) {
        super( id, name, namespaceId, modifiable, namespaceType, entityType );
    }


    public static TableModel from( LogicalTable table ) {
        return new TableModel( table.id, table.name, table.namespaceId, table.modifiable, table.namespaceType, table.entityType );
    }

}

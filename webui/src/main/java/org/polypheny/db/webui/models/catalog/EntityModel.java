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

import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;

@AllArgsConstructor
public class EntityModel {

    public final NamespaceType namespaceType;

    @Nullable
    public final Long id;

    @Nullable
    public final String name;

    public final EntityType type;


    public static EntityModel from( LogicalEntity entity ) {
        return new EntityModel( entity.namespaceType, entity.id, entity.name, entity.entityType );
    }


}

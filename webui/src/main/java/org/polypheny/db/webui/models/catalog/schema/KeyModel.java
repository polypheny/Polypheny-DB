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

package org.polypheny.db.webui.models.catalog.schema;

import java.util.List;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.logical.LogicalKey;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.webui.models.catalog.IdEntity;

public class KeyModel extends IdEntity {

    public final long entityId;
    public final long namespaceId;
    public final List<Long> columnIds;
    public final boolean isPrimary;


    public KeyModel( @Nullable Long id, @Nullable String name, long entityId, long namespaceId, List<Long> columnIds, boolean isPrimary ) {
        super( id, name );
        this.entityId = entityId;
        this.namespaceId = namespaceId;
        this.columnIds = columnIds;
        this.isPrimary = isPrimary;
    }


    public static KeyModel from( LogicalKey key ) {
        return new KeyModel( key.id, null, key.tableId, key.namespaceId, key.columnIds, key instanceof LogicalPrimaryKey );
    }

}

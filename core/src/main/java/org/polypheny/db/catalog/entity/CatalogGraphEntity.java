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
import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.partition.properties.PartitionProperty;

@EqualsAndHashCode(callSuper = false)
public class CatalogGraphEntity extends CatalogEntity implements CatalogObject, Serializable {

    static final long serialVersionUID = 3103009888330430956L;


    public CatalogGraphEntity(
            long id,
            @NonNull String name,
            long namespaceId,
            long databaseId,
            int ownerId,
            @NonNull String ownerName,
            @NonNull Catalog.EntityType type,
            Long primaryKey,
            ImmutableList<Long> fieldIds,
            @NonNull ImmutableList<Integer> dataPlacements,
            boolean modifiable,
            PartitionProperty partitionProperty ) {
        super( id, name, fieldIds, namespaceId, databaseId, ownerId, ownerName, type, primaryKey, dataPlacements, modifiable, partitionProperty );
        assert fieldIds.size() == 2 : "GraphObjects can only consist of two fields <id> and the object itself.";
    }


    @Getter
    public enum GraphObjectType {
        NODES( "_nodes" ),
        RELATIONSHIPS( "_rels" );


        private final String name;


        GraphObjectType( String name ) {
            this.name = name;
        }
    }

}

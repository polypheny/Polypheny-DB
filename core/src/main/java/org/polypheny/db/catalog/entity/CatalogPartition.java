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
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;


@EqualsAndHashCode
public class CatalogPartition implements CatalogObject {

    private static final long serialVersionUID = -1124423133579338133L;

    public final long id;

    @Getter
    public final ImmutableList<String> partitionQualifiers;

    // To be checked if even needed
    @Getter
    public final long partitionGroupId;
    public final long tableId;
    public final long schemaId;
    public final long databaseId;
    public final boolean isUnbound;


    public CatalogPartition(
            final long id,
            final long tableId,
            final long schemaId,
            final long databaseId,
            final List<String> partitionQualifiers,
            final boolean isUnbound,
            final long partitionGroupId ) {
        this.id = id;
        this.tableId = tableId;
        this.schemaId = schemaId;
        this.databaseId = databaseId;
        this.partitionQualifiers = ImmutableList.copyOf( partitionQualifiers );
        this.isUnbound = isUnbound;
        this.partitionGroupId = partitionGroupId;
    }


    @Override
    public Serializable[] getParameterArray() {
        throw new RuntimeException( "Not implemented" );
    }

}

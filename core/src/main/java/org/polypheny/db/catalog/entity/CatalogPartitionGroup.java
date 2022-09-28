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
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;


@EqualsAndHashCode
public final class CatalogPartitionGroup implements CatalogObject {

    private static final long serialVersionUID = 6229244317971622972L;

    public final long id;
    public final String partitionGroupName;
    public final long tableId;
    public final long schemaId;
    public final long databaseId;
    public final ImmutableList<String> partitionQualifiers;
    public final ImmutableList<Long> partitionIds;
    public final boolean isUnbound;

    public final long partitionKey;


    public CatalogPartitionGroup(
            final long id,
            final String partitionGroupName,
            final long tableId,
            final long schemaId,
            final long databaseId,
            final long partitionKey,
            final List<String> partitionQualifiers,
            final List<Long> partitionIds,
            final boolean isUnbound ) {
        this.id = id;
        this.partitionGroupName = partitionGroupName;
        this.tableId = tableId;
        this.schemaId = schemaId;
        this.databaseId = databaseId;
        this.partitionKey = partitionKey;
        // TODO @HENNLO Although the qualifiers are now part of CatalogPartitions, it might be a good improvement to
        //  accumulate all qualifiers of all internal partitions here to speed up query time.
        if ( partitionQualifiers != null ) {
            this.partitionQualifiers = ImmutableList.copyOf( partitionQualifiers );
        } else {
            this.partitionQualifiers = null;
        }
        this.partitionIds = ImmutableList.copyOf( partitionIds );
        this.isUnbound = isUnbound;
    }


    @SneakyThrows
    public String getTableName() {
        return Catalog.getInstance().getTable( tableId ).name;
    }


    @SneakyThrows
    public String getDatabaseName() {
        return Catalog.getInstance().getDatabase( databaseId ).name;
    }


    @SneakyThrows
    public String getSchemaName() {
        return Catalog.getInstance().getSchema( schemaId ).name;
    }


    @Override
    public Serializable[] getParameterArray() {
        throw new RuntimeException( "Not implemented" );
    }

}

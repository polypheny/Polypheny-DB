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

import java.io.Serializable;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;


@EqualsAndHashCode
public final class CatalogPartitionGroup implements CatalogEntity {

    private static final long serialVersionUID = 2312903632511266177L;

    public final long id;
    public final String partitionGroupName;

    public final long tableId;
    public final long schemaId;
    public final long databaseId;
    public final List<String> partitionQualifiers;
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
            final boolean isUnbound ) {
        this.id = id;
        this.partitionGroupName = partitionGroupName;
        this.tableId = tableId;
        this.schemaId = schemaId;
        this.databaseId = databaseId;
        this.partitionKey = partitionKey;
        this.partitionQualifiers = partitionQualifiers;
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

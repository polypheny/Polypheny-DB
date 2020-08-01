/*
 * Copyright 2019-2020 The Polypheny Project
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
import lombok.NonNull;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;

import java.io.Serializable;

@EqualsAndHashCode
public final class CatalogPartition implements CatalogEntity, Comparable<CatalogTable> {


    private static final long serialVersionUID = 2312903632511266177L;

    public final long id;
    public final long tableId;
    public final long schemaId;
    public final long databaseId;
    public final int ownerId;
    public final String ownerName;
    public final long partitionKey;



    public CatalogPartition(final long id,
                            final long tableId,
                            final long schemaId,
                            final long databaseId,
                            final int ownerId,
                            @NonNull final String ownerName,
                            final long partitionKey) {

        this.id = id;
        this.tableId = tableId;
        this.schemaId = schemaId;
        this.databaseId = databaseId;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.partitionKey = partitionKey;

        System.out.println("HENNLO: Partiton has been created: " + id);
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
    public int compareTo(CatalogTable catalogTable) {
        //TODO: To be implemented
        return 0;
    }

    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }
}

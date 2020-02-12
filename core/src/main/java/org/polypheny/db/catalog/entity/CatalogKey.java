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


import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NonNull;


/**
 *
 */
@EqualsAndHashCode
public class CatalogKey {

    public final long id;
    public final long tableId;
    public final String tableName;
    public final long schemaId;
    public final String schemaName;
    public final long databaseId;
    public final String databaseName;
    public List<Long> columnIds;
    public List<String> columnNames;


    public CatalogKey(
            final long id,
            final long tableId,
            @NonNull final String tableName,
            final long schemaId,
            @NonNull final String schemaName,
            final long databaseId,
            @NonNull final String databaseName,
            final List<Long> columnIds,
            final List<String> columnNames ) {
        this.id = id;
        this.tableId = tableId;
        this.tableName = tableName;
        this.schemaId = schemaId;
        this.schemaName = schemaName;
        this.databaseId = databaseId;
        this.databaseName = databaseName;
        this.columnIds = columnIds;
        this.columnNames = columnNames;
    }


    public CatalogKey(
            final long id,
            final long tableId,
            @NonNull final String tableName,
            final long schemaId,
            @NonNull final String schemaName,
            final long databaseId,
            @NonNull final String databaseName ) {
        this( id, tableId, tableName, schemaId, schemaName, databaseId, databaseName, null, null );
    }

}

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

import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Event;

import java.io.Serializable;

@EqualsAndHashCode
public final class CatalogTrigger implements CatalogEntity {

    private static final long serialVersionUID = -4752365450652498995L;
    private final Long schemaId;
    private final String name;
    private final Event event;
    private final long databaseId;
    
    private final long triggerId;
    private final String query;

    public CatalogTrigger(Long schemaId, String name, Long databaseId, Long triggerId, Event event, String query, final String... arguments) {
        this.name = name;
        this.schemaId = schemaId;
        this.databaseId = databaseId;
        this.event = event;
        this.query = query;
        this.triggerId = triggerId;
    }

    @Override
    public Serializable[] getParameterArray() {
        // throw UOE because should not be used.
        throw new UnsupportedOperationException();
    }

    @SneakyThrows
    public String getDatabaseName() {
        return Catalog.getInstance().getDatabase(databaseId).name;
    }

    @SneakyThrows
    public String getSchemaName() {
        return Catalog.getInstance().getSchema(schemaId).name;
    }


    @SneakyThrows
    public Catalog.SchemaType getSchemaType() {
        return Catalog.getInstance().getSchema(schemaId).schemaType;
    }

    public String getName() {
        return name;
    }

    public String getQuery() {
        return query;
    }

    public long getTriggerId() {
        return triggerId;
    }
}

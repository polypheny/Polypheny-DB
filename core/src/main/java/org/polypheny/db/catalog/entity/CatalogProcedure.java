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

import com.google.common.collect.Lists;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.util.Pair;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

@EqualsAndHashCode
public final class CatalogProcedure implements CatalogEntity {

    private static final long serialVersionUID = 1781666800808312001L;
    private final String name;
    private final Long schemaId;
    private final long databaseId;
    private final Long procedureId;
    private final ProcedureArguments arguments;
    private final String query;

    public CatalogProcedure(Long schemaId, String name, Long databaseId, Long procedureId, String query, List<Pair<String, Object>> arguments) {
        this.name = name;
        this.schemaId = schemaId;
        this.databaseId = databaseId;
        this.procedureId = procedureId;
        this.arguments = new ProcedureArguments(Collections.unmodifiableList(Lists.newArrayList(arguments)));
        this.query = query;
    }

    @Override
    public Serializable[] getParameterArray() {
        // throw UOE because should not be used.
        throw new UnsupportedOperationException();
    }

    @SneakyThrows
    public String getDatabaseName() {
        return Catalog.getInstance().getDatabase( databaseId ).name;
    }

    @SneakyThrows
    public String getSchemaName() {
        return Catalog.getInstance().getSchema( schemaId ).name;
    }


    @SneakyThrows
    public Catalog.SchemaType getSchemaType() {
        return Catalog.getInstance().getSchema( schemaId ).schemaType;
    }

    public String getName() {
        return name;
    }

    public String getQuery() {
        return query;
    }

    public Long getProcedureId() {
        return procedureId;
    }

    static class ProcedureArguments implements CatalogEntity{
        private final List<Pair<String, Object>> arguments;

        ProcedureArguments(List<Pair<String, Object>> arguments) {
            this.arguments = arguments;
        }

        @Override
        public Serializable[] getParameterArray() {
            return arguments.toArray(Serializable[]::new);
        }
    }

}

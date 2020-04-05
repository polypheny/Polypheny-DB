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


import java.io.Serializable;
import java.util.Comparator;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


/**
 *
 */
@EqualsAndHashCode
public final class CatalogDatabase implements CatalogEntity, Comparable<CatalogDatabase> {

    private static final long serialVersionUID = 4711611630126858410L;

    public final long id;
    public final String name;
    public final int ownerId;
    public final String ownerName;
    public final Long defaultSchemaId; // can be null
    public final String defaultSchemaName; // can be null


    public CatalogDatabase(
            final long id,
            @NonNull final String name,
            final int ownerId,
            @NonNull final String ownerName,
            final Long defaultSchemaId,
            final String defaultSchemaName ) {
        this.id = id;
        this.name = name;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.defaultSchemaId = defaultSchemaId;
        this.defaultSchemaName = defaultSchemaName;
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{ name, ownerName, defaultSchemaName };
    }


    @Override
    public int compareTo( CatalogDatabase o ) {
        if ( o != null ) {
            return (int) (this.id - o.id);
        }
        return -1;
    }


    @RequiredArgsConstructor
    public static class PrimitiveCatalogDatabase {

        public final String tableCat;
        public final String owner;
        public final String defaultSchema;
    }
}

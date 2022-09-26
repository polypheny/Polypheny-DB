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

import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;


@EqualsAndHashCode
public final class CatalogDatabase implements CatalogObject, Comparable<CatalogDatabase> {

    private static final long serialVersionUID = -4529369849606480011L;

    public final long id;
    public final String name;
    public final int ownerId;
    public final String ownerName;
    public final Long defaultNamespaceId; // can be null
    public final String defaultNamespaceName; // can be null


    public CatalogDatabase(
            final long id,
            @NonNull final String name,
            final int ownerId,
            @NonNull final String ownerName,
            final Long defaultNamespaceId,
            final String defaultNamespaceName ) {
        this.id = id;
        this.name = name;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.defaultNamespaceId = defaultNamespaceId;
        this.defaultNamespaceName = defaultNamespaceName;
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{ name, ownerName, defaultNamespaceName };
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

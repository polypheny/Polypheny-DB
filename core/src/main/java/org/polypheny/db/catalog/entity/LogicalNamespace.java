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


import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.With;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.logistic.NamespaceType;


@EqualsAndHashCode(callSuper = false)
@With
@Value
public class LogicalNamespace extends CatalogNamespace implements CatalogObject, Comparable<LogicalNamespace> {

    private static final long serialVersionUID = 3090632164988970558L;

    @Serialize
    public long id;
    @Serialize
    @Getter
    public String name;
    @Serialize
    @Getter
    @EqualsAndHashCode.Exclude
    public NamespaceType namespaceType;
    @Serialize
    public boolean caseSensitive;


    public LogicalNamespace(
            @Deserialize("id") final long id,
            @Deserialize("name") @NonNull final String name,
            @Deserialize("namespaceType") @NonNull final NamespaceType namespaceType,
            @Deserialize("caseSensitive") boolean caseSensitive ) {
        super( id, name, namespaceType );
        this.id = id;
        this.name = name;
        this.namespaceType = namespaceType;
        this.caseSensitive = caseSensitive;
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{ name, Catalog.DATABASE_NAME, Catalog.USER_NAME, CatalogObject.getEnumNameOrNull( namespaceType ) };
    }


    @Override
    public int compareTo( LogicalNamespace o ) {
        if ( o != null ) {
            return (int) (this.id - o.id);
        }

        return -1;
    }


    @RequiredArgsConstructor
    public static class PrimitiveCatalogSchema {

        public final String tableSchem;
        public final String tableCatalog;
        public final String schemaType;

    }

}

/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.catalog.graph;

import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import org.polypheny.db.catalog.Catalog.NamespaceType;

public class CatalogGraph {

    @Serialize
    public final long id;

    @Serialize
    public final String name;

    @Serialize
    public final long databaseId;

    @Serialize
    public final NamespaceType namespaceType;


    public CatalogGraph(
            @Deserialize("id") long id,
            @Deserialize("name") String name,
            @Deserialize("databaseId") long databaseId,
            @Deserialize("namespaceType") NamespaceType namespaceType ) {
        this.id = id;
        this.name = name;
        this.databaseId = databaseId;
        this.namespaceType = namespaceType;
    }

}

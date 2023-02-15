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

import io.activej.serializer.BinarySerializer;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.ModelCatalog;
import org.polypheny.db.catalog.SerializableCatalog;

public class GraphCatalog implements ModelCatalog, SerializableCatalog {

    public List<CatalogGraph> graphs = new ArrayList<>();

    @Getter
    BinarySerializer<GraphCatalog> serializer = SerializableCatalog.builder.get().build( GraphCatalog.class );

    private boolean openChanges = false;


    @Override
    public void commit() {

        openChanges = false;
    }


    @Override
    public void rollback() {

        openChanges = false;
    }


    @Override
    public boolean hasUncommittedChanges() {
        return openChanges;
    }


    public void addGraph( long id, String name, long databaseId, NamespaceType namespaceType ) {
    }


    public void addSubstitutionGraph( long id, String name, long namespaceId, NamespaceType document ) {
    }

}

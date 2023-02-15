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

package org.polypheny.db.catalog.document;

import io.activej.serializer.BinarySerializer;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.ModelCatalog;
import org.polypheny.db.catalog.SerializableCatalog;

public class DocumentCatalog implements ModelCatalog, SerializableCatalog {

    Map<Long, CatalogDocDatabase> databases = new HashMap<>();
    Map<Long, CatalogCollection> collections = new HashMap<>();

    @Getter
    BinarySerializer<DocumentCatalog> serializer = SerializableCatalog.builder.get().build( DocumentCatalog.class );

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
        return false;
    }


    public void addDatabase( long id, String name, long databaseId, NamespaceType namespaceType ) {
    }


    public void addCollection( long id, String name, long namespaceId ) {

    }


    public void addSubstitutionCollection( long id, String name, long namespaceId, NamespaceType relational ) {
    }

}

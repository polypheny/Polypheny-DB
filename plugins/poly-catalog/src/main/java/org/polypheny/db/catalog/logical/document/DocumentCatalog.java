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

package org.polypheny.db.catalog.logical.document;

import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.NCatalog;
import org.polypheny.db.catalog.Serializable;

@Value
public class DocumentCatalog implements NCatalog, Serializable {

    @Getter
    public BinarySerializer<DocumentCatalog> serializer = Serializable.builder.get().build( DocumentCatalog.class );

    @Serialize
    public Map<Long, CatalogCollection> collections;

    @Serialize
    public String name;

    @Serialize
    public long id;


    public DocumentCatalog( long id, String name ) {
        this( id, name, new ConcurrentHashMap<>() );
    }


    public DocumentCatalog(
            @Deserialize("id") long id,
            @Deserialize("name") String name,
            @Deserialize("collections") Map<Long, CatalogCollection> collections ) {
        this.collections = collections;
        this.id = id;
        this.name = name;
    }


    @NonFinal
    boolean openChanges = false;


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


    @Override
    public NamespaceType getType() {
        return NamespaceType.DOCUMENT;
    }


    public void addCollection( long id, String name, long namespaceId ) {

    }


    @Override
    public DocumentCatalog copy() {
        return deserialize( serialize(), DocumentCatalog.class );
    }

}

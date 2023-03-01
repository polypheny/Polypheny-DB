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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Value;
import lombok.With;
import lombok.experimental.NonFinal;
import org.polypheny.db.catalog.IdBuilder;
import org.polypheny.db.catalog.NCatalog;
import org.polypheny.db.catalog.Serializable;
import org.polypheny.db.catalog.catalogs.LogicalDocumentCatalog;
import org.polypheny.db.catalog.entity.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.catalog.logistic.Pattern;

@Value
@With
public class DocumentCatalog implements NCatalog, Serializable, LogicalDocumentCatalog {

    @Getter
    public BinarySerializer<DocumentCatalog> serializer = Serializable.builder.get().build( DocumentCatalog.class );

    @Serialize
    public IdBuilder idBuilder;
    @Serialize
    public Map<Long, LogicalCollection> collections;
    @Getter
    @Serialize
    public LogicalNamespace logicalNamespace;


    public DocumentCatalog( LogicalNamespace logicalNamespace, IdBuilder idBuilder ) {
        this( logicalNamespace, idBuilder, new ConcurrentHashMap<>() );
    }


    public DocumentCatalog(
            @Deserialize("logicalNamespace") LogicalNamespace logicalNamespace,
            @Deserialize("idBuilder") IdBuilder idBuilder,
            @Deserialize("collections") Map<Long, LogicalCollection> collections ) {
        this.logicalNamespace = logicalNamespace;
        this.collections = collections;

        this.idBuilder = idBuilder;
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


    @Override
    public boolean checkIfExistsEntity( long namespaceId, String entityName ) {
        return false;
    }


    @Override
    public boolean checkIfExistsEntity( long tableId ) {
        return false;
    }


    @Override
    public LogicalCollection getCollection( long collectionId ) {
        return null;
    }


    @Override
    public List<LogicalCollection> getCollections( long namespaceId, Pattern namePattern ) {
        return null;
    }


    @Override
    public long addCollection( Long id, String name, long schemaId, int currentUserId, EntityType entity, boolean modifiable ) {
        return 0;
    }


    @Override
    public void deleteCollection( long id ) {

    }

}

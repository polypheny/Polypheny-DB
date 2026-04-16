/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.schema.document;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.util.Optional;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.LogicalDocumentCatalog;
import org.polypheny.db.type.PolySerializable;

/**
 * Stored schema metadata for a document collection.
 */
public class SchemaMeta implements PolySerializable {

    @Serialize
    @JsonProperty
    public String schemaJson;

    @Serialize
    @JsonProperty
    public String enforcement;

    @Serialize
    @JsonProperty
    public long version;

    private final BinarySerializer<SchemaMeta> serializer = PolySerializable.buildSerializer( SchemaMeta.class );


    public SchemaMeta( @Deserialize("schemaJson") String schemaJson, @Deserialize("enforcement") String enforcement, @Deserialize("version") long version ) {
        this.schemaJson = schemaJson;
        this.enforcement = enforcement;
        this.version = version;
    }


    @Override
    public BinarySerializer<SchemaMeta> getSerializer() {
        return serializer;
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), SchemaMeta.class );
    }


    public static Optional<SchemaMeta> readCurrent( Catalog catalog, long namespaceId, long collectionId ) {
        LogicalDocumentCatalog logicalDocumentCatalog = catalog.getLogicalDoc( namespaceId );
        return logicalDocumentCatalog.getCollectionSchema( collectionId );
    }


    public static void writeCurrent( Catalog catalog, long namespaceId, long collectionId, SchemaMeta meta ) {
        LogicalDocumentCatalog logicalDocumentCatalog = catalog.getLogicalDoc( namespaceId );
        logicalDocumentCatalog.upsertCollectionSchema( collectionId, meta.schemaJson, meta.enforcement );
        catalog.change();
    }


    public static void clear( Catalog catalog, long namespaceId, long collectionId ) {
        LogicalDocumentCatalog logicalDocumentCatalog = catalog.getLogicalDoc( namespaceId );
        logicalDocumentCatalog.dropCollectionSchema( collectionId );
        catalog.change();
    }

}
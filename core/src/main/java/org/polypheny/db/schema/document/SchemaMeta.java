/*
 * Copyright 2019-2025 The Polypheny Project
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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.LogicalDocumentCatalog;
import org.polypheny.db.catalog.impl.logical.DocumentCatalog;
import org.polypheny.db.schema.Path;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Persisted schema metadata for a document collection.
 * Holds the canonical schema JSON, the enforcement mode, and a version number.
 * Provides a serializer and convenience methods to read, write, and clear
 * the stored metadata via the logical document catalog.
 */
public class SchemaMeta implements PolySerializable {

    // persisted fields
    @Serialize
    @JsonProperty
    public String schemaJson;       // canonical {"root":{...},"additionalProperties":"..."}
    @Serialize
    @JsonProperty
    public String enforcement;      // "OFF" | "WARN" | "STRICT"
    @Serialize
    @JsonProperty
    public long version;          // informational


    /**
     * Creates a new {@code SchemaMeta}.
     *
     * @param schemaJson canonical schema JSON
     * @param enforcement enforcement mode as string
     * @param version informational version number
     */
    public SchemaMeta(
            @Deserialize("schemaJson") final String schemaJson,
            @Deserialize("enforcement") final String enforcement,
            @Deserialize("version") final long version
    ) {
        this.schemaJson = schemaJson;
        this.enforcement = enforcement;
        this.version = version;
    }


    /**
     * Serializer instance for binary (de)serialization of SchemaMeta.
     */
    private final BinarySerializer<SchemaMeta> serializer =
            PolySerializable.buildSerializer( SchemaMeta.class );


    /**
     * Returns the serializer for this type.
     *
     * @return binary serializer capable of reading/writing {@code SchemaMeta}
     */
    @Override
    public BinarySerializer<SchemaMeta> getSerializer() {
        return serializer;
    }


    /**
     * Produces a deep copy by serializing and deserializing this instance.
     *
     * @return a new {@code SchemaMeta} instance with identical content
     */
    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), SchemaMeta.class );
    }


    /**
     * Reads the current schema metadata of a collection from the logical document catalog.
     *
     * @param catalog catalog entry point
     * @param namespaceId namespace identifier
     * @param collectionId collection identifier
     * @return optional containing the current {@code SchemaMeta} if present, empty otherwise
     */
    public static Optional<SchemaMeta> readCurrent(
            final Catalog catalog, final long namespaceId, final long collectionId ) {
        LogicalDocumentCatalog ldc = catalog.getLogicalDoc( namespaceId );
        return ldc.getCollectionSchema( collectionId );
    }


    /**
     * Writes (creates or updates) the current schema metadata of a collection into the logical document catalog.
     * Also marks the catalog as changed so that the update is persisted on commit.
     *
     * @param catalog catalog entry point
     * @param namespaceId namespace identifier
     * @param collectionId collection identifier
     * @param meta schema metadata to store
     */
    public static void writeCurrent(
            final Catalog catalog, final long namespaceId, final long collectionId, final SchemaMeta meta ) {
        LogicalDocumentCatalog ldc = catalog.getLogicalDoc( namespaceId );
        ldc.upsertCollectionSchema( collectionId, meta.schemaJson, meta.enforcement );
        catalog.change();
    }


    /**
     * Removes the current schema metadata of a collection from the logical document catalog.
     * Also marks the catalog as changed so that the deletion is persisted on commit.
     *
     * @param catalog catalog entry point
     * @param namespaceId namespace identifier
     * @param collectionId collection identifier
     */
    public static void clear(
            final Catalog catalog, final long namespaceId, final long collectionId ) {
        LogicalDocumentCatalog ldc = catalog.getLogicalDoc( namespaceId );
        ldc.dropCollectionSchema( collectionId );
        catalog.change();
    }

}

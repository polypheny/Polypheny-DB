/*
 * Copyright 2019-2025 The Polypheny Project
 * Licensed under the Apache License, Version 2.0
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
 */
/**
 * Persisted schema metadata for a document collection.
 * Storage is delegated to the DocumentCatalog of the collection's namespace.
 */
public class SchemaMeta implements PolySerializable {

    // ---------- persisted fields ----------
    @Serialize @JsonProperty public String schemaJson;       // canonical {"root":{...},"additionalProperties":"..."}
    @Serialize @JsonProperty public String enforcement;      // "OFF" | "WARN" | "STRICT"
    @Serialize @JsonProperty public long   version;          // informational
    @Serialize @JsonProperty public long   updatedAtEpochMs; // ms epoch

    public SchemaMeta(
            @Deserialize("schemaJson")       final String schemaJson,
            @Deserialize("enforcement")      final String enforcement,
            @Deserialize("version")          final long version,
            @Deserialize("updatedAtEpochMs") final long updatedAtEpochMs
    ) {
        this.schemaJson       = schemaJson;
        this.enforcement      = enforcement;
        this.version          = version;
        this.updatedAtEpochMs = updatedAtEpochMs;
    }

    private final BinarySerializer<SchemaMeta> serializer =
            PolySerializable.buildSerializer(SchemaMeta.class);

    @Override
    public BinarySerializer<SchemaMeta> getSerializer() {
        return serializer;
    }

    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize(serialize(), SchemaMeta.class);
    }

    public Optional<EnforcementMode> enforcementMode() {
        try {
            return Optional.of(EnforcementMode.valueOf(enforcement));
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    // ---------- Catalog-backed accessors (interface-only; no impl casts) ----------

    public static Optional<SchemaMeta> readCurrent(
            final Catalog catalog, final long namespaceId, final long collectionId) {
        LogicalDocumentCatalog ldc = catalog.getLogicalDoc(namespaceId);
        return ldc.getCollectionSchema(collectionId);
    }

    public static void writeCurrent(
            final Catalog catalog, final long namespaceId, final long collectionId, final SchemaMeta meta) {
        LogicalDocumentCatalog ldc = catalog.getLogicalDoc(namespaceId);
        ldc.upsertCollectionSchema(collectionId, meta.schemaJson, meta.enforcement);
        // mark catalog dirty so commit persists
        catalog.change();
    }

    public static void clear(
            final Catalog catalog, final long namespaceId, final long collectionId) {
        LogicalDocumentCatalog ldc = catalog.getLogicalDoc(namespaceId);
        ldc.dropCollectionSchema(collectionId);
        catalog.change();
    }
}

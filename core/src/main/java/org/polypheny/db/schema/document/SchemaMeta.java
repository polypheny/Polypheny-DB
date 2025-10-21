/*
 * Copyright 2019-2025 The Polypheny Project
 * Licensed under the Apache License, Version 2.0
 */

package org.polypheny.db.schema.document;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.type.PolySerializable;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Persisted schema metadata for a document collection.
 */
public class SchemaMeta implements PolySerializable {

    // ---------- persisted fields ----------
    @Serialize @JsonProperty public String schemaJson;       // canonical JSON for DocumentSchema
    @Serialize @JsonProperty public String enforcement;      // "OFF" | "WARN" | "STRICT"
    @Serialize @JsonProperty public long   version;          // increment on every upsert
    @Serialize @JsonProperty public long   updatedAtEpochMs; // audit timestamp (ms since epoch)

    // ---------- ctor / serialization ----------
    public SchemaMeta(
            @Deserialize("schemaJson")      final String schemaJson,
            @Deserialize("enforcement")     final String enforcement,
            @Deserialize("version")         final long version,
            @Deserialize("updatedAtEpochMs")final long updatedAtEpochMs
    ) {
        this.schemaJson = schemaJson;
        this.enforcement = enforcement;
        this.version = version;
        this.updatedAtEpochMs = updatedAtEpochMs;
    }

    // Build once and return via getSerializer()
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

    // ---------- convenience accessors (do not introduce new deps) ----------

    /** Raw canonical JSON for the document schema. */
    public String schemaJson() {
        return schemaJson;
    }

    /** Current enforcement mode string ("OFF" | "WARN" | "STRICT"). */
    public String enforcementString() {
        return enforcement;
    }

    /** Map the stored enforcement string to the enum, if available. */
    public Optional<EnforcementMode> enforcementMode() {
        try {
            return Optional.of(EnforcementMode.valueOf(enforcement));
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    // ---------- minimal in-memory registry (temporary persistence) ----------

    /**
     * Simple in-memory registry keyed by logical collection id.
     * This is a temporary mechanism to unblock callers; replace with
     * catalog-backed storage when ready.
     */
    private static final ConcurrentMap<Long, SchemaMeta> REGISTRY = new ConcurrentHashMap<>();

    /**
     * Read the current SchemaMeta for a collection.
     * The Catalog parameter is accepted for future catalog-backed wiring.
     */
    public static Optional<SchemaMeta> readCurrent(final Catalog catalog, final long collectionId) {
        return Optional.ofNullable(REGISTRY.get(collectionId));
    }

    /**
     * Upsert the current SchemaMeta for a collection.
     * Call this from your CREATE COLLECTION and ALTER COLLECTION SCHEMA paths.
     */
    public static void writeCurrent(final Catalog catalog, final long collectionId, final SchemaMeta meta) {
        REGISTRY.put(collectionId, meta);
    }

    /**
     * Clear any in-memory entry for the given collection.
     */
    public static void clear(final long collectionId) {
        REGISTRY.remove(collectionId);
    }
}

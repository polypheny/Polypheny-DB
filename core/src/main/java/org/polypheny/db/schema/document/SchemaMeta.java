/*
 * Copyright 2019-2025 The Polypheny Project
 * Licensed under the Apache License, Version 2.0
 */

package org.polypheny.db.schema.document;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.activej.serializer.BinarySerializer;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import org.polypheny.db.type.PolySerializable;

/**
 * Persisted schema metadata for a document collection.
 */
public class SchemaMeta implements PolySerializable {

    // Build once and return via getSerializer()
    private final BinarySerializer<SchemaMeta> serializer =
            PolySerializable.buildSerializer(SchemaMeta.class);

    @Serialize @JsonProperty public String schemaJson;       // canonical JSON for DocumentSchema
    @Serialize @JsonProperty public String enforcement;      // "OFF" | "WARN" | "STRICT"
    @Serialize @JsonProperty public long   version;          // increment on every upsert
    @Serialize @JsonProperty public long   updatedAtEpochMs; // audit timestamp (ms since epoch)

        public SchemaMeta(
            @Deserialize("schemaJson") String schemaJson,
            @Deserialize("enforcement") String enforcement,
            @Deserialize("version") long version,
            @Deserialize("updatedAtEpochMs") long updatedAtEpochMs
    ) {
        this.schemaJson = schemaJson;
        this.enforcement = enforcement;
        this.version = version;
        this.updatedAtEpochMs = updatedAtEpochMs;
    }

    @Override
    public BinarySerializer<SchemaMeta> getSerializer() {
        return serializer;
    }

    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize(serialize(), SchemaMeta.class);
    }
}

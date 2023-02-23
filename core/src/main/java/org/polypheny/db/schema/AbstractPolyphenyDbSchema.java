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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.schema;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogEntityPlacement;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Triple;


/**
 * Schema.
 *
 * Wrapper around user-defined schema used internally.
 */
@Getter
public abstract class AbstractPolyphenyDbSchema implements PolyphenyDbSchema {


    private final ConcurrentHashMap<Pair<Long, Long>, CatalogEntity> logicalRelational;
    private final ConcurrentHashMap<Pair<Long, Long>, CatalogEntity> logicalDocument;
    private final ConcurrentHashMap<Pair<Long, Long>, CatalogEntity> logicalGraph;
    private final ConcurrentHashMap<Triple<Long, Long, Long>, CatalogEntityPlacement> physicalRelational;
    private final ConcurrentHashMap<Triple<Long, Long, Long>, CatalogEntityPlacement> physicalDocument;
    private final ConcurrentHashMap<Triple<Long, Long, Long>, CatalogEntityPlacement> physicalGraph;


    public AbstractPolyphenyDbSchema(
            Map<Pair<Long, Long>, CatalogEntity> logicalRelational,
            Map<Pair<Long, Long>, CatalogEntity> logicalDocument,
            Map<Pair<Long, Long>, CatalogEntity> logicalGraph,
            Map<Triple<Long, Long, Long>, CatalogEntityPlacement> physicalRelational,
            Map<Triple<Long, Long, Long>, CatalogEntityPlacement> physicalDocument,
            Map<Triple<Long, Long, Long>, CatalogEntityPlacement> physicalGraph ) {
        this.logicalRelational = new ConcurrentHashMap<>( logicalRelational );
        this.logicalDocument = new ConcurrentHashMap<>( logicalDocument );
        this.logicalGraph = new ConcurrentHashMap<>( logicalGraph );
        this.physicalRelational = new ConcurrentHashMap<>( physicalRelational );
        this.physicalDocument = new ConcurrentHashMap<>( physicalDocument );
        this.physicalGraph = new ConcurrentHashMap<>( physicalGraph );

    }


    /**
     * Creates a root schema.
     */
    public static PolyphenyDbSchema createRootSchema() {
        return PolySchemaBuilder.getInstance().getCurrent();
    }


    /**
     * Returns a snapshot representation of this PolyphenyDbSchema.
     */
    protected abstract PolyphenyDbSchema snapshot( AbstractPolyphenyDbSchema parent, SchemaVersion version );

    protected abstract boolean isCacheEnabled();


}

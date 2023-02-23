/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.schema;


import java.util.Map;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.entity.CatalogEntityPlacement;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Triple;


/**
 * A concrete implementation of {@link AbstractPolyphenyDbSchema} that maintains minimal state.
 */
class SimplePolyphenyDbSchema extends AbstractPolyphenyDbSchema {

    /**
     * Creates a SimplePolyphenyDbSchema.
     */
    public SimplePolyphenyDbSchema(
            Map<Pair<Long, Long>, CatalogEntity> logicalRelational,
            Map<Pair<Long, Long>, CatalogEntity> logicalDocument,
            Map<Pair<Long, Long>, CatalogEntity> logicalGraph,
            Map<Triple<Long, Long, Long>, CatalogEntityPlacement> physicalRelational,
            Map<Triple<Long, Long, Long>, CatalogEntityPlacement> physicalDocument,
            Map<Triple<Long, Long, Long>, CatalogEntityPlacement> physicalGraph ) {
        super( logicalRelational, logicalDocument, logicalGraph, physicalRelational, physicalDocument, physicalGraph );

    }


    @Override
    protected PolyphenyDbSchema snapshot( AbstractPolyphenyDbSchema parent, SchemaVersion version ) {

        return null;
    }


    @Override
    protected boolean isCacheEnabled() {
        return false;
    }

}


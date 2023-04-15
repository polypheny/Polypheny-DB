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

package org.polypheny.db.polyfier.core.client.profile;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

@Getter
@Builder
public class Profile implements Serializable {
    /**
     * Describes the schema that is either generated or preset on this PolyphenyDB Instance.
     */
    private final SchemaConfig schemaConfig;

    /**
     * Describes how the data which the PolyphenyDB Instance queries is generated.
     */
    private final DataConfig dataConfig;

    /**
     * Describes how the queries the PolyphenyDB instance executes are generated.
     */
    private final QueryConfig queryConfig;

    /**
     * Describes which Datastores the PolyphenyDB instance uses.
     */
    private final StoreConfig storeConfig;

    /**
     * Describes what kind of PolyphenyDB instance is started.
     */
    private final StartConfig startConfig;

    /**
     * Describes how the data is partitioned in the PolyphenyDB instance.
     */
    private final PartitionConfig partitionConfig;

    /**
     * Lists what seeds are issued to the PolyphenyDB for generating queries.
     */
    private final SeedsConfig issuedSeeds;

}

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

package org.polypheny.db.catalog.snapshot;

import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.logistic.Pattern;

public interface LogicalDocSnapshot {

    /**
     * Get the collection with the given id.
     *
     * @param id The id of the collection
     * @return The requested collection
     */
    @NonNull Optional<LogicalCollection> getCollection( long id );

    @NonNull Optional<LogicalCollection> getCollection( long namespaceId, String name );

    /**
     * Get a collection of collections which match the given naming pattern.
     *
     * @param namePattern The naming pattern of the collection itself, null if all are matched
     * @return collection of collections matching conditions
     */
    @NonNull List<LogicalCollection> getCollections( long namespaceId, @Nullable Pattern namePattern );

    @NonNull List<LogicalCollection> getCollections(@Nullable Pattern namespacePattern,@Nullable Pattern namePattern );


}

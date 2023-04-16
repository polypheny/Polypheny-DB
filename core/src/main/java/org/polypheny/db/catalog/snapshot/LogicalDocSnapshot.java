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

package org.polypheny.db.catalog.snapshot;

import java.util.List;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.logistic.Pattern;

public interface LogicalDocSnapshot {
    //// DOCUMENT

    /**
     * Get the collection with the given id.
     *
     * @param collectionId The id of the collection
     * @return The requested collection
     */
    LogicalCollection getCollection( long collectionId );

    /**
     * Get a collection of collections which match the given naming pattern.
     *
     * @param namespaceId
     * @param namePattern The naming pattern of the collection itself, null if all are matched
     * @return collection of collections matching conditions
     */
    List<LogicalCollection> getCollections( long namespaceId, Pattern namePattern );


    @Deprecated
    LogicalCollection getLogicalCollection( List<String> names );

    LogicalCollection getLogicalCollection( long id );

    LogicalCollection getLogicalCollection( long namespaceId, String name );

    List<LogicalCollection> getLogicalCollections( long namespaceId, Pattern name );


    LogicalCollection getCollection( String collection );

    LogicalCollection getCollection( long id, String collection );

}

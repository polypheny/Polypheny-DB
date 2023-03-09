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

package org.polypheny.db.catalog.snapshot.impl;

import java.util.List;
import org.polypheny.db.catalog.catalogs.LogicalCatalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.LogicalDocSnapshot;

public class LogicalDocSnapshotImpl implements LogicalDocSnapshot {

    public LogicalDocSnapshotImpl( LogicalCatalog value ) {

    }


    @Override
    public LogicalCollection getCollection( long collectionId ) {
        return null;
    }


    @Override
    public List<LogicalCollection> getCollections( Pattern namePattern ) {
        return null;
    }


    @Override
    public LogicalCollection getLogicalCollection( List<String> names ) {
        return null;
    }


    @Override
    public LogicalCollection getLogicalCollection( long id ) {
        return null;
    }


    @Override
    public LogicalCollection getLogicalCollection( long namespaceId, String name ) {
        return null;
    }


    @Override
    public List<LogicalCollection> getLogicalCollections( long namespaceId, Pattern name ) {
        return null;
    }

}

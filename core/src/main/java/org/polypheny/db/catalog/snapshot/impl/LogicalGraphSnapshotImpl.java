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
import java.util.Map;
import org.polypheny.db.catalog.catalogs.LogicalGraphCatalog;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.LogicalGraphSnapshot;

public class LogicalGraphSnapshotImpl implements LogicalGraphSnapshot {

    public LogicalGraphSnapshotImpl( Map<Long, LogicalGraphCatalog> value ) {
    }


    @Override
    public LogicalGraph getGraph( long id ) {
        return null;
    }


    @Override
    public List<LogicalGraph> getGraphs( Pattern graphName ) {
        return null;
    }


    @Override
    public LogicalGraph getLogicalGraph( List<String> names ) {
        return null;
    }




    @Override
    public LogicalGraph getLogicalGraph( long namespaceId, String name ) {
        return null;
    }


    @Override
    public List<LogicalGraph> getLogicalGraphs( long namespaceId, Pattern name ) {
        return null;
    }

}

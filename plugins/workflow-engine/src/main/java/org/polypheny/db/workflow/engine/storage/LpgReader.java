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

package org.polypheny.db.workflow.engine.storage;

import java.util.Iterator;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;

public class LpgReader extends CheckpointReader {

    public LpgReader( LogicalGraph graph, Transaction transaction ) {
        super( graph, transaction );
    }


    @Override
    public AlgNode getAlgNode( AlgCluster cluster ) {
        throw new NotImplementedException();
    }


    @Override
    public Iterator<PolyValue[]> getArrayIterator() {
        throw new NotImplementedException();
    }


    @Override
    public Iterator<List<PolyValue>> getIteratorFromQuery( String query ) {
        throw new NotImplementedException();
    }


    private LogicalGraph getGraph() {
        return (LogicalGraph) entity;
    }

}

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

import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;

public class LpgWriter extends CheckpointWriter {

    public LpgWriter( LogicalGraph graph, Transaction transaction ) {
        super( graph, transaction );
    }


    public void writeNode( PolyNode node ) {

    }


    public void writeEdge( PolyEdge edge ) {

    }




    private LogicalGraph getGraph() {
        return (LogicalGraph) entity;
    }


    @Override
    public void write( List<PolyValue> tuple ) {
        throw new NotImplementedException();
    }


    @Override
    public void close() throws Exception {
        throw new NotImplementedException();
    }

}
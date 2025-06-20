/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.workflow.engine.storage.writer;

import java.util.Iterator;
import java.util.List;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.CheckpointMetadata.LpgMetadata;
import org.polypheny.db.workflow.engine.storage.LpgBatchWriter;

public class LpgWriter extends CheckpointWriter {

    /**
     * How many nodes and edges are inspected for metadata extraction.
     * A higher value results in a larger overhead.
     */
    private static final int MAX_INSPECTIONS = 100;

    private final LpgBatchWriter writer;
    private boolean isWritingEdges = false;
    private long nodeWriteCount = 0;
    private long edgeWriteCount = 0;


    public LpgWriter( LogicalGraph graph, Transaction transaction, LpgMetadata metadata, boolean disableBatching ) {
        super( graph, transaction, metadata );
        writer = new LpgBatchWriter( graph, transaction, disableBatching );
    }


    public void write( GraphPropertyHolder value ) {
        if ( value.isNode() ) {
            writeNode( value.asNode() );
        } else {
            writeEdge( value.asEdge() );
        }
    }


    public void writeFromIterator( Iterator<GraphPropertyHolder> iterator ) {
        while ( iterator.hasNext() ) {
            write( iterator.next() );
        }
    }


    public void writeNode( PolyNode node ) {
        if ( isWritingEdges ) {
            throw new GenericRuntimeException( "Cannot write node after writing edges" );
        }
        if ( nodeWriteCount < MAX_INSPECTIONS ) {
            metadata.asLpg().addLabelsAndProps( node );
        }
        nodeWriteCount++;
        writer.write( node );
    }


    public void writeNode( Iterator<PolyNode> iterator, ExecutionContext ctx ) throws ExecutorException {
        while ( iterator.hasNext() ) {
            writeNode( iterator.next() );
            ctx.checkInterrupted();
        }
    }


    public void writeEdge( PolyEdge edge ) {
        if ( edgeWriteCount < MAX_INSPECTIONS ) {
            metadata.asLpg().addLabelsAndProps( edge );
        }
        edgeWriteCount++;
        writer.write( edge );
        isWritingEdges = true;
    }


    public void writeEdge( Iterator<PolyEdge> iterator, ExecutionContext ctx ) throws ExecutorException {
        while ( iterator.hasNext() ) {
            writeEdge( iterator.next() );
            ctx.checkInterrupted();
        }
    }


    @Override
    public void write( List<PolyValue> tuple ) {
        PolyValue value = tuple.get( 0 );
        if ( value.isNode() ) {
            writeNode( value.asNode() );
        } else if ( value.isEdge() ) {
            writeEdge( value.asEdge() );
        } else {
            throw new IllegalArgumentException( "LpgWriter can only write PolyNode or PolyEdge values, but found: " + value.getClass().getSimpleName() );
        }
    }


    @Override
    public long getWriteCount() {
        return nodeWriteCount + edgeWriteCount;
    }


    @Override
    public void close() throws Exception {
        LpgMetadata meta = metadata.asLpg();
        meta.setNodeCount( nodeWriteCount );
        meta.setEdgeCount( edgeWriteCount );
        if ( transaction.isActive() ) { // ensure writer is only closed once
            try {
                writer.close();
            } finally {
                super.close();
            }
        }
    }


    private LogicalGraph getGraph() {
        return (LogicalGraph) entity;
    }

}

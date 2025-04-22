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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;

public class LpgBatchWriter implements AutoCloseable {

    private static final long MAX_BYTES_PER_BATCH = 10 * 1024 * 1024L; // 10 MiB, upper limit to (estimated) size of batch in bytes
    private static final int MAX_TUPLES_PER_BATCH = 10_000; // upper limit to tuples per batch


    private final Transaction transaction;
    private final LogicalGraph graph;
    private final List<PolyNode> nodeValues = new ArrayList<>();
    private final List<PolyEdge> edgeValues = new ArrayList<>();
    private long batchSize = -1;


    public LpgBatchWriter( LogicalGraph graph, Transaction transaction ) {
        this.transaction = transaction;
        this.graph = graph;
    }


    public void write( PolyNode node ) {
        if ( batchSize == -1 ) {
            batchSize = QueryUtils.computeBatchSize( new PolyValue[]{ node }, MAX_BYTES_PER_BATCH, MAX_TUPLES_PER_BATCH );
            System.out.println( "Batch size is " + batchSize );
        }
        nodeValues.add( node );

        if ( nodeValues.size() < batchSize ) {
            return;
        }
        executeBatch( false );
    }


    public void write( PolyEdge edge ) {
        if ( batchSize == -1 ) {
            batchSize = QueryUtils.computeBatchSize( new PolyValue[]{ edge }, MAX_BYTES_PER_BATCH, MAX_TUPLES_PER_BATCH );
            System.out.println( "Batch size is " + batchSize );
        }
        edgeValues.add( edge );

        if ( edgeValues.size() < batchSize ) {
            return;
        }
        executeBatch( false );
    }


    private void executeBatch( boolean isEdges ) {

        Statement statement = transaction.createStatement();
        AlgNode modify = isEdges ? getModify( statement, List.of(), edgeValues ) : getModify( statement, nodeValues, List.of() );
        AlgRoot root = AlgRoot.of( modify, Kind.INSERT );

        List<? extends PolyValue> values = isEdges ? edgeValues : nodeValues;
        int batchSize = values.size();

        System.out.println( "Executing batch of size " + batchSize );
        ExecutedContext executedContext = QueryUtils.executeAlgRoot( root, statement );

        if ( executedContext.getException().isPresent() ) {
            throw new GenericRuntimeException( "An error occurred while writing a batch: ", executedContext.getException().get() );
        }
        List<List<PolyValue>> results = executedContext.getIterator().getAllRowsAndClose();
        long changedCount = results.size() == 1 ? results.get( 0 ).get( 0 ).asLong().longValue() : 0;
        if ( changedCount != batchSize ) {
            throw new GenericRuntimeException( "Unable to write all values of the batch: " + changedCount + " of " + batchSize + " tuples were written" );
        }

        values.clear();
    }


    private AlgNode getModify( Statement statement, List<PolyNode> nodes, List<PolyEdge> edges ) {
        AlgCluster cluster = AlgCluster.createGraph(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ),
                statement.getDataContext().getSnapshot() );

        AlgNode values = new LogicalLpgValues( cluster, cluster.traitSet(), nodes, edges, ImmutableList.of(), deriveTupleType( cluster, nodes ) );
        return new LogicalLpgModify( cluster, values.getTraitSet(), graph, values, Operation.INSERT, null, null ); // TODO: what are ids and operations?
    }


    private AlgDataType deriveTupleType( AlgCluster cluster, Collection<PolyNode> nodes ) {
        // TODO: use static function in LpgValues after PolyAlg is merged
        AlgDataTypeFactory.Builder builder = cluster.getTypeFactory().builder();
        AlgDataType nodeType = cluster.getTypeFactory().createPolyType( PolyType.NODE );
        for ( PolyNode node : nodes ) {
            String name = node.variableName == null ? "null" : node.variableName.value;
            builder.add( name, null, nodeType );
        }
        return builder.build();
    }


    @Override
    public void close() throws Exception {
        if ( !nodeValues.isEmpty() ) {
            executeBatch( false );
        }
        if ( !edgeValues.isEmpty() ) {
            executeBatch( true );
        }
    }

}

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;

public class DocBatchWriter implements AutoCloseable {

    private static final long MAX_BYTES_PER_BATCH = 10 * 1024 * 1024L; // 10 MiB, upper limit to (estimated) size of batch in bytes
    private static final int MAX_TUPLES_PER_BATCH = 10_000; // upper limit to tuples per batch


    private final Transaction transaction;
    private final LogicalCollection collection;
    private final List<Map<Long, PolyValue>> paramValues = new ArrayList<>();
    private long batchSize = -1;
    private final AlgRoot root;
    private final Statement statement;


    public DocBatchWriter( LogicalCollection collection, Transaction transaction ) {
        this.transaction = transaction;
        this.collection = collection;

        this.statement = transaction.createStatement();

        AlgCluster cluster = AlgCluster.createDocument(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ),
                statement.getDataContext().getSnapshot() );

        //AlgNode values = new LogicalDocumentValues( cluster, cluster.traitSet(), List.of(), List.of( new RexDynamicParam( DocumentType.ofId(), 0 ) ) );
        AlgNode values = LogicalDocumentValues.createDynamic( cluster, List.of( new RexDynamicParam( DocumentType.ofId(), 0 ) ) );
        AlgNode modify = LogicalDocumentModify.create( collection, values, Operation.INSERT, null, null, null );
        this.root = AlgRoot.of( modify, Kind.INSERT );

    }


    public void write( PolyDocument value ) {
        if ( batchSize == -1 ) {
            batchSize = QueryUtils.computeBatchSize( new PolyValue[]{ value }, MAX_BYTES_PER_BATCH, MAX_TUPLES_PER_BATCH );
            System.out.println( "Batch size is " + batchSize );
        }
        paramValues.add( Map.of( 0L, value ) );

        if ( paramValues.size() < batchSize ) {
            return;
        }
        executeBatch();
        //executePreparedBatch();
    }


    private void executeBatch() {
        int batchSize = paramValues.size();

        Statement statement = transaction.createStatement();

        AlgCluster cluster = AlgCluster.createDocument(
                statement.getQueryProcessor().getPlanner(),
                new RexBuilder( statement.getTransaction().getTypeFactory() ),
                statement.getDataContext().getSnapshot() );

        AlgNode values = LogicalDocumentValues.create( cluster, paramValues.stream().map( e -> (PolyDocument) e.get( 0L ) ).toList() );
        AlgNode modify = LogicalDocumentModify.create( collection, values, Operation.INSERT, null, null, null );
        AlgRoot root = AlgRoot.of( modify, Kind.INSERT );

        System.out.println( "Executing batch of size " + batchSize );
        System.out.println("The batch is :" + paramValues);
        ExecutedContext executedContext = QueryUtils.executeAlgRoot( root, statement );

        if ( executedContext.getException().isPresent() ) {
            throw new GenericRuntimeException( "An error occurred while writing a batch: ", executedContext.getException().get() );
        }
        List<List<PolyValue>> results = executedContext.getIterator().getAllRowsAndClose();
        long changedCount = results.size(); // TODO: result format is weird. For 3 written values: [[1], [1], [1]], for 10000 only 100 times [1]...
        if ( changedCount < 1 && batchSize > 0 ) { // Temporary solution
            throw new GenericRuntimeException( "Unable to write all values of the batch: " + changedCount + " of " + batchSize + " tuples were written. Result is " + results );
        }

        paramValues.clear();
        statement.getDataContext().resetParameterValues();
    }


    private void executePreparedBatch() {
        // TODO: currently results in NullPointerException in RefactorFunctions.fromDocument: "Cannot invoke "org.polypheny.db.type.entity.PolyValue.toTypedJson()" because "doc" is null"
        int batchSize = paramValues.size();

        statement.getDataContext().setParameterTypes( Map.of( 0L, DocumentType.ofId() ) );
        statement.getDataContext().setParameterValues( paramValues );

        System.out.println( "Executing prepared batch of size " + batchSize );
        ExecutedContext executedContext = QueryUtils.executeAlgRoot( root, statement );

        if ( executedContext.getException().isPresent() ) {
            throw new GenericRuntimeException( "An error occurred while writing a batch: ", executedContext.getException().get() );
        }
        List<List<PolyValue>> results = executedContext.getIterator().getAllRowsAndClose();
        long changedCount = results.size(); // TODO: result format is weird. For 3 written values: [[1], [1], [1]]
        if ( changedCount < 1 && batchSize > 0 ) { // Temporary solution
            throw new GenericRuntimeException( "Unable to write all values of the batch: " + changedCount + " of " + batchSize + " tuples were written. Result is " + results );
        }

        paramValues.clear();
        statement.getDataContext().resetParameterValues();
    }


    @Override
    public void close() throws Exception {
        if ( !paramValues.isEmpty() ) {
            executeBatch();
            //executePreparedBatch();
        }
    }

}

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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

public class RelBatchReader implements Iterator<PolyValue[]>, AutoCloseable {

    static final int MAX_TUPLES_PER_BATCH = 100_000; // upper limit to tuples per batch

    private final Transaction transaction;
    private final Statement readStatement;
    private final Pair<ParsedQueryContext, AlgRoot> parsed;
    private final AlgDataTypeFactory typeFactory = AlgDataTypeFactory.DEFAULT;
    private final LogicalTable table;
    private final int sortColumnIndex; // TODO: accept multiple sort cols
    private final boolean isUnique;
    private final AlgDataType sortColumnType;

    private Iterator<PolyValue[]> currentBatch;
    private PolyValue lastRead = null;
    private PolyValue[] firstRow;
    private int currentRowCount = 0; // number of rows read in the current batch

    private boolean hasNext = true;


    public RelBatchReader( LogicalTable table, Transaction transaction, String sortColumn, boolean isUnique ) {
        this.table = table;
        this.transaction = transaction;
        this.readStatement = transaction.createStatement();
        this.isUnique = isUnique;
        assert isUnique : "RelBatchReader currently expects a primary key column with no duplicates";

        this.sortColumnIndex = table.getTupleType().getFieldNames().indexOf( sortColumn );
        assert sortColumnIndex != -1 : "Invalid sort column";

        this.sortColumnType = table.getTupleType().getFields().get( sortColumnIndex ).getType();

        firstRow = readFirstRow(); // TODO: set batch size according to byte size of first row

        String query = "SELECT " + QueryUtils.quoteAndJoin( table.getColumnNames() ) + " FROM " + QueryUtils.quotedIdentifier( table ) +
                " WHERE " + sortColumn + " > ?" +
                " ORDER BY " + sortColumn +
                " LIMIT " + MAX_TUPLES_PER_BATCH;

        QueryContext context = QueryContext.builder()
                .query( query )
                .language( QueryLanguage.from( "SQL" ) )
                .isAnalysed( false )
                .origin( StorageManager.ORIGIN )
                .namespaceId( table.getNamespaceId() )
                .transactionManager( transaction.getTransactionManager() )
                .transactions( List.of( transaction ) ).build();

        this.parsed = QueryUtils.parseAndTranslateQuery( context, readStatement );
    }


    @Override
    public boolean hasNext() {
        return hasNext;
    }


    @Override
    public PolyValue[] next() {
        assert hasNext;

        if ( lastRead == null ) {
            lastRead = firstRow[sortColumnIndex];
            readBatch();
            return firstRow;
        }

        PolyValue[] row = currentBatch.next();
        currentRowCount++;

        if ( !currentBatch.hasNext() ) {
            if ( currentRowCount < MAX_TUPLES_PER_BATCH ) {
                hasNext = false;
                closeIterator();
            } else {
                lastRead = row[sortColumnIndex];
                readBatch();
            }
        }

        return row;
    }


    /**
     * Either sets the currentBatch iterator to the next batch if there are still rows to read
     * or sets hasNext to false and closes the iterator.
     */
    private void readBatch() {
        readStatement.getDataContext().addParameterValues( 0, sortColumnType, List.of( lastRead ) );

        ExecutedContext executedContext = QueryUtils.executeQuery( parsed, readStatement );
        readStatement.getDataContext().resetParameterValues();

        if ( executedContext.getException().isPresent() ) {
            throw new GenericRuntimeException( "An error occurred while reading a batch: ", executedContext.getException().get() );
        }

        currentBatch = executedContext.getIterator().getIterator();
        if ( !currentBatch.hasNext() ) {
            hasNext = false;
            closeIterator();
        }
        currentRowCount = 0;
    }


    private PolyValue[] readFirstRow() {
        String query = "SELECT " + QueryUtils.quoteAndJoin( table.getColumnNames() ) + " FROM " + QueryUtils.quotedIdentifier( table ) +
                " ORDER BY " + table.getColumnNames().get( sortColumnIndex ) +
                " LIMIT 1";

        QueryContext context = QueryContext.builder()
                .query( query )
                .language( QueryLanguage.from( "SQL" ) )
                .isAnalysed( false )
                .origin( StorageManager.ORIGIN )
                .namespaceId( table.getNamespaceId() )
                .transactionManager( transaction.getTransactionManager() )
                .transactions( List.of( transaction ) ).build();

        Statement statement = transaction.createStatement();

        ExecutedContext executedContext = QueryUtils.executeQuery( QueryUtils.parseAndTranslateQuery( context, statement ), statement );
        if ( executedContext.getException().isPresent() ) {
            throw new GenericRuntimeException( "An error occurred while reading the first row: ", executedContext.getException().get() );
        }
        List<List<PolyValue>> values = executedContext.getIterator().getAllRowsAndClose();
        if ( values.isEmpty() ) {
            hasNext = false;
            return null;
        }
        return values.get( 0 ).toArray( new PolyValue[0] );
    }


    private void closeIterator() {
        if ( currentBatch == null ) {
            return;
        }
        try {
            if ( currentBatch instanceof AutoCloseable ) {
                ((AutoCloseable) currentBatch).close();
            }
        } catch ( Exception ignored ) {
        }
        currentBatch = null;
    }


    @Override
    public void close() throws Exception {
        closeIterator();
    }


    public static class AsyncRelBatchReader extends RelBatchReader {

        private final BlockingQueue<PolyValue[]> queue;
        private final Thread t;


        public AsyncRelBatchReader( LogicalTable table, Transaction transaction, String sortColumn, boolean isUnique ) {
            super( table, transaction, sortColumn, isUnique );
            queue = new LinkedBlockingQueue<>( 2 * MAX_TUPLES_PER_BATCH );

            t = new Thread( () -> {
                while ( super.hasNext() ) {
                    PolyValue[] row = super.next();
                    try {
                        queue.put( row );
                    } catch ( InterruptedException e ) {
                        throw new RuntimeException( e );
                    }
                }
            } );
            t.start();
        }


        @Override
        public boolean hasNext() {
            return super.hasNext() || !queue.isEmpty();
        }


        @Override
        public PolyValue[] next() {
            try {
                return queue.take();
            } catch ( InterruptedException e ) {
                throw new RuntimeException( e );
            }
        }

    }

}

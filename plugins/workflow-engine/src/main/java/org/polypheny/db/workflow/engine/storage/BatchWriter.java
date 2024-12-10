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
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

public class BatchWriter implements AutoCloseable {

    private static final long MAX_BYTES_PER_BATCH = 10 * 1024 * 1024L; // 10 MiB, upper limit to (estimated) size of batch in bytes
    private static final int MAX_TUPLES_PER_BATCH = 10_000; // upper limit to tuples per batch


    private final Map<Long, AlgDataType> paramTypes;
    private final List<Map<Long, PolyValue>> paramValues = new ArrayList<>();
    private long batchSize = -1;

    private final Statement writeStatement;
    private final Pair<ParsedQueryContext, AlgRoot> parsed;


    public BatchWriter( QueryContext context, Statement statement, Map<Long, AlgDataType> paramTypes ) {
        this.writeStatement = statement;
        this.parsed = QueryUtils.parseAndTranslateQuery( context, writeStatement );
        this.paramTypes = paramTypes;
    }


    public void write( Map<Long, PolyValue> valueMap ) {
        if ( batchSize == -1 ) {
            batchSize = QueryUtils.computeBatchSize( valueMap.values().toArray( new PolyValue[0] ), MAX_BYTES_PER_BATCH, MAX_TUPLES_PER_BATCH );
        }
        paramValues.add( valueMap );

        if ( paramValues.size() < batchSize ) {
            return;
        }
        executeBatch();
    }


    private void executeBatch() {
        int batchSize = paramValues.size();

        writeStatement.getDataContext().setParameterTypes( paramTypes );
        writeStatement.getDataContext().setParameterValues( paramValues );

        // create new implementation for each batch
        ExecutedContext executedContext = QueryUtils.executeQuery( parsed, writeStatement );

        if ( executedContext.getException().isPresent() ) {
            throw new GenericRuntimeException( "An error occurred while writing a batch: ", executedContext.getException().get() );
        }
        List<List<PolyValue>> results = executedContext.getIterator().getAllRowsAndClose();
        long changedCount = results.size() == 1 ? results.get( 0 ).get( 0 ).asLong().longValue() : 0;
        if ( changedCount != batchSize ) {
            throw new GenericRuntimeException( "Unable to write all values of the batch: " + changedCount + " of " + batchSize + " tuples were written" );
        }

        paramValues.clear();
        writeStatement.getDataContext().resetParameterValues();
    }


    @Override
    public void close() throws Exception {
        if ( !paramValues.isEmpty() ) {
            executeBatch();
        }
    }

}

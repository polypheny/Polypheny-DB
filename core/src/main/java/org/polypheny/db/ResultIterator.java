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

package org.polypheny.db;

import static org.reflections.Reflections.log;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.commons.lang3.time.StopWatch;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;


@Value
public class ResultIterator implements AutoCloseable {

    Iterator<PolyValue[]> iterator;
    int batch;
    ExecutionTimeMonitor executionTimeMonitor;
    boolean isIndex;
    boolean isTimed;
    AlgDataType rowType;
    StatementEvent statementEvent;
    PolyImplementation implementation;


    public ResultIterator( Iterator<PolyValue[]> iterator, Statement statement, int batch, boolean isTimed, boolean isIndex, boolean isAnalyzed, AlgDataType rowType, ExecutionTimeMonitor executionTimeMonitor, PolyImplementation implementation ) {
        this.iterator = iterator;
        this.batch = batch;
        this.isIndex = isIndex;
        this.isTimed = isTimed;
        this.statementEvent = isAnalyzed ? statement.getMonitoringEvent() : null;
        this.executionTimeMonitor = executionTimeMonitor;
        this.implementation = implementation;
        this.rowType = rowType;
    }


    public List<List<PolyValue>> getNextBatch() {
        StopWatch stopWatch = null;
        try {
            if ( isTimed ) {
                stopWatch = new StopWatch();
                stopWatch.start();
            }
            List<List<PolyValue>> res = new ArrayList<>();
            int i = 0;
            while ( (batch < 0 || i++ < batch) && iterator.hasNext() ) {
                res.add( Lists.newArrayList( iterator.next() ) );
            }

            if ( isTimed ) {
                stopWatch.stop();
                executionTimeMonitor.setExecutionTime( stopWatch.getNanoTime() );
            }

            // Only if it is an index
            if ( statementEvent != null && isIndex ) {
                statementEvent.setIndexSize( res.size() );
            }

            return res;
        } catch ( Throwable t ) {
            try {
                close();
                throw new GenericRuntimeException( t );
            } catch ( Exception e ) {
                throw new GenericRuntimeException( t );
            }
        }
    }


    public boolean hasMoreRows() {
        return implementation.hasMoreRows();
    }


    public List<List<PolyValue>> getAllRowsAndClose() {
        List<List<PolyValue>> result = getNextBatch();
        try {
            close();
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
        return result;
    }


    @NotNull
    private <D> List<D> getNextBatch( @Nullable Function<PolyValue[], D> transformer ) {
        final Iterable<PolyValue[]> iterable = () -> iterator;

        if ( transformer == null ) {
            return (List<D>) StreamSupport
                    .stream( iterable.spliterator(), false )
                    .toList();
        }
        return StreamSupport
                .stream( iterable.spliterator(), false )
                .map( transformer )
                .toList();
    }


    public List<PolyValue[]> getArrayRows() {
        return getNextBatch( rowType.getFieldCount() == 1 ? e -> (PolyValue[]) e : null );
    }


    @Override
    public void close() throws Exception {
        try {
            if ( iterator instanceof AutoCloseable ) {
                ((AutoCloseable) iterator).close();
            }
        } catch ( Exception e ) {
            log.error( "Exception while closing result iterator", e );
        }
    }

}

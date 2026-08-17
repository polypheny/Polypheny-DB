/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.shared.execution;

import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnStatistics;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateCallDescriptor;
import org.polypheny.db.adapter.parquet.shared.aggregate.AggregateGroupState;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.aggregate.GroupKey;
import org.polypheny.db.type.entity.PolyValue;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractAggregateEnumerator implements Enumerator<PolyValue[]> {

    protected Enumerator<PolyValue[]> enumerator;


    protected AbstractAggregateEnumerator( Supplier<Enumerator<PolyValue[]>> enumeratorSupplier ) {
        enumerator = enumeratorSupplier.get();
    }


    /**
     * Gets the current element in the collection.
     *
     * <p>After an enumerator is created or after the {@link #reset} method is
     * called, the {@link #moveNext} method must be called to advance the
     * enumerator to the first element of the collection before reading the
     * value of the {@code current} property; otherwise, {@code current} is
     * undefined.
     *
     * <p>This method also throws {@link NoSuchElementException} if
     * the last call to {@code moveNext} returned {@code false}, which indicates
     * the end of the collection.
     *
     * <p>This method does not move the position of the enumerator, and
     * consecutive calls to {@code current} return the same object until either
     * {@code moveNext} or {@code reset} is called.
     *
     * <p>An enumerator remains valid as long as the collection remains
     * unchanged. If changes are made to the collection, such as adding,
     * modifying, or deleting elements, the enumerator is irrecoverably
     * invalidated. The next call to {@code moveNext} or {@code reset} may,
     * at the discretion of the implementation, throw a
     * {@link ConcurrentModificationException}. If the collection is
     * modified between {@code moveNext} and {@code current}, {@code current}
     * returns the element that it is set to, even if the enumerator is already
     * invalidated.
     *
     * @return Current element
     * @throws ConcurrentModificationException if collection
     * has been modified
     * @throws NoSuchElementException if {@code moveToNext}
     * has not been called, has not been called since the most
     * recent call to {@code reset}, or returned false
     */
    @Override
    public PolyValue[] current() {
        return enumerator.current();
    }


    /**
     * Advances the enumerator to the next element of the collection.
     *
     * <p>After an enumerator is created or after the {@code reset} method is
     * called, an enumerator is positioned before the first element of the
     * collection, and the first call to the {@code moveNext} method moves the
     * enumerator over the first element of the collection.
     *
     * <p>If {@code moveNext} passes the end of the collection, the enumerator
     * is positioned after the last element in the collection and
     * {@code moveNext} returns {@code false}. When the enumerator is at this
     * position, subsequent calls to {@code moveNext} also return {@code false}
     * until {@code #reset} is called.
     *
     * <p>An enumerator remains valid as long as the collection remains
     * unchanged. If changes are made to the collection, such as adding,
     * modifying, or deleting elements, the enumerator is irrecoverably
     * invalidated. The next call to {@code moveNext} or {@link #reset} may,
     * at the discretion of the implementation, throw a
     * {@link ConcurrentModificationException}.
     *
     * @return {@code true} if the enumerator was successfully advanced to the
     * next element; {@code false} if the enumerator has passed the end of
     * the collection
     */
    @Override
    public boolean moveNext() {
        return enumerator.moveNext();
    }


    /**
     * Sets the enumerator to its initial position, which is before the first
     * element in the collection.
     *
     * <p>An enumerator remains valid as long as the collection remains
     * unchanged. If changes are made to the collection, such as adding,
     * modifying, or deleting elements, the enumerator is irrecoverably
     * invalidated. The next call to {@link #moveNext} or {@code reset} may,
     * at the discretion of the implementation, throw a
     * {@link ConcurrentModificationException}.
     *
     * <p>This method is optional; it may throw
     * {@link UnsupportedOperationException}.
     *
     * <p><b>Notes to Implementers</b>
     *
     * <p>All calls to Reset must result in the same state for the enumerator.
     * The preferred implementation is to move the enumerator to the beginning
     * of the collection, before the first element. This invalidates the
     * enumerator if the collection has been modified since the enumerator was
     * created, which is consistent with {@link #moveNext()} and
     * {@link #current()}.
     */
    @Override
    public void reset() {
        enumerator.reset();
    }


    /**
     * Closes this enumerable and releases resources.
     *
     * <p>This method is idempotent. Calling it multiple times has the same effect
     * as calling it once.
     */
    @Override
    public void close() {
        enumerator.close();
    }


    /**
     * Reads all source files in parallel.
     *
     * @param sourceFiles a list of source files for the provided parquet table
     * @param cancelFlag a cancel operation flag
     * @return aggregated data
     */
    protected static <T> Map<GroupKey, AggregateGroupState> readAll( List<T> sourceFiles, Function<T, Map<GroupKey, AggregateGroupState>> reader, AggregateCallDescriptor[] aggregateCalls, AtomicBoolean cancelFlag ) {
        if ( sourceFiles.isEmpty() ) {
            return new LinkedHashMap<>();
        }
        if ( sourceFiles.size() == 1 ) {
            return reader.apply( sourceFiles.get( 0 ) );
        }

        int parallelism = Math.min( sourceFiles.size(), Math.max( 1, Runtime.getRuntime().availableProcessors() ) );
        ExecutorService executor = Executors.newFixedThreadPool( parallelism );

        try {
            Map<GroupKey, AggregateGroupState> aggregates = new LinkedHashMap<>();
            List<Callable<Map<GroupKey, AggregateGroupState>>> tasks = sourceFiles.stream()
                    .<Callable<Map<GroupKey, AggregateGroupState>>>map( sourceFile -> () -> reader.apply( sourceFile ) )
                    .toList();

            List<Future<Map<GroupKey, AggregateGroupState>>> futures = executor.invokeAll( tasks );
            for ( Future<Map<GroupKey, AggregateGroupState>> future : futures ) {
                mergeAggregates( aggregates, future.get(), aggregateCalls );
            }
            return aggregates;
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            cancelFlag.set( true );
            throw new RuntimeException( "Interrupted while calculating streaming aggregate.", e );
        } catch ( ExecutionException e ) {
            throw new RuntimeException( "Unable to calculate streaming aggregate.", e.getCause() );
        } finally {
            executor.shutdownNow();
        }
    }


    /**
     * Merges aggregates from different sources.
     *
     * @param target a target aggregate.
     * @param source a source aggregate.
     */
    protected static void mergeAggregates( Map<GroupKey, AggregateGroupState> target, Map<GroupKey, AggregateGroupState> source, AggregateCallDescriptor[] aggregateCalls ) {
        source.forEach( ( key, sourceAggregates ) -> target.computeIfAbsent( key, ignored -> new AggregateGroupState( aggregateCalls ) ).merge( sourceAggregates ) );
    }


    /**
     * Creates aggregate call descriptors.
     *
     * @param aggregateKinds an array of aggregate kinds.
     * @param aggregateArgs aggregate function arguments.
     * @return aggregate call descriptors.
     */
    protected static AggregateCallDescriptor[] aggregateCalls( String[] aggregateKinds, int[] aggregateArgs ) {
        AggregateCallDescriptor[] calls = new AggregateCallDescriptor[aggregateKinds.length];
        for ( int i = 0; i < aggregateKinds.length; i++ ) {
            calls[i] = AggregateCallDescriptor.of( aggregateKinds[i], aggregateArgs[i] );
        }
        return calls;
    }


    /**
     * Retrieves a row count of the source file from previously saved statistics.
     *
     * @param sourceFile a source file.
     * @return a row count.
     */
    protected static long sourceRowCount( ParquetSourceFile sourceFile ) {
        Optional<Long> rowCount = sourceFile.columnStatistics().values().stream()
                .map( ParquetColumnStatistics::rowCount )
                .findFirst();
        return rowCount.orElseGet( () -> new ParquetSchemaReader( sourceFile.asSource() ).getEstimatedRowCount() );
    }


    /**
     * Builds a result set from the aggregates.
     *
     * @param aggregates aggregates to build the result rows from.
     * @param aggregateCalls a list of aggregate functions.
     * @return returns a list of {@link PolyValue}s representing rows.
     */
    protected static List<PolyValue[]> buildRows( int groupCount, Map<GroupKey, AggregateGroupState> aggregates, AggregateCallDescriptor[] aggregateCalls ) {
        if ( groupCount == 0 && aggregates.isEmpty() ) {
            aggregates.put( GroupKey.Empty, new AggregateGroupState( aggregateCalls ) );
        }
        List<PolyValue[]> resultRows = new ArrayList<>( aggregates.size() );
        int aggregateCount = aggregateCalls.length;
        for ( Map.Entry<GroupKey, AggregateGroupState> entry : aggregates.entrySet() ) {
            PolyValue[] resultRow = new PolyValue[groupCount + aggregateCount];
            for ( int i = 0; i < groupCount; i++ ) {
                resultRow[i] = (PolyValue) entry.getKey().value( i );
            }
            for ( int i = 0; i < aggregateCount; i++ ) {
                resultRow[groupCount + i] = entry.getValue().result( i );
            }
            resultRows.add( resultRow );
        }
        return resultRows;
    }

}

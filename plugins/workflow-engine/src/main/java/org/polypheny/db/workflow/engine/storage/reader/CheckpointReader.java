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

package org.polypheny.db.workflow.engine.storage.reader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.engine.storage.QueryUtils;
import org.polypheny.db.workflow.engine.storage.StorageManager;

public abstract class CheckpointReader implements AutoCloseable {

    final LogicalEntity entity;
    final Transaction transaction;
    private final Set<AutoCloseable> openIterators = new HashSet<>();


    public CheckpointReader( LogicalEntity entity, Transaction transaction ) {
        this.entity = entity;
        this.transaction = transaction;
    }


    public abstract AlgNode getAlgNode( AlgCluster cluster );

    /**
     * Get a new PolyValue[] iterator for this checkpoint.
     * The iterator is closed automatically when this reader is closed.
     *
     * @return a PolyValue[] iterator representing the tuples of this checkpoint.
     */
    public abstract Iterator<PolyValue[]> getArrayIterator();


    /**
     * Get a new iterator for this checkpoint that transforms the raw PolyValue array into a
     * mutable list.
     * The iterator is closed automatically when this reader is closed.
     *
     * @return An iterator that yields tuples as mutable lists.
     */
    public final Iterator<List<PolyValue>> getIterator() {
        return arrayToListIterator( getArrayIterator(), false );
    }


    /**
     * Convenience method that wraps the iterator from {@code getIterator()} into an Iterable
     * to enable enhanced for loops.
     *
     * @return An iterable that yields tuples as mutable lists.
     */
    public final Iterable<List<PolyValue>> getIterable() {
        return this::getIterator;
    }


    /**
     * Get a new iterator for this checkpoint that transforms the raw PolyValue array into a
     * list.
     *
     * @param fixedSize whether the generated lists by the iterator should be of fixedSize (more efficient).
     * @return An iterator that yields tuples as lists.
     */
    public final Iterator<List<PolyValue>> getIterator( boolean fixedSize ) {
        return arrayToListIterator( getArrayIterator(), fixedSize );
    }


    /**
     * Read this checkpoint through the means of a custom query.
     * The query may not modify any data. It can only read this checkpoint.
     * For user defined input, it is advised to use dynamic parameters in the CheckpointQuery, to avoid SQL injections.
     *
     * @param query The CheckpointQuery to be executed.
     * @return the result tuple type and an iterator of the query result
     */
    public Pair<AlgDataType, Iterator<List<PolyValue>>> getIteratorFromQuery( CheckpointQuery query ) {
        return getIteratorFromQuery( query, List.of( this ) );
    }


    /**
     * Convenience method that wraps the iterator from {@code getIteratorFromQuery(query)} into an Iterable
     * to enable enhanced for loops.
     *
     * @return the result tuple type and an iterable of the query result
     */
    public Pair<AlgDataType, Iterable<List<PolyValue>>> getIterableFromQuery( CheckpointQuery query ) {
        Pair<AlgDataType, Iterator<List<PolyValue>>> pair = getIteratorFromQuery( query, List.of( this ) );
        return Pair.of( pair.left, () -> pair.right );
    }


    public AlgDataType getTupleType() {
        return entity.getTupleType();
    }


    void registerIterator( Iterator<PolyValue[]> iterator ) {
        if ( iterator instanceof AutoCloseable closeable ) {
            openIterators.add( closeable );
        }
    }


    public static Iterator<List<PolyValue>> arrayToListIterator( Iterator<PolyValue[]> iterator, boolean fixedSize ) {
        if ( fixedSize ) {
            return new Iterator<>() {
                private final Iterator<PolyValue[]> arrayIterator = iterator;


                @Override
                public boolean hasNext() {
                    return arrayIterator.hasNext();
                }


                @Override
                public List<PolyValue> next() {
                    return Arrays.asList( arrayIterator.next() );
                }
            };
        }
        return new Iterator<>() {
            private final Iterator<PolyValue[]> arrayIterator = iterator;


            @Override
            public boolean hasNext() {
                return arrayIterator.hasNext();
            }


            @Override
            public List<PolyValue> next() {
                return new ArrayList<>( Arrays.asList( arrayIterator.next() ) );
            }
        };
    }


    /**
     * Read any of the checkpoints for the given input readers through the means of a custom query.
     * This reader is used as the primary reader and must always be part of the inputs list. When it gets closed, the iterator will also be closed.
     * The query may not modify any data. It can only read the checkpoints of the specified readers.
     * For user defined input, it is advised to use dynamic parameters in the CheckpointQuery, to avoid SQL injections.
     *
     * @param query The CheckpointQuery to be executed.
     * @param inputs The readers whose checkpoints can be used in the query. The index of a reader in this list corresponds to the placeholder index in the CheckpointQuery.
     * @return the result tuple type and an iterator of the query result
     */
    public Pair<AlgDataType, Iterator<List<PolyValue>>> getIteratorFromQuery( CheckpointQuery query, List<CheckpointReader> inputs ) {
        assert inputs.contains( this );
        List<LogicalEntity> entities = inputs.stream().map( reader -> reader.entity ).toList();

        String queryStr = query.getQueryWithPlaceholdersReplaced( entities );
        QueryContext context = QueryContext.builder()
                .query( queryStr )
                .language( QueryLanguage.from( query.getQueryLanguage() ) )
                .isAnalysed( false )
                .batch( 100 ) // TODO: ensure this has the desired effect, then change to suitable value
                .origin( StorageManager.ORIGIN )
                .namespaceId( entity.getNamespaceId() )
                .transactionManager( transaction.getTransactionManager() )
                .transactions( List.of( transaction ) ).build();

        Statement statement = transaction.createStatement();
        Pair<ParsedQueryContext, AlgRoot> parsed = QueryUtils.parseAndTranslateQuery( context, statement );

        if ( !QueryUtils.validateAlg( parsed.right, false, entities ) ) {
            throw new GenericRuntimeException( "The specified query is not permitted: " + queryStr );
        }

        if ( query.hasParams() ) {
            statement.getDataContext().setParameterTypes( query.getParameterTypes() );
            statement.getDataContext().setParameterValues( List.of( query.getParameterValues() ) );
        }

        ExecutedContext executedContext = QueryUtils.executeQuery( parsed, statement );
        if ( executedContext.getException().isPresent() ) {
            throw new GenericRuntimeException( "An error occurred while executing a query on a checkpoint." );
        }

        Iterator<PolyValue[]> iterator = executedContext.getIterator().getIterator();
        registerIterator( iterator );
        return Pair.of( executedContext.getIterator().getRowType(), arrayToListIterator( iterator, false ) );
    }


    /**
     * Convenience method that wraps the iterator from {@code getIteratorFromQuery(query, inputs)} into an Iterable
     * to enable enhanced for loops.
     *
     * @return the result tuple type and an iterable of the query result
     */
    public Pair<AlgDataType, Iterable<List<PolyValue>>> getIterableFromQuery( CheckpointQuery query, List<CheckpointReader> inputs ) {
        Pair<AlgDataType, Iterator<List<PolyValue>>> pair = getIteratorFromQuery( query, inputs );
        return Pair.of( pair.left,
                () -> pair.right );
    }


    @Override
    public void close() {
        for ( AutoCloseable iterator : openIterators ) {
            try {
                iterator.close();
            } catch ( Exception ignored ) {
            }
        }
        openIterators.clear();
        transaction.commit();  // read-only transaction
    }

}

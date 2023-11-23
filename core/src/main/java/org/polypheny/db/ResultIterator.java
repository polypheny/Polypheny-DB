/*
 * Copyright 2019-2023 The Polypheny Project
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.commons.lang3.time.StopWatch;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.processing.LanguageContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.ResultContext;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

public class ResultIterator implements AutoCloseable {

    public final static Map<String, LanguageContext> REGISTER = new HashMap<>();


    private final Iterator<PolyValue[]> iterator;
    private final int batch;
    private final ExecutionTimeMonitor executionTimeMonitor;
    private final boolean isIndex;
    private final boolean isTimed;
    private final AlgDataType rowType;

    private final StatementEvent statementEvent;


    public ResultIterator( Iterator<PolyValue[]> iterator, Statement statement, int batch, boolean isTimed, boolean isIndex, boolean isAnalyzed, AlgDataType rowType, ExecutionTimeMonitor executionTimeMonitor ) {
        this.iterator = iterator;
        this.batch = batch;
        this.isIndex = isIndex;
        this.isTimed = isTimed;
        this.statementEvent = isAnalyzed ? statement.getMonitoringEvent() : null;
        this.executionTimeMonitor = executionTimeMonitor;
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

            //List<List<T>> res = MetaImpl.collect( cursorFactory, (Iterator<Object>) iterator., new ArrayList<>() ).stream().map( e -> (List<T>) e ).collect( Collectors.toList() );

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


    public List<List<PolyValue>> getAllRowsAndClose() {
        List<List<PolyValue>> result = getNextBatch();
        try {
            close();
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e );
        }
        return result;
    }


    public List<PolyValue> getSingleRows() {
        return getNextBatch( null );
    }


    @NotNull
    private <D> List<D> getNextBatch( @Nullable Function<PolyValue[], D> transformer ) {
        final Iterable<PolyValue[]> iterable = () -> iterator;

        if ( transformer == null ) {
            return (List<D>) StreamSupport
                    .stream( iterable.spliterator(), false )
                    .collect( Collectors.toList() );
        }
        return StreamSupport
                .stream( iterable.spliterator(), false )
                .map( transformer )
                .collect( Collectors.toList() );
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


    public static List<Pair<QueryContext, ResultContext>> anyQuery( QueryContext queries ) {
        LanguageContext operators = REGISTER.get( queries.getLanguage().getSerializedName() );
        List<Pair<QueryContext, ResultContext>> iterators = new ArrayList<>();
        for ( QueryContext query : operators.getSplitter().apply( queries ) ) {
            iterators.add( Pair.of( query, operators.getToIterator().apply( query ) ) );
        }
        return iterators;
    }


    public static List<List<List<PolyValue>>> anyQueryAsValues( QueryContext queries ) {
        LanguageContext operators = REGISTER.get( queries.getLanguage().getSerializedName() );
        List<Pair<QueryContext, ResultContext>> iters = anyQuery( queries );
        BiFunction<QueryContext, ResultContext, List<List<PolyValue>>> operator = operators.getToPolyValue();
        return iters.stream().map( p -> operator.apply( p.left, p.right ) ).collect( Collectors.toList() );

    }


    public static void addLanguage(
            String language,
            LanguageContext function ) {
        REGISTER.put( language, function );
    }


    public static void removeLanguage( String name ) {
        REGISTER.remove( name );
    }


}

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

package org.polypheny.db.processing;


import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.calcite.linq4j.Linq4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;

@Value
@NonFinal
public class ImplementationContext {

    PolyImplementation implementation;

    ParsedQueryContext query;

    Statement statement;

    @Nullable
    Throwable exception;


    public static ImplementationContext ofError( Throwable e, ParsedQueryContext parsed, Statement statement ) {
        return new ImplementationContext( null, parsed, statement, e );
    }


    public ExecutedContext execute( Statement statement ) {
        long time = System.nanoTime();
        try {
            ResultIterator result = implementation.execute( statement, query.getBatch(), query.isAnalysed(), query.isAnalysed(), false );
            time = System.nanoTime() - time;
            return new ExecutedContext( implementation, null, query, time, result, statement );
        } catch ( Throwable e ) {
            time = System.nanoTime() - time;
            return ExecutedContext.ofError( e, this, time );
        }

    }


    public Optional<Throwable> getException() {
        return Optional.ofNullable( exception );
    }


    @EqualsAndHashCode(callSuper = true)
    @Value
    public static class ExecutedContext extends ImplementationContext {

        @NotNull
        ResultIterator iterator;

        long executionTime;


        private ExecutedContext( PolyImplementation implementation, @Nullable Throwable error, ParsedQueryContext query, long executionTime, @NotNull ResultIterator iterator, Statement statement ) {
            super( implementation, query, statement, error );
            this.executionTime = executionTime;
            this.iterator = iterator;
        }


        public static ExecutedContext ofError( Throwable e, ImplementationContext implementation, @Nullable Long time ) {
            ResultIterator iterator = new ResultIterator( Linq4j.singletonEnumerable( new PolyValue[]{ PolyString.of( e.getMessage() ) } ).iterator(), implementation.statement, 1, false, false, false, null, null, implementation.implementation );
            return new ExecutedContext( implementation.implementation, e, implementation.query, time == null ? 0L : time, iterator, implementation.statement );
        }


    }

}

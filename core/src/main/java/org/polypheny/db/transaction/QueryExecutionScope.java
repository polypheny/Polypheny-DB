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

package org.polypheny.db.transaction;

/** Hooks for interfaces that can cancel the query being executed on this thread. */
public abstract class QueryExecutionScope implements AutoCloseable {

    private static final ThreadLocal<QueryExecutionScope> CURRENT = new ThreadLocal<>();


    protected final void activate() {
        CURRENT.set( this );
    }


    public static void statementStarted( Transaction transaction ) {
        QueryExecutionScope current = CURRENT.get();
        if ( current != null ) {
            current.beginStatement( transaction );
        }
    }


    public static InterruptionPause transactionFinishing( boolean committing ) {
        QueryExecutionScope current = CURRENT.get();
        return current == null ? null : current.beginTransactionFinish( committing );
    }


    public static long remainingTimeoutMillis() {
        QueryExecutionScope current = CURRENT.get();
        return current == null ? 0 : current.remainingMillis();
    }


    protected abstract long remainingMillis();

    protected abstract void beginStatement( Transaction transaction );

    protected abstract InterruptionPause beginTransactionFinish( boolean committing );


    @Override
    public void close() {
        CURRENT.remove();
    }


    @FunctionalInterface
    public interface InterruptionPause extends AutoCloseable {

        @Override
        void close();

    }

}

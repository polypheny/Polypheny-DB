/*
 * Copyright 2019-2021 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.runtime;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.prepare.PolyphenyDbPrepare.Query;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.util.Holder;


/**
 * Collection of hooks that can be set by observers and are executed at various parts of the query preparation process.
 *
 * For testing and debugging rather than for end-users.
 */
public enum Hook {
    /**
     * Called to get the current time. Use this to return a predictable time in tests.
     */
    CURRENT_TIME,

    /**
     * Called to get stdin, stdout, stderr. Use this to re-assign streams in tests.
     */
    STANDARD_STREAMS,

    /**
     * Returns a boolean value, whether{@link AlgBuilder}  should simplify expressions.
     * Default true.
     */
    REL_BUILDER_SIMPLIFY,

    /**
     * Returns a boolean value, whether the return convention should be {@link BindableConvention}.
     * Default false.
     */
    ENABLE_BINDABLE,

    /**
     * Called with the SQL string and parse tree, in an array.
     */
    PARSE_TREE,

    /**
     * Converts a SQL string to a {@link Query} object. This hook is an opportunity to execute a {@link AlgNode} query
     * plan in the JDBC driver rather than the usual SQL string.
     */
    STRING_TO_QUERY,

    /**
     * Called with the generated Java plan, just before it is compiled by Janino.
     */
    JAVA_PLAN,

    /**
     * Called with the output of sql-to-rel-converter.
     */
    CONVERTED,

    /**
     * Called with the created planner.
     */
    PLANNER,

    /**
     * Called after de-correlation and field trimming, but before optimization.
     */
    TRIMMED,

    /**
     * Called when a constant expression is being reduced.
     */
    EXPRESSION_REDUCER,

    /**
     * Called to create a Program to optimize the statement.
     */
    PROGRAM,

    /**
     * Called with a query that has been generated to send to a back-end system.
     * The query might be a SQL string (for the JDBC adapter), a list of Mongo pipeline expressions (for the MongoDB adapter), et cetera.
     */
    QUERY_PLAN;

    private final List<Consumer<Object>> handlers = new CopyOnWriteArrayList<>();

    private final ThreadLocal<List<Consumer<Object>>> threadHandlers = ThreadLocal.withInitial( ArrayList::new );


    /**
     * Adds a handler for this Hook.
     *
     * Returns a {@link Hook.Closeable} so that you can use the following try-finally pattern to prevent leaks:
     *
     * <blockquote><pre>
     *     final Hook.Closeable closeable = Hook.FOO.add(HANDLER);
     *     try {
     *         ...
     *     } finally {
     *         closeable.close();
     *     }</pre>
     * </blockquote>
     */
    public <T> Closeable add( final Consumer<T> handler ) {
        //noinspection unchecked
        handlers.add( (Consumer<Object>) handler );
        return () -> remove( handler );
    }


    /**
     * Removes a handler from this Hook.
     */
    private boolean remove( Consumer handler ) {
        return handlers.remove( handler );
    }


    /**
     * Adds a handler for this thread.
     */
    public <T> Closeable addThread( final Consumer<T> handler ) {
        //noinspection unchecked
        threadHandlers.get().add( (Consumer<Object>) handler );
        return () -> removeThread( handler );
    }


    /**
     * Removes a thread handler from this Hook.
     */
    private boolean removeThread( Consumer handler ) {
        return threadHandlers.get().remove( handler );
    }


    /**
     * Returns a function that, when a hook is called, will "return" a given value. (Because of the way hooks work, it "returns" the value by writing into a {@link Holder}.
     */
    public static <V> Consumer<Holder<V>> propertyJ( final V v ) {
        return holder -> {
            holder.set( v );
        };
    }


    /**
     * Runs all handlers registered for this Hook, with the given argument.
     */
    public void run( Object arg ) {
        for ( Consumer<Object> handler : handlers ) {
            handler.accept( arg );
        }
        for ( Consumer<Object> handler : threadHandlers.get() ) {
            handler.accept( arg );
        }
    }


    /**
     * Returns the value of a property hook.
     * (Property hooks take a {@link Holder} as an argument.)
     */
    public <V> V get( V defaultValue ) {
        final Holder<V> holder = Holder.of( defaultValue );
        run( holder );
        return holder.get();
    }


    /**
     * Removes a Hook after use.
     */
    public interface Closeable extends AutoCloseable {

        /**
         * Closeable that does nothing.
         */
        Closeable EMPTY = () -> {
        };

        // override, removing "throws"
        @Override
        void close();

    }
}


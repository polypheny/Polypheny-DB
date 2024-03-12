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

package org.polypheny.db.util.trace;


import java.io.File;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Functions;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.plan.AlgImplementor;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.prepare.Prepare;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Contains all of the {@link org.slf4j.Logger tracers} used within org.polypheny.db.class libraries.
 *
 * <h3>Note to developers</h3>
 *
 * Please ensure that every tracer used in org.polypheny.db.is added to this class as a <em>public static final</em> member called <code><i>component</i>Tracer</code>. For example, {@link #getPlannerTracer} is the
 * tracer used by all classes which take part in the query planning process.
 *
 * The javadoc in this file is the primary source of information on what tracers are available, so the javadoc against each tracer member must be an up-to-date description of what that tracer does.
 *
 * In the class where the tracer is used, create a <em>private</em> (or perhaps <em>protected</em>) <em>static final</em> member called <code>tracer</code>.
 */
public abstract class PolyphenyDbTrace {

    /**
     * The "org.polypheny.db.sql.parser" tracer reports parser events in {@link Parser} and other classes at DEBUG.
     */
    public static final Logger PARSER_LOGGER = getParserTracer();

    private static final ThreadLocal<Function2<Void, File, String>> DYNAMIC_HANDLER = ThreadLocal.withInitial( Functions::ignore2 );


    /**
     * The "org.polypheny.db.plan.RelOptPlanner" tracer prints the query optimization process.
     *
     * Levels:
     *
     * <ul>
     * <li>{@link Logger#debug(String)} (formerly FINE) prints rules as they fire;</li>
     * <li>{@link Logger#trace(String)} (formerly FINER) prints and validates the whole expression pool and rule queue as each rule fires;</li>
     * <li>{@link Logger#trace(String)} (formerly FINEST) also prints finer details like rule importances.</li>
     * </ul>
     */
    public static Logger getPlannerTracer() {
        return LoggerFactory.getLogger( AlgPlanner.class.getName() );
    }


    /**
     * The "org.polypheny.db.prepare.Prepare" tracer prints the generated program at DEBUG (formerly, FINE)  or higher.
     */
    public static Logger getStatementTracer() {
        return LoggerFactory.getLogger( Prepare.class.getName() );
    }


    /**
     * The "org.polypheny.db.alg.RelImplementorImpl" tracer reports when expressions are bound to variables (DEBUG, formerly FINE)
     */
    public static Logger getRelImplementorTracer() {
        return LoggerFactory.getLogger( AlgImplementor.class );
    }


    /**
     * The tracer "org.polypheny.db.sql.timing" traces timing for various stages of query processing.
     *
     * @see PolyphenyDbTimingTracer
     */
    public static Logger getSqlTimingTracer() {
        return LoggerFactory.getLogger( "org.polypheny.db.sql.timing" );
    }


    /**
     * The "org.polypheny.db.sql.parser" tracer reports parse events.
     */
    public static Logger getParserTracer() {
        return LoggerFactory.getLogger( "org.polypheny.db.sql.parser" );
    }


    /**
     * The "org.polypheny.db.sql2rel" tracer reports parse events.
     */
    public static Logger getSqlToRelTracer() {
        return LoggerFactory.getLogger( "org.polypheny.db.sql2rel" );
    }


    /**
     * Thread-local handler that is called with dynamically generated Java code. It exists for unit-testing.
     * The handler is never null; the default handler does nothing.
     */
    public static ThreadLocal<Function2<Void, File, String>> getDynamicHandler() {
        return DYNAMIC_HANDLER;
    }

}


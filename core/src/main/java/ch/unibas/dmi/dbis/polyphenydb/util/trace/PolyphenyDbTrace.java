/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.util.trace;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelImplementor;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import java.io.File;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Contains all of the {@link org.slf4j.Logger tracers} used within ch.unibas.dmi.dbis.polyphenydb.class libraries.
 *
 * <h3>Note to developers</h3>
 *
 * Please ensure that every tracer used in ch.unibas.dmi.dbis.polyphenydb.is added to this class as a <em>public static final</em> member called <code><i>component</i>Tracer</code>. For example, {@link #getPlannerTracer} is the
 * tracer used by all classes which take part in the query planning process.
 *
 * The javadoc in this file is the primary source of information on what tracers are available, so the javadoc against each tracer member must be an up-to-date description of what that tracer does.
 *
 * In the class where the tracer is used, create a <em>private</em> (or perhaps <em>protected</em>) <em>static final</em> member called <code>tracer</code>.
 */
public abstract class PolyphenyDbTrace {

    /**
     * The "ch.unibas.dmi.dbis.polyphenydb.sql.parser" tracer reports parser events in {@link SqlParser} and other classes at DEBUG.
     */
    public static final Logger PARSER_LOGGER = getParserTracer();

    private static final ThreadLocal<Function2<Void, File, String>> DYNAMIC_HANDLER = ThreadLocal.withInitial( Functions::ignore2 );


    /**
     * The "ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner" tracer prints the query optimization process.
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
        return LoggerFactory.getLogger( RelOptPlanner.class.getName() );
    }


    /**
     * The "ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare" tracer prints the generated program at DEBUG (formerly, FINE)  or higher.
     */
    public static Logger getStatementTracer() {
        return LoggerFactory.getLogger( Prepare.class.getName() );
    }


    /**
     * The "ch.unibas.dmi.dbis.polyphenydb.rel.RelImplementorImpl" tracer reports when expressions are bound to variables (DEBUG, formerly FINE)
     */
    public static Logger getRelImplementorTracer() {
        return LoggerFactory.getLogger( RelImplementor.class );
    }


    /**
     * The tracer "ch.unibas.dmi.dbis.polyphenydb.sql.timing" traces timing for various stages of query processing.
     *
     * @see PolyphenyDbTimingTracer
     */
    public static Logger getSqlTimingTracer() {
        return LoggerFactory.getLogger( "ch.unibas.dmi.dbis.polyphenydb.sql.timing" );
    }


    /**
     * The "ch.unibas.dmi.dbis.polyphenydb.sql.parser" tracer reports parse events.
     */
    public static Logger getParserTracer() {
        return LoggerFactory.getLogger( "ch.unibas.dmi.dbis.polyphenydb.sql.parser" );
    }


    /**
     * The "ch.unibas.dmi.dbis.polyphenydb.sql2rel" tracer reports parse events.
     */
    public static Logger getSqlToRelTracer() {
        return LoggerFactory.getLogger( "ch.unibas.dmi.dbis.polyphenydb.sql2rel" );
    }


    /**
     * Thread-local handler that is called with dynamically generated Java code. It exists for unit-testing.
     * The handler is never null; the default handler does nothing.
     */
    public static ThreadLocal<Function2<Void, File, String>> getDynamicHandler() {
        return DYNAMIC_HANDLER;
    }
}


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

package ch.unibas.dmi.dbis.polyphenydb.test;


import ch.unibas.dmi.dbis.polyphenydb.piglet.Ast;
import ch.unibas.dmi.dbis.polyphenydb.piglet.Ast.Program;
import ch.unibas.dmi.dbis.polyphenydb.piglet.Handler;
import ch.unibas.dmi.dbis.polyphenydb.piglet.parser.ParseException;
import ch.unibas.dmi.dbis.polyphenydb.piglet.parser.PigletParser;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.tools.PigRelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;

import com.google.common.collect.Ordering;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


/**
 * Fluent API to perform Piglet Parser test actions.
 */
class Fluent {

    private final String pig;


    Fluent( String pig ) {
        this.pig = pig;
    }


    private Program parseProgram( String s ) throws ParseException {
        return new PigletParser( new StringReader( s ) ).stmtListEof();
    }


    public Fluent explainContains( String expected ) throws ParseException {
        final Program program = parseProgram( pig );
        final PigRelBuilder builder = PigRelBuilder.create( PigRelBuilderTest.config().build() );
        new Handler( builder ).handle( program );
        assertThat( Util.toLinux( RelOptUtil.toString( builder.peek() ) ), is( expected ) );
        return this;
    }


    public Fluent returns( final String out ) throws ParseException {
        return returns( s -> {
            assertThat( s, is( out ) );
            return null;
        } );
    }


    public Fluent returnsUnordered( String... lines ) throws ParseException {
        final List<String> expectedLines = Ordering.natural().immutableSortedCopy( Arrays.asList( lines ) );
        return returns( s -> {
            final List<String> actualLines = new ArrayList<>();
            for ( ; ; ) {
                int i = s.indexOf( '\n' );
                if ( i < 0 ) {
                    if ( !s.isEmpty() ) {
                        actualLines.add( s );
                    }
                    break;
                } else {
                    actualLines.add( s.substring( 0, i ) );
                    s = s.substring( i + 1 );
                }
            }
            assertThat( Ordering.natural().sortedCopy( actualLines ), is( expectedLines ) );
            return null;
        } );
    }


    public Fluent returns( Function<String, Void> checker ) throws ParseException {
        final Program program = parseProgram( pig );
        final PigRelBuilder builder = PigRelBuilder.create( PigRelBuilderTest.config().build() );
        final StringWriter sw = new StringWriter();
        new PolyphenyDbHandler( builder, sw ).handle( program );
        checker.apply( Util.toLinux( sw.toString() ) );
        return this;
    }


    public Fluent parseContains( String expected ) throws ParseException {
        final Program program = parseProgram( pig );
        assertThat( Util.toLinux( Ast.toString( program ) ), is( expected ) );
        return this;
    }
}


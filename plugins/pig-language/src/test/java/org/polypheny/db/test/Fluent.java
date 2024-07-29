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

package org.polypheny.db.test;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.Ordering;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.polypheny.db.piglet.Ast;
import org.polypheny.db.piglet.Ast.Program;
import org.polypheny.db.piglet.Handler;
import org.polypheny.db.piglet.parser.ParseException;
import org.polypheny.db.piglet.parser.PigletParser;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.tools.PigAlgBuilder;
import org.polypheny.db.util.Util;


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
        final PigAlgBuilder builder = PigAlgBuilderTest.builder();
        final Program program = parseProgram( pig );
        new Handler( builder ).handle( program );
        assertThat( Util.toLinux( AlgOptUtil.toString( builder.peek() ) ), is( expected ) );
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
        final PigAlgBuilder builder = PigAlgBuilderTest.builder();
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


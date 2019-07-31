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
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.test;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.DataContext.SlimDataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Interpreter;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.ContextImpl;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.ScannableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.tools.FrameworkConfig;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.tools.Planner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.QueryProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * Unit tests for {@link ch.unibas.dmi.dbis.polyphenydb.interpreter.Interpreter}.
 */
public class InterpreterTest {

    private SchemaPlus rootSchema;
    private Planner planner;
    private MyDataContext dataContext;


    /**
     * Implementation of {@link DataContext} for executing queries without a
     * connection.
     */
    private class MyDataContext implements DataContext {

        private final Planner planner;


        MyDataContext( Planner planner ) {
            this.planner = planner;
        }


        public SchemaPlus getRootSchema() {
            return rootSchema;
        }


        public JavaTypeFactory getTypeFactory() {
            return (JavaTypeFactory) planner.getTypeFactory();
        }


        public QueryProvider getQueryProvider() {
            return null;
        }


        public Object get( String name ) {
            return null;
        }
    }


    @Before
    public void setUp() {
        rootSchema = Frameworks.createRootSchema( true );
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig( SqlParser.Config.DEFAULT )
                .defaultSchema( PolyphenyDbAssert.addSchema( rootSchema, PolyphenyDbAssert.SchemaSpec.HR ) )
                .prepareContext( new ContextImpl(
                        PolyphenyDbSchema.from( rootSchema ),
                        new SlimDataContext() {
                            @Override
                            public JavaTypeFactory getTypeFactory() {
                                return new JavaTypeFactoryImpl();
                            }
                        },
                        "",
                        0,
                        0,
                        null ) )
                .build();
        planner = Frameworks.getPlanner( config );
        dataContext = new MyDataContext( planner );
    }


    @After
    public void tearDown() {
        rootSchema = null;
        planner = null;
        dataContext = null;
    }


    /**
     * Tests executing a simple plan using an interpreter.
     */
    @Test
    public void testInterpretProjectFilterValues() throws Exception {
        SqlNode parse = planner.parse( "select y, x\n" + "from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)\n" + "where x > 1" );

        SqlNode validate = planner.validate( parse );
        RelNode convert = planner.rel( validate ).rel;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[b, 2]", "[c, 3]" );
    }


    /**
     * Tests a plan where the sort field is projected away.
     */
    @Test
    public void testInterpretOrder() throws Exception {
        final String sql = "select y\n" + "from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)\n" + "order by -x";
        SqlNode parse = planner.parse( sql );
        SqlNode validate = planner.validate( parse );
        RelNode convert = planner.rel( validate ).project();

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[c]", "[b]", "[a]" );
    }


    private static void assertRows( Interpreter interpreter, String... rows ) {
        assertRows( interpreter, false, rows );
    }


    private static void assertRowsUnordered( Interpreter interpreter, String... rows ) {
        assertRows( interpreter, true, rows );
    }


    private static void assertRows( Interpreter interpreter, boolean unordered, String... rows ) {
        final List<String> list = new ArrayList<>();
        for ( Object[] row : interpreter ) {
            list.add( Arrays.toString( row ) );
        }
        final List<String> expected = Arrays.asList( rows );
        if ( unordered ) {
            Collections.sort( list );
            Collections.sort( expected );
        }
        assertThat( list, equalTo( expected ) );
    }


    /**
     * Tests executing a simple plan using an interpreter.
     */
    @Test
    public void testInterpretTable() throws Exception {
        SqlNode parse = planner.parse( "select * from \"hr\".\"emps\" order by \"empid\"" );

        SqlNode validate = planner.validate( parse );
        RelNode convert = planner.rel( validate ).rel;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[100, 10, Bill, 10000.0, 1000]", "[110, 10, Theodore, 11500.0, 250]", "[150, 10, Sebastian, 7000.0, null]", "[200, 20, Eric, 8000.0, 500]" );
    }


    /**
     * Tests executing a plan on a {@link ScannableTable} using an interpreter.
     */
    @Test
    public void testInterpretScannableTable() throws Exception {
        rootSchema.add( "beatles", new ScannableTableTest.BeatlesTable() );
        SqlNode parse = planner.parse( "select * from \"beatles\" order by \"i\"" );

        SqlNode validate = planner.validate( parse );
        RelNode convert = planner.rel( validate ).rel;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[4, John]", "[4, Paul]", "[5, Ringo]", "[6, George]" );
    }


    @Test
    public void testAggregateCount() throws Exception {
        rootSchema.add( "beatles", new ScannableTableTest.BeatlesTable() );
        SqlNode parse = planner.parse( "select  count(*) from \"beatles\"" );

        SqlNode validate = planner.validate( parse );
        RelNode convert = planner.rel( validate ).rel;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[4]" );
    }


    @Test
    public void testAggregateMax() throws Exception {
        rootSchema.add( "beatles", new ScannableTableTest.BeatlesTable() );
        SqlNode parse = planner.parse( "select  max(\"i\") from \"beatles\"" );

        SqlNode validate = planner.validate( parse );
        RelNode convert = planner.rel( validate ).rel;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[6]" );
    }


    @Test
    public void testAggregateMin() throws Exception {
        rootSchema.add( "beatles", new ScannableTableTest.BeatlesTable() );
        SqlNode parse = planner.parse( "select  min(\"i\") from \"beatles\"" );

        SqlNode validate = planner.validate( parse );
        RelNode convert = planner.rel( validate ).rel;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[4]" );
    }


    @Test
    public void testAggregateGroup() throws Exception {
        rootSchema.add( "beatles", new ScannableTableTest.BeatlesTable() );
        SqlNode parse = planner.parse( "select \"j\", count(*) from \"beatles\" group by \"j\"" );

        SqlNode validate = planner.validate( parse );
        RelNode convert = planner.rel( validate ).rel;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRowsUnordered( interpreter, "[George, 1]", "[Paul, 1]", "[John, 1]", "[Ringo, 1]" );
    }


    @Test
    public void testAggregateGroupFilter() throws Exception {
        rootSchema.add( "beatles", new ScannableTableTest.BeatlesTable() );
        final String sql = "select \"j\",\n" + "  count(*) filter (where char_length(\"j\") > 4)\n" + "from \"beatles\" group by \"j\"";
        SqlNode parse = planner.parse( sql );
        SqlNode validate = planner.validate( parse );
        RelNode convert = planner.rel( validate ).rel;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRowsUnordered( interpreter, "[George, 1]", "[Paul, 0]", "[John, 0]", "[Ringo, 1]" );
    }


    /**
     * Tests executing a plan on a single-column {@link ScannableTable} using an interpreter.
     */
    @Test
    public void testInterpretSimpleScannableTable() throws Exception {
        rootSchema.add( "simple", new ScannableTableTest.SimpleTable() );
        SqlNode parse = planner.parse( "select * from \"simple\" limit 2" );

        SqlNode validate = planner.validate( parse );
        RelNode convert = planner.rel( validate ).rel;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[0]", "[10]" );
    }


    /**
     * Tests executing a UNION ALL query using an interpreter.
     */
    @Test
    public void testInterpretUnionAll() throws Exception {
        rootSchema.add( "simple", new ScannableTableTest.SimpleTable() );
        SqlNode parse = planner.parse( "select * from \"simple\"\n" + "union all\n" + "select * from \"simple\"\n" );

        SqlNode validate = planner.validate( parse );
        RelNode convert = planner.rel( validate ).rel;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[0]", "[10]", "[20]", "[30]", "[0]", "[10]", "[20]", "[30]" );
    }


    /**
     * Tests executing a UNION query using an interpreter.
     */
    @Test
    public void testInterpretUnion() throws Exception {
        rootSchema.add( "simple", new ScannableTableTest.SimpleTable() );
        SqlNode parse = planner.parse( "select * from \"simple\"\n" + "union\n" + "select * from \"simple\"\n" );

        SqlNode validate = planner.validate( parse );
        RelNode convert = planner.rel( validate ).rel;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[0]", "[10]", "[20]", "[30]" );
    }
}


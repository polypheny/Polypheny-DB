/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.sql;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.QueryProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataContext.SlimDataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.adapter.java.ReflectiveSchema;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.interpreter.Interpreter;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.ContextImpl;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.schema.HrSchema;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.ScannableTable;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.test.ScannableTableTest.BeatlesTable;
import org.polypheny.db.test.ScannableTableTest.SimpleTable;
import org.polypheny.db.tools.FrameworkConfig;
import org.polypheny.db.tools.Frameworks;
import org.polypheny.db.tools.Planner;
import org.polypheny.db.transaction.Statement;


/**
 * Unit tests for {@link Interpreter}.
 */
public class InterpreterTest extends SqlLanguagelDependant {

    private SchemaPlus rootSchema;
    private Planner planner;
    private MyDataContext dataContext;


    /**
     * Implementation of {@link DataContext} for executing queries without a connection.
     */
    private class MyDataContext implements DataContext {

        private final Planner planner;


        MyDataContext( Planner planner ) {
            this.planner = planner;
        }


        @Override
        public SchemaPlus getRootSchema() {
            return rootSchema;
        }


        @Override
        public JavaTypeFactory getTypeFactory() {
            return (JavaTypeFactory) planner.getTypeFactory();
        }


        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }


        @Override
        public Object get( String name ) {
            return null;
        }


        @Override
        public void addAll( Map<String, Object> map ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public Statement getStatement() {
            return null;
        }


        @Override
        public void addParameterValues( long index, AlgDataType type, List<Object> data ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public AlgDataType getParameterType( long index ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public List<Map<Long, Object>> getParameterValues() {
            throw new UnsupportedOperationException();
        }


        @Override
        public void setParameterValues( List<Map<Long, Object>> values ) {

        }


        @Override
        public Map<Long, AlgDataType> getParameterTypes() {
            return null;
        }


        @Override
        public void setParameterTypes( Map<Long, AlgDataType> types ) {

        }

    }


    @Before
    public void setUp() {
        rootSchema = Frameworks.createRootSchema( true ).add( "hr", new ReflectiveSchema( new HrSchema() ), NamespaceType.RELATIONAL );

        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig( ParserConfig.DEFAULT )
                .defaultSchema( rootSchema )
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
        Node parse = planner.parse( "select y, x\n" + "from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)\n" + "where x > 1" );

        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[b, 2]", "[c, 3]" );
    }


    /**
     * Tests a plan where the sort field is projected away.
     */
    @Test
    public void testInterpretOrder() throws Exception {
        final String sql = "select y\n" + "from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(x, y)\n" + "order by -x";
        Node parse = planner.parse( sql );
        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).project();

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
        Node parse = planner.parse( "select * from \"hr\".\"emps\" order by \"empid\"" );

        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[100, 1, Bill, 4000, 2]", "[150, 1, Sebastian, 6000, 2]", "[150, 4, Hans, 4400, 10]", "[200, 2, Eric, 2500, 3]" );
    }


    /**
     * Tests executing a plan on a {@link ScannableTable} using an interpreter.
     */
    @Test
    public void testInterpretScannableTable() throws Exception {
        rootSchema.add( "beatles", new BeatlesTable() );
        Node parse = planner.parse( "select * from \"beatles\" order by \"i\"" );

        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[4, John]", "[4, Paul]", "[5, Ringo]", "[6, George]" );
    }


    @Test
    public void testAggregateCount() throws Exception {
        rootSchema.add( "beatles", new BeatlesTable() );
        Node parse = planner.parse( "select  count(*) from \"beatles\"" );

        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[4]" );
    }


    @Test
    public void testAggregateMax() throws Exception {
        rootSchema.add( "beatles", new BeatlesTable() );
        Node parse = planner.parse( "select  max(\"i\") from \"beatles\"" );

        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[6]" );
    }


    @Test
    public void testAggregateMin() throws Exception {
        rootSchema.add( "beatles", new BeatlesTable() );
        Node parse = planner.parse( "select  min(\"i\") from \"beatles\"" );

        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[4]" );
    }


    @Test
    public void testAggregateGroup() throws Exception {
        rootSchema.add( "beatles", new BeatlesTable() );
        Node parse = planner.parse( "select \"j\", count(*) from \"beatles\" group by \"j\"" );

        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRowsUnordered( interpreter, "[George, 1]", "[Paul, 1]", "[John, 1]", "[Ringo, 1]" );
    }


    @Test
    public void testAggregateGroupFilter() throws Exception {
        rootSchema.add( "beatles", new BeatlesTable() );
        final String sql = "select \"j\",\n" + "  count(*) filter (where char_length(\"j\") > 4)\n" + "from \"beatles\" group by \"j\"";
        Node parse = planner.parse( sql );
        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRowsUnordered( interpreter, "[George, 1]", "[Paul, 0]", "[John, 0]", "[Ringo, 1]" );
    }


    /**
     * Tests executing a plan on a single-column {@link ScannableTable} using an interpreter.
     */
    @Test
    public void testInterpretSimpleScannableTable() throws Exception {
        rootSchema.add( "simple", new SimpleTable() );
        Node parse = planner.parse( "select * from \"simple\" limit 2" );

        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[0]", "[10]" );
    }


    /**
     * Tests executing a UNION ALL query using an interpreter.
     */
    @Test
    public void testInterpretUnionAll() throws Exception {
        rootSchema.add( "simple", new SimpleTable() );
        Node parse = planner.parse( "select * from \"simple\"\n" + "union all\n" + "select * from \"simple\"\n" );

        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[0]", "[10]", "[20]", "[30]", "[0]", "[10]", "[20]", "[30]" );
    }


    /**
     * Tests executing a UNION query using an interpreter.
     */
    @Test
    public void testInterpretUnion() throws Exception {
        rootSchema.add( "simple", new SimpleTable() );
        Node parse = planner.parse( "select * from \"simple\"\n" + "union\n" + "select * from \"simple\"\n" );

        Node validate = planner.validate( parse );
        AlgNode convert = planner.alg( validate ).alg;

        final Interpreter interpreter = new Interpreter( dataContext, convert );
        assertRows( interpreter, "[0]", "[10]", "[20]", "[30]" );
    }

}


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

package org.polypheny.db.entity;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Iterator;
import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;


/**
 * Unit test for {@link ScannableEntity}.
 */
public class ScannableEntityTest {

    @BeforeAll
    public static void setUpClass() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    @Test
    public void testTens() {
        final Enumerator<PolyValue[]> cursor = tens();
        assertTrue( cursor.moveNext() );
        assertThat( cursor.current()[0], equalTo( PolyInteger.of( 0 ) ) );
        assertThat( cursor.current().length, equalTo( 1 ) );
        assertTrue( cursor.moveNext() );
        assertThat( cursor.current()[0], equalTo( PolyInteger.of( 10 ) ) );
        assertTrue( cursor.moveNext() );
        assertThat( cursor.current()[0], equalTo( PolyInteger.of( 20 ) ) );
        assertTrue( cursor.moveNext() );
        assertThat( cursor.current()[0], equalTo( PolyInteger.of( 30 ) ) );
        assertFalse( cursor.moveNext() );
    }

//    /**
//     * A table with one column.
//     */
//    @Test
//    public void testSimple() throws Exception {
//        PolyphenyDbAssert.that()
//                .with( newSchema( "s", "simple", new SimpleTable() ) )
//                .query( "select * from \"s\".\"simple\"" )
//                .returnsUnordered( "i=0", "i=10", "i=20", "i=30" );
//    }
//
//
//    /**
//     * A table with two columns.
//     */
//    @Test
//    public void testSimple2() throws Exception {
//        PolyphenyDbAssert.that()
//                .with( newSchema( "s", "beatles", new BeatlesTable() ) )
//                .query( "select * from \"s\".\"beatles\"" )
//                .returnsUnordered( "i=4; j=John", "i=4; j=Paul", "i=6; j=George", "i=5; j=Ringo" );
//    }
//
//
//    /**
//     * A filter on a {@link FilterableTable} with two columns (cooperative).
//     */
//    @Test
//    public void testFilterableTableCooperative() throws Exception {
//        final StringBuilder buf = new StringBuilder();
//        final Table table = new BeatlesFilterableTable( buf, true );
//        final String explain = "PLAN=EnumerableInterpreter\n"
//                + "  BindableScan(table=[[s, beatles]], filters=[[=($0, 4)]])";
//        PolyphenyDbAssert.that()
//                .with( newSchema( "s", "beatles", table ) )
//                .query( "select * from \"s\".\"beatles\" where \"i\" = 4" )
//                .explainContains( explain )
//                .returnsUnordered( "i=4; j=John; k=1940", "i=4; j=Paul; k=1942" );
//        // Only 2 rows came out of the table. If the value is 4, it means that the planner did not pass the filter down.
//        assertThat( buf.toString(), is( "returnCount=2, filter=4" ) );
//    }
//
//
//    /**
//     * A filter on a {@link FilterableTable} with two columns (noncooperative).
//     */
//    @Test
//    public void testFilterableTableNonCooperative() throws Exception {
//        final StringBuilder buf = new StringBuilder();
//        final Table table = new BeatlesFilterableTable( buf, false );
//        final String explain = "PLAN=EnumerableInterpreter\n"
//                + "  BindableScan(table=[[s, beatles2]], filters=[[=($0, 4)]])";
//        PolyphenyDbAssert.that()
//                .with( newSchema( "s", "beatles2", table ) )
//                .query( "select * from \"s\".\"beatles2\" where \"i\" = 4" )
//                .explainContains( explain )
//                .returnsUnordered( "i=4; j=John; k=1940", "i=4; j=Paul; k=1942" );
//        assertThat( buf.toString(), is( "returnCount=4" ) );
//    }
//
//
//    /**
//     * A filter on a {@link ProjectableFilterableTable} with two columns (cooperative).
//     */
//    @Test
//    public void testProjectableFilterableCooperative() throws Exception {
//        final StringBuilder buf = new StringBuilder();
//        final Table table = new BeatlesProjectableFilterableTable( buf, true );
//        final String explain = "PLAN=EnumerableInterpreter\n"
//                + "  BindableScan(table=[[s, beatles]], filters=[[=($0, 4)]], projects=[[1]])";
//        PolyphenyDbAssert.that()
//                .with( newSchema( "s", "beatles", table ) )
//                .query( "select \"j\" from \"s\".\"beatles\" where \"i\" = 4" )
//                .explainContains( explain )
//                .returnsUnordered( "j=John", "j=Paul" );
//        // Only 2 rows came out of the table. If the value is 4, it means that the planner did not pass the filter down.
//        assertThat( buf.toString(), is( "returnCount=2, filter=4, projects=[1]" ) );
//    }
//
//
//    @Test
//    public void testProjectableFilterableNonCooperative() throws Exception {
//        final StringBuilder buf = new StringBuilder();
//        final Table table = new BeatlesProjectableFilterableTable( buf, false );
//        final String explain = "PLAN=EnumerableInterpreter\n"
//                + "  BindableScan(table=[[s, beatles2]], filters=[[=($0, 4)]], projects=[[1]]";
//        PolyphenyDbAssert.that()
//                .with( newSchema( "s", "beatles2", table ) )
//                .query( "select \"j\" from \"s\".\"beatles2\" where \"i\" = 4" )
//                .explainContains( explain )
//                .returnsUnordered( "j=John", "j=Paul" );
//        assertThat( buf.toString(), is( "returnCount=4, projects=[1, 0]" ) );
//    }
//
//
//    /**
//     * A filter on a {@link ProjectableFilterableTable} with two columns, and a project in the query. (Cooperative)
//     */
//    @Test
//    public void testProjectableFilterableWithProjectAndFilter() throws Exception {
//        final StringBuilder buf = new StringBuilder();
//        final Table table = new BeatlesProjectableFilterableTable( buf, true );
//        final String explain = "PLAN=EnumerableInterpreter\n"
//                + "  BindableScan(table=[[s, beatles]], filters=[[=($0, 4)]], projects=[[2, 1]]";
//        PolyphenyDbAssert.that()
//                .with( newSchema( "s", "beatles", table ) )
//                .query( "select \"k\",\"j\" from \"s\".\"beatles\" where \"i\" = 4" )
//                .explainContains( explain )
//                .returnsUnordered( "k=1940; j=John", "k=1942; j=Paul" );
//        assertThat( buf.toString(), is( "returnCount=2, filter=4, projects=[2, 1]" ) );
//    }
//
//
//    /**
//     * A filter on a {@link ProjectableFilterableTable} with two columns, and a project in the query (NonCooperative).
//     */
//    @Test
//    public void testProjectableFilterableWithProjectFilterNonCooperative() throws Exception {
//        final StringBuilder buf = new StringBuilder();
//        final Table table = new BeatlesProjectableFilterableTable( buf, false );
//        final String explain = "PLAN=EnumerableInterpreter\n"
//                + "  BindableScan(table=[[s, beatles]], filters=[[>($2, 1941)]], projects=[[0, 2]])";
//        PolyphenyDbAssert.that()
//                .with( newSchema( "s", "beatles", table ) )
//                .query( "select \"i\",\"k\" from \"s\".\"beatles\" where \"k\" > 1941" )
//                .explainContains( explain )
//                .returnsUnordered( "i=4; k=1942", "i=6; k=1943" );
//        assertThat( buf.toString(), is( "returnCount=4, projects=[0, 2]" ) );
//    }
//
//
//    /**
//     * A filter and project on a {@link ProjectableFilterableTable}. The table refuses to execute the filter, so Polypheny-DB should add a pull up and
//     * transform the filter (projecting the column needed by the filter).
//     */
//    @Test
//    public void testPFTableRefusesFilterCooperative() throws Exception {
//        final StringBuilder buf = new StringBuilder();
//        final Table table = new BeatlesProjectableFilterableTable( buf, false );
//        final String explain = "PLAN=EnumerableInterpreter\n"
//                + "  BindableScan(table=[[s, beatles2]], filters=[[=($0, 4)]], projects=[[2]])";
//        PolyphenyDbAssert.that()
//                .with( newSchema( "s", "beatles2", table ) )
//                .query( "select \"k\" from \"s\".\"beatles2\" where \"i\" = 4" )
//                .explainContains( explain )
//                .returnsUnordered( "k=1940", "k=1942" );
//        assertThat( buf.toString(), is( "returnCount=4, projects=[2, 0]" ) );
//    }
//
//
//    @Test
//    public void testPFPushDownProjectFilterInAggregateNoGroup() {
//        final StringBuilder buf = new StringBuilder();
//        final Table table = new BeatlesProjectableFilterableTable( buf, false );
//        final String explain = "PLAN=EnumerableAggregate(group=[{}], M=[MAX($0)])\n"
//                + "  EnumerableInterpreter\n"
//                + "    BindableScan(table=[[s, beatles]], filters=[[>($0, 1)]], projects=[[2]])";
//        PolyphenyDbAssert.that()
//                .with( newSchema( "s", "beatles", table ) )
//                .query( "select max(\"k\") as m from \"s\".\"beatles\" where \"i\" > 1" )
//                .explainContains( explain )
//                .returnsUnordered( "M=1943" );
//    }
//
//
//    @Test
//    public void testPFPushDownProjectFilterAggregateGroup() {
//        final String sql = "select \"i\", count(*) as c\n"
//                + "from \"s\".\"beatles\"\n"
//                + "where \"k\" > 1900\n"
//                + "group by \"i\"";
//        final StringBuilder buf = new StringBuilder();
//        final Table table = new BeatlesProjectableFilterableTable( buf, false );
//        final String explain = "PLAN="
//                + "EnumerableAggregate(group=[{0}], C=[COUNT()])\n"
//                + "  EnumerableInterpreter\n"
//                + "    BindableScan(table=[[s, beatles]], filters=[[>($2, 1900)]], projects=[[0]])";
//        PolyphenyDbAssert.that()
//                .with( newSchema( "s", "beatles", table ) )
//                .query( sql )
//                .explainContains( explain )
//                .returnsUnordered( "i=4; C=2", "i=5; C=1", "i=6; C=1" );
//    }
//
//
//    @Test
//    public void testPFPushDownProjectFilterAggregateNested() {
//        final StringBuilder buf = new StringBuilder();
//        final String sql = "select \"k\", count(*) as c\n"
//                + "from (\n"
//                + "  select \"k\", \"i\" from \"s\".\"beatles\" group by \"k\", \"i\") t\n"
//                + "where \"k\" = 1940\n"
//                + "group by \"k\"";
//        final Table table = new BeatlesProjectableFilterableTable( buf, false );
//        final String explain = "PLAN="
//                + "EnumerableAggregate(group=[{0}], C=[COUNT()])\n"
//                + "  EnumerableAggregate(group=[{0, 1}])\n"
//                + "    EnumerableInterpreter\n"
//                + "      BindableScan(table=[[s, beatles]], filters=[[=($2, 1940)]], projects=[[2, 0]])";
//        PolyphenyDbAssert.that()
//                .with( newSchema( "s", "beatles", table ) )
//                .query( sql )
//                .explainContains( explain )
//                .returnsUnordered( "k=1940; C=2" );
//    }
//
//
//    /**
//     * Test case for "ArrayIndexOutOfBoundsException when using just a single column in interpreter".
//     */
//    @Test
//    public void testPFTableRefusesFilterSingleColumn() throws Exception {
//        final StringBuilder buf = new StringBuilder();
//        final Table table = new BeatlesProjectableFilterableTable( buf, false );
//        final String explain = "PLAN=EnumerableInterpreter\n"
//                + "  BindableScan(table=[[s, beatles2]], filters=[[>($2, 1941)]], projects=[[2]])";
//        PolyphenyDbAssert.that()
//                .with( newSchema( "s", "beatles2", table ) )
//                .query( "select \"k\" from \"s\".\"beatles2\" where \"k\" > 1941" )
//                .explainContains( explain )
//                .returnsUnordered( "k=1942", "k=1943" );
//        assertThat( buf.toString(), is( "returnCount=4, projects=[2]" ) );
//    }
//
//
//    /**
//     * Test case for "AssertionError when pushing project to ProjectableFilterableTable".
//     * Cannot push down a project if it is not a permutation of columns; in this case, it contains a literal.
//     */
//    @Test
//    public void testCannotPushProject() throws Exception {
//        final StringBuilder buf = new StringBuilder();
//        final Table table = new BeatlesProjectableFilterableTable( buf, true );
//        final String explain = "PLAN=EnumerableCalc(expr#0..2=[{inputs}], expr#3=[3], k=[$t2], j=[$t1], i=[$t0], EXPR$3=[$t3])\n"
//                + "  EnumerableInterpreter\n"
//                + "    BindableScan(table=[[s, beatles]])";
//        PolyphenyDbAssert.that()
//                .with( newSchema( "s", "beatles", table ) )
//                .query( "select \"k\",\"j\",\"i\",3 from \"s\".\"beatles\"" )
//                .explainContains( explain )
//                .returnsUnordered( "k=1940; j=John; i=4; EXPR$3=3", "k=1940; j=Ringo; i=5; EXPR$3=3", "k=1942; j=Paul; i=4; EXPR$3=3", "k=1943; j=George; i=6; EXPR$3=3" );
//        assertThat( buf.toString(), is( "returnCount=4" ) );
//    }
//
//
//    /**
//     * Test case for "In prepared statement, CsvScannableTable.scan is called twice".
//     */
//    @Test
//    public void testPrepared2() throws SQLException {
//        final Properties properties = new Properties();
//        properties.setProperty( "caseSensitive", "true" );
//        try (
//                Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:", properties )
//        ) {
//            final PolyphenyDbEmbeddedConnection polyphenyDbEmbeddedConnection = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
//
//            final AtomicInteger scanCount = new AtomicInteger();
//            final AtomicInteger enumerateCount = new AtomicInteger();
//            final Schema schema =
//                    new AbstractSchema() {
//                        @Override
//                        protected Map<String, Table> getTables() {
//                            return ImmutableMap.of( "TENS",
//                                    new SimpleTable() {
//                                        private Enumerable<Object[]> superScan( DataContext root ) {
//                                            return super.scan( root );
//                                        }
//
//
//                                        @Override
//                                        public Enumerable<Object[]>
//                                        scan( final DataContext root ) {
//                                            scanCount.incrementAndGet();
//                                            return new AbstractEnumerable<Object[]>() {
//                                                @Override
//                                                public Enumerator<Object[]> enumerator() {
//                                                    enumerateCount.incrementAndGet();
//                                                    return superScan( root ).enumerator();
//                                                }
//                                            };
//                                        }
//                                    } );
//                        }
//                    };
//            polyphenyDbEmbeddedConnection.getRootSchema().add( "TEST", schema );
//            final String sql = "select * from \"TEST\".\"TENS\" where \"i\" < ?";
//            final PreparedStatement statement = polyphenyDbEmbeddedConnection.prepareStatement( sql );
//            assertThat( scanCount.get(), is( 0 ) );
//            assertThat( enumerateCount.get(), is( 0 ) );
//
//            // First execute
//            statement.setInt( 1, 20 );
//            assertThat( scanCount.get(), is( 0 ) );
//            ResultSet resultSet = statement.executeQuery();
//            assertThat( scanCount.get(), is( 1 ) );
//            assertThat( enumerateCount.get(), is( 1 ) );
//            assertThat( resultSet, Matchers.returnsUnordered( "i=0", "i=10" ) );
//            assertThat( scanCount.get(), is( 1 ) );
//            assertThat( enumerateCount.get(), is( 1 ) );
//
//            // Second execute
//            resultSet = statement.executeQuery();
//            assertThat( scanCount.get(), is( 2 ) );
//            assertThat( resultSet, Matchers.returnsUnordered( "i=0", "i=10" ) );
//            assertThat( scanCount.get(), is( 2 ) );
//
//            // Third execute
//            statement.setInt( 1, 30 );
//            resultSet = statement.executeQuery();
//            assertThat( scanCount.get(), is( 3 ) );
//            assertThat( resultSet, Matchers.returnsUnordered( "i=0", "i=10", "i=20" ) );
//            assertThat( scanCount.get(), is( 3 ) );
//        }
//    }
//
//
//    protected ConnectionPostProcessor newSchema( final String schemaName, final String tableName, final Table table ) {
//        return connection -> {
//            PolyphenyDbEmbeddedConnection con = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
//            SchemaPlus rootSchema = con.getRootSchema();
//            SchemaPlus schema = rootSchema.add( schemaName, new AbstractSchema() );
//            schema.add( tableName, table );
//            connection.setSchema( schemaName );
//            return connection;
//        };
//    }


    private static Integer getFilter( boolean cooperative, List<RexNode> filters ) {
        final Iterator<RexNode> filterIter = filters.iterator();
        while ( filterIter.hasNext() ) {
            final RexNode node = filterIter.next();
            if ( cooperative
                    && node instanceof RexCall
                    && ((RexCall) node).getOperator().getOperatorName() == OperatorName.EQUALS
                    && ((RexCall) node).getOperands().get( 0 ) instanceof RexIndexRef
                    && ((RexIndexRef) ((RexCall) node).getOperands().get( 0 )).getIndex() == 0
                    && ((RexCall) node).getOperands().get( 1 ) instanceof RexLiteral ) {
                final RexNode op1 = ((RexCall) node).getOperands().get( 1 );
                filterIter.remove();
                return ((RexLiteral) op1).getValue().asBigDecimal().intValue();
            }
        }
        return null;
    }


    private static Enumerator<PolyValue[]> tens() {
        return new Enumerator<>() {
            int row = -1;
            PolyValue[] current;


            @Override
            public PolyValue[] current() {
                return current;
            }


            @Override
            public boolean moveNext() {
                if ( ++row < 4 ) {
                    current = new PolyValue[]{ PolyInteger.of( row * 10 ) };
                    return true;
                } else {
                    return false;
                }
            }


            @Override
            public void reset() {
                row = -1;
            }


            @Override
            public void close() {
                current = null;
            }
        };
    }


}


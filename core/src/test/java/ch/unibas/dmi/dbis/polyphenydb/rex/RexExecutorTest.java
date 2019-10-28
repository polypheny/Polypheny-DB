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

package ch.unibas.dmi.dbis.polyphenydb.rex;


import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.DataContext.SlimDataContext;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.ContextImpl;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptSchema;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.AbstractPolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlBinaryOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlMonotonicBinaryOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.InferTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.tools.FrameworkConfig;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.util.DateString;
import ch.unibas.dmi.dbis.polyphenydb.util.NlsString;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.QueryProvider;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit test for {@link ch.unibas.dmi.dbis.polyphenydb.rex.RexExecutorImpl}.
 */
public class RexExecutorTest {

    public RexExecutorTest() {
    }


    protected void check( final Action action ) throws Exception {
        PolyphenyDbSchema rootSchema = AbstractPolyphenyDbSchema.createRootSchema( false );
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema( rootSchema.plus() )
                .prepareContext( new ContextImpl(
                        rootSchema,
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
        Frameworks.withPrepare(
                new Frameworks.PrepareAction<Void>( config ) {
                    @Override
                    public Void apply( RelOptCluster cluster, RelOptSchema relOptSchema, SchemaPlus rootSchema ) {
                        final RexBuilder rexBuilder = cluster.getRexBuilder();
                        DataContext dataContext = Schemas.createDataContext( rootSchema );
                        final RexExecutorImpl executor = new RexExecutorImpl( dataContext );
                        action.check( rexBuilder, executor );
                        return null;
                    }
                } );
    }


    /**
     * Tests an executor that uses variables stored in a {@link DataContext}. Can change the value of the variable and execute again.
     */
    @Test
    public void testVariableExecution() throws Exception {
        check( ( rexBuilder, executor ) -> {
            Object[] values = new Object[1];
            final DataContext testContext = new TestDataContext( values );
            final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
            final RelDataType varchar = typeFactory.createSqlType( SqlTypeName.VARCHAR );
            final RelDataType integer = typeFactory.createSqlType( SqlTypeName.INTEGER );
            // Polypheny-DB is internally creating the input ref via a RexRangeRef
            // which eventually leads to a RexInputRef. So we are good.
            final RexInputRef input = rexBuilder.makeInputRef( varchar, 0 );
            final RexNode lengthArg = rexBuilder.makeLiteral( 3, integer, true );
            final RexNode substr = rexBuilder.makeCall( SqlStdOperatorTable.SUBSTRING, input, lengthArg );
            ImmutableList<RexNode> constExps = ImmutableList.of( substr );

            final RelDataType rowType = typeFactory.builder()
                    .add( "someStr", varchar )
                    .build();

            final RexExecutable exec = executor.getExecutable( rexBuilder, constExps, rowType );
            exec.setDataContext( testContext );
            values[0] = "Hello World";
            Object[] result = exec.execute();
            assertTrue( result[0] instanceof String );
            assertThat( (String) result[0], equalTo( "llo World" ) );
            values[0] = "Polypheny";
            result = exec.execute();
            assertTrue( result[0] instanceof String );
            assertThat( (String) result[0], equalTo( "lypheny" ) );
        } );
    }


    @Test
    public void testConstant() throws Exception {
        check( ( rexBuilder, executor ) -> {
            final List<RexNode> reducedValues = new ArrayList<>();
            final RexLiteral ten = rexBuilder.makeExactLiteral( BigDecimal.TEN );
            executor.reduce( rexBuilder, ImmutableList.of( ten ), reducedValues );
            assertThat( reducedValues.size(), equalTo( 1 ) );
            assertThat( reducedValues.get( 0 ), instanceOf( RexLiteral.class ) );
            assertThat( ((RexLiteral) reducedValues.get( 0 )).getValue2(), equalTo( (Object) 10L ) );
        } );
    }


    /**
     * Reduces several expressions to constants.
     */
    @Test
    public void testConstant2() throws Exception {
        // Same as testConstant; 10 -> 10
        checkConstant( 10L, rexBuilder -> rexBuilder.makeExactLiteral( BigDecimal.TEN ) );
        // 10 + 1 -> 11
        checkConstant( 11L, rexBuilder -> rexBuilder.makeCall( SqlStdOperatorTable.PLUS, rexBuilder.makeExactLiteral( BigDecimal.TEN ), rexBuilder.makeExactLiteral( BigDecimal.ONE ) ) );
        // date 'today' <= date 'today' -> true
        checkConstant( true, rexBuilder -> {
            final DateString d = DateString.fromCalendarFields( Util.calendar() );
            return rexBuilder.makeCall( SqlStdOperatorTable.LESS_THAN_OR_EQUAL, rexBuilder.makeDateLiteral( d ), rexBuilder.makeDateLiteral( d ) );
        } );
        // date 'today' < date 'today' -> false
        checkConstant( false, rexBuilder -> {
            final DateString d = DateString.fromCalendarFields( Util.calendar() );
            return rexBuilder.makeCall( SqlStdOperatorTable.LESS_THAN, rexBuilder.makeDateLiteral( d ), rexBuilder.makeDateLiteral( d ) );
        } );
    }


    private void checkConstant( final Object operand, final Function<RexBuilder, RexNode> function ) throws Exception {
        check( ( rexBuilder, executor ) -> {
            final List<RexNode> reducedValues = new ArrayList<>();
            final RexNode expression = function.apply( rexBuilder );
            assert expression != null;
            executor.reduce( rexBuilder, ImmutableList.of( expression ), reducedValues );
            assertThat( reducedValues.size(), equalTo( 1 ) );
            assertThat( reducedValues.get( 0 ), instanceOf( RexLiteral.class ) );
            assertThat( ((RexLiteral) reducedValues.get( 0 )).getValue2(), equalTo( operand ) );
        } );
    }


    @Test
    public void testSubstring() throws Exception {
        check( ( rexBuilder, executor ) -> {
            final List<RexNode> reducedValues = new ArrayList<>();
            final RexLiteral hello = rexBuilder.makeCharLiteral( new NlsString( "Hello world!", null, null ) );
            final RexNode plus = rexBuilder.makeCall( SqlStdOperatorTable.PLUS, rexBuilder.makeExactLiteral( BigDecimal.ONE ), rexBuilder.makeExactLiteral( BigDecimal.ONE ) );
            RexLiteral four = rexBuilder.makeExactLiteral( BigDecimal.valueOf( 4 ) );
            final RexNode substring = rexBuilder.makeCall( SqlStdOperatorTable.SUBSTRING, hello, plus, four );
            executor.reduce( rexBuilder, ImmutableList.of( substring, plus ), reducedValues );
            assertThat( reducedValues.size(), equalTo( 2 ) );
            assertThat( reducedValues.get( 0 ), instanceOf( RexLiteral.class ) );
            assertThat( ((RexLiteral) reducedValues.get( 0 )).getValue2(), equalTo( (Object) "ello" ) ); // substring('Hello world!, 2, 4)
            assertThat( reducedValues.get( 1 ), instanceOf( RexLiteral.class ) );
            assertThat( ((RexLiteral) reducedValues.get( 1 )).getValue2(), equalTo( (Object) 2L ) );
        } );
    }


    @Test
    public void testBinarySubstring() throws Exception {
        check( ( rexBuilder, executor ) -> {
            final List<RexNode> reducedValues = new ArrayList<>();
            // hello world! -> 48656c6c6f20776f726c6421
            final RexLiteral binaryHello = rexBuilder.makeBinaryLiteral( new ByteString( "Hello world!".getBytes( UTF_8 ) ) );
            final RexNode plus = rexBuilder.makeCall( SqlStdOperatorTable.PLUS, rexBuilder.makeExactLiteral( BigDecimal.ONE ), rexBuilder.makeExactLiteral( BigDecimal.ONE ) );
            RexLiteral four = rexBuilder.makeExactLiteral( BigDecimal.valueOf( 4 ) );
            final RexNode substring = rexBuilder.makeCall( SqlStdOperatorTable.SUBSTRING, binaryHello, plus, four );
            executor.reduce( rexBuilder, ImmutableList.of( substring, plus ), reducedValues );
            assertThat( reducedValues.size(), equalTo( 2 ) );
            assertThat( reducedValues.get( 0 ), instanceOf( RexLiteral.class ) );
            assertThat( ((RexLiteral) reducedValues.get( 0 )).getValue2().toString(), equalTo( (Object) "656c6c6f" ) ); // substring('Hello world!, 2, 4)
            assertThat( reducedValues.get( 1 ), instanceOf( RexLiteral.class ) );
            assertThat( ((RexLiteral) reducedValues.get( 1 )).getValue2(), equalTo( (Object) 2L ) );
        } );
    }


    @Test
    public void testDeterministic1() throws Exception {
        check( ( rexBuilder, executor ) -> {
            final RexNode plus = rexBuilder.makeCall( SqlStdOperatorTable.PLUS, rexBuilder.makeExactLiteral( BigDecimal.ONE ), rexBuilder.makeExactLiteral( BigDecimal.ONE ) );
            assertThat( RexUtil.isDeterministic( plus ), equalTo( true ) );
        } );
    }


    @Test
    public void testDeterministic2() throws Exception {
        check( ( rexBuilder, executor ) -> {
            final RexNode plus = rexBuilder.makeCall( PLUS_RANDOM, rexBuilder.makeExactLiteral( BigDecimal.ONE ), rexBuilder.makeExactLiteral( BigDecimal.ONE ) );
            assertThat( RexUtil.isDeterministic( plus ), equalTo( false ) );
        } );
    }


    @Test
    public void testDeterministic3() throws Exception {
        check( ( rexBuilder, executor ) -> {
            final RexNode plus =
                    rexBuilder.makeCall( SqlStdOperatorTable.PLUS,
                            rexBuilder.makeCall( PLUS_RANDOM, rexBuilder.makeExactLiteral( BigDecimal.ONE ), rexBuilder.makeExactLiteral( BigDecimal.ONE ) ),
                            rexBuilder.makeExactLiteral( BigDecimal.ONE ) );
            assertThat( RexUtil.isDeterministic( plus ), equalTo( false ) );
        } );
    }


    private static final SqlBinaryOperator PLUS_RANDOM =
            new SqlMonotonicBinaryOperator(
                    "+",
                    SqlKind.PLUS,
                    40,
                    true,
                    ReturnTypes.NULLABLE_SUM,
                    InferTypes.FIRST_KNOWN,
                    OperandTypes.PLUS_OPERATOR ) {
                @Override
                public boolean isDeterministic() {
                    return false;
                }
            };


    /**
     * Test case for "SelfPopulatingList is not thread-safe".
     */
    @Test
    public void testSelfPopulatingList() {
        final List<Thread> threads = new ArrayList<>();
        //noinspection MismatchedQueryAndUpdateOfCollection
        final List<String> list = new RexSlot.SelfPopulatingList( "$", 1 );
        final Random random = new Random();
        for ( int i = 0; i < 10; i++ ) {
            threads.add(
                    new Thread( () -> {
                        for ( int j = 0; j < 1000; j++ ) {
                            // Random numbers between 0 and ~1m, smaller values more common
                            final int index = random.nextInt( 1234567 ) >> random.nextInt( 16 ) >> random.nextInt( 16 );
                            list.get( index );
                        }
                    } ) );
        }
        for ( Thread runnable : threads ) {
            runnable.start();
        }
        for ( Thread runnable : threads ) {
            try {
                runnable.join();
            } catch ( InterruptedException e ) {
                e.printStackTrace();
            }
        }
        final int size = list.size();
        for ( int i = 0; i < size; i++ ) {
            assertThat( list.get( i ), is( "$" + i ) );
        }
    }


    @Test
    public void testSelfPopulatingList30() {
        //noinspection MismatchedQueryAndUpdateOfCollection
        final List<String> list = new RexSlot.SelfPopulatingList( "$", 30 );
        final String s = list.get( 30 );
        assertThat( s, is( "$30" ) );
    }


    /**
     * Callback for {@link #check}. Test code will typically use {@code builder} to create some expressions, call {@link ch.unibas.dmi.dbis.polyphenydb.rex.RexExecutorImpl#reduce} to evaluate them into a list, then check that the results are as expected.
     */
    interface Action {

        void check( RexBuilder rexBuilder, RexExecutorImpl executor );
    }


    /**
     * ArrayList-based DataContext to check Rex execution.
     */
    public static class TestDataContext implements DataContext {

        private final Object[] values;


        public TestDataContext( Object[] values ) {
            this.values = values;
        }


        @Override
        public SchemaPlus getRootSchema() {
            throw new RuntimeException( "Unsupported" );
        }


        @Override
        public JavaTypeFactory getTypeFactory() {
            throw new RuntimeException( "Unsupported" );
        }


        @Override
        public QueryProvider getQueryProvider() {
            throw new RuntimeException( "Unsupported" );
        }


        @Override
        public Object get( String name ) {
            if ( name.equals( "inputRecord" ) ) {
                return values;
            } else {
                Assert.fail( "Wrong DataContext access" );
                return null;
            }
        }


        @Override
        public void addAll( Map<String, Object> map ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public Transaction getTransaction() {
            return null;
        }
    }
}


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


import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataContext.SlimDataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.prepare.ContextImpl;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexExecutable;
import org.polypheny.db.rex.RexExecutorImpl;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexSlot.SelfPopulatingList;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.schema.AbstractPolyphenyDbSchema;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.sql.language.SqlBinaryOperator;
import org.polypheny.db.sql.language.fun.SqlMonotonicBinaryOperator;
import org.polypheny.db.tools.FrameworkConfig;
import org.polypheny.db.tools.Frameworks;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.Util;


/**
 * Unit test for {@link org.polypheny.db.rex.RexExecutorImpl}.
 */
public class RexExecutorTest extends SqlLanguagelDependant {

    public RexExecutorTest() {
    }


    protected void check( final Action action ) throws Exception {
        PolyphenyDbSchema rootSchema = AbstractPolyphenyDbSchema.createRootSchema( "" );
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
                    public Void apply( AlgOptCluster cluster, AlgOptSchema algOptSchema, SchemaPlus rootSchema ) {
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
            final AlgDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
            final AlgDataType varchar = typeFactory.createPolyType( PolyType.VARCHAR );
            final AlgDataType integer = typeFactory.createPolyType( PolyType.INTEGER );
            // Polypheny-DB is internally creating the input ref via a RexRangeRef
            // which eventually leads to a RexInputRef. So we are good.
            final RexInputRef input = rexBuilder.makeInputRef( varchar, 0 );
            final RexNode lengthArg = rexBuilder.makeLiteral( 3, integer, true );
            final RexNode substr = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.SUBSTRING ), input, lengthArg );
            ImmutableList<RexNode> constExps = ImmutableList.of( substr );

            final AlgDataType rowType = typeFactory.builder()
                    .add( "someStr", null, varchar )
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
        checkConstant( 11L, rexBuilder -> rexBuilder.makeCall( OperatorRegistry.get( OperatorName.PLUS ), rexBuilder.makeExactLiteral( BigDecimal.TEN ), rexBuilder.makeExactLiteral( BigDecimal.ONE ) ) );
        // date 'today' <= date 'today' -> true
        checkConstant( true, rexBuilder -> {
            final DateString d = DateString.fromCalendarFields( Util.calendar() );
            return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ), rexBuilder.makeDateLiteral( d ), rexBuilder.makeDateLiteral( d ) );
        } );
        // date 'today' < date 'today' -> false
        checkConstant( false, rexBuilder -> {
            final DateString d = DateString.fromCalendarFields( Util.calendar() );
            return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.LESS_THAN ), rexBuilder.makeDateLiteral( d ), rexBuilder.makeDateLiteral( d ) );
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
            final RexNode plus = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.PLUS ), rexBuilder.makeExactLiteral( BigDecimal.ONE ), rexBuilder.makeExactLiteral( BigDecimal.ONE ) );
            RexLiteral four = rexBuilder.makeExactLiteral( BigDecimal.valueOf( 4 ) );
            final RexNode substring = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.SUBSTRING ), hello, plus, four );
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
            final RexNode plus = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.PLUS ), rexBuilder.makeExactLiteral( BigDecimal.ONE ), rexBuilder.makeExactLiteral( BigDecimal.ONE ) );
            RexLiteral four = rexBuilder.makeExactLiteral( BigDecimal.valueOf( 4 ) );
            final RexNode substring = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.SUBSTRING ), binaryHello, plus, four );
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
            final RexNode plus = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.PLUS ), rexBuilder.makeExactLiteral( BigDecimal.ONE ), rexBuilder.makeExactLiteral( BigDecimal.ONE ) );
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
                    rexBuilder.makeCall(
                            OperatorRegistry.get( OperatorName.PLUS ),
                            rexBuilder.makeCall( PLUS_RANDOM, rexBuilder.makeExactLiteral( BigDecimal.ONE ), rexBuilder.makeExactLiteral( BigDecimal.ONE ) ),
                            rexBuilder.makeExactLiteral( BigDecimal.ONE ) );
            assertThat( RexUtil.isDeterministic( plus ), equalTo( false ) );
        } );
    }


    private static final SqlBinaryOperator PLUS_RANDOM =
            new SqlMonotonicBinaryOperator(
                    "+",
                    Kind.PLUS,
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
        final List<String> list = new SelfPopulatingList( "$", 1 );
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
        final List<String> list = new SelfPopulatingList( "$", 30 );
        final String s = list.get( 30 );
        assertThat( s, is( "$30" ) );
    }


    /**
     * Callback for {@link #check}. Test code will typically use {@code builder} to create some expressions, call {@link org.polypheny.db.rex.RexExecutorImpl#reduce} to evaluate them into a list, then check that the results are as expected.
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

}


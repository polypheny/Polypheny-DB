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

package ch.unibas.dmi.dbis.polyphenydb.tools;


import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.DataContext.SlimDataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableTableScan;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.ContextImpl;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.plan.ConventionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptAbstractTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptSchema;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.AbstractConverter;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbPrepareImpl;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistributionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalFilter;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystemImpl;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.ModifiableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Path;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.ProjectableFilterableTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.schema.Statistic;
import ch.unibas.dmi.dbis.polyphenydb.schema.Statistics;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainFormat;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.AnsiSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParseException;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.test.PolyphenyDbAssert;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.junit.Test;


/**
 * Unit tests for methods in {@link Frameworks}.
 */
public class FrameworksTest {

    @Test
    public void testOptimize() {
        RelNode x =
                Frameworks.withPlanner( ( cluster, relOptSchema, rootSchema ) -> {
                    final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
                    final Table table = new AbstractTable() {
                        public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
                            final RelDataType stringType = typeFactory.createJavaType( String.class );
                            final RelDataType integerType = typeFactory.createJavaType( Integer.class );
                            return typeFactory.builder()
                                    .add( "s", stringType )
                                    .add( "i", integerType )
                                    .build();
                        }
                    };

                    // "SELECT * FROM myTable"
                    final RelOptAbstractTable relOptTable = new RelOptAbstractTable( relOptSchema, "myTable", table.getRowType( typeFactory ) ) {
                    };
                    final EnumerableTableScan tableRel = EnumerableTableScan.create( cluster, relOptTable );

                    // "WHERE i > 1"
                    final RexBuilder rexBuilder = cluster.getRexBuilder();
                    final RexNode condition =
                            rexBuilder.makeCall(
                                    SqlStdOperatorTable.GREATER_THAN,
                                    rexBuilder.makeFieldAccess( rexBuilder.makeRangeReference( tableRel ), "i", true ),
                                    rexBuilder.makeExactLiteral( BigDecimal.ONE ) );
                    final LogicalFilter filter = LogicalFilter.create( tableRel, condition );

                    // Specify that the result should be in Enumerable convention.
                    final RelNode rootRel = filter;
                    final RelOptPlanner planner = cluster.getPlanner();
                    RelTraitSet desiredTraits = cluster.traitSet().replace( EnumerableConvention.INSTANCE );
                    final RelNode rootRel2 = planner.changeTraits( rootRel, desiredTraits );
                    planner.setRoot( rootRel2 );

                    // Now, plan.
                    return planner.findBestExp();
                } );
        String s = RelOptUtil.dumpPlan( "", x, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES );
        assertThat(
                Util.toLinux( s ),
                equalTo( "EnumerableFilter(condition=[>($1, 1)])\n  EnumerableTableScan(table=[[myTable]])\n" ) );
    }


    /**
     * Unit test to test create root schema which has no "metadata" schema.
     */
    @Test
    public void testCreateRootSchemaWithNoMetadataSchema() {
        SchemaPlus rootSchema = Frameworks.createRootSchema( false );
        assertThat( rootSchema.getSubSchemaNames().size(), equalTo( 0 ) );
    }


    /**
     * Tests that validation (specifically, inferring the result of adding two DECIMAL(19, 0) values together) happens differently with a type system that allows a larger maximum precision for decimals.
     *
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-413">[POLYPHENYDB-413] Add RelDataTypeSystem plugin, allowing different max precision of a DECIMAL</a>.
     *
     * Also tests the plugin system, by specifying implementations of a plugin interface with public and private constructors.
     */
    @Test
    public void testTypeSystem() {
        checkTypeSystem( 19, Frameworks.newConfigBuilder()
                .prepareContext( new ContextImpl(
                        PolyphenyDbSchema.from( Frameworks.createRootSchema( false ) ),
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
                .build() );
        checkTypeSystem( 25, Frameworks.newConfigBuilder().typeSystem( HiveLikeTypeSystem.INSTANCE )
                .prepareContext( new ContextImpl(
                        PolyphenyDbSchema.from( Frameworks.createRootSchema( false ) ),
                        new SlimDataContext() {
                            @Override
                            public JavaTypeFactory getTypeFactory() {
                                return new JavaTypeFactoryImpl( HiveLikeTypeSystem.INSTANCE );
                            }
                        },
                        "",
                        0,
                        0,
                        null ) )
                .build() );
        checkTypeSystem( 31, Frameworks.newConfigBuilder().typeSystem( new HiveLikeTypeSystem2() )
                .prepareContext( new ContextImpl(
                        PolyphenyDbSchema.from( Frameworks.createRootSchema( false ) ),
                        new SlimDataContext() {
                            @Override
                            public JavaTypeFactory getTypeFactory() {
                                return new JavaTypeFactoryImpl( new HiveLikeTypeSystem2() );
                            }
                        },
                        "",
                        0,
                        0,
                        null ) )
                .build() );
    }


    private void checkTypeSystem( final int expected, FrameworkConfig config ) {
        Frameworks.withPrepare(
                new Frameworks.PrepareAction<Void>( config ) {
                    @Override
                    public Void apply( RelOptCluster cluster, RelOptSchema relOptSchema, SchemaPlus rootSchema ) {
                        final RelDataType type = cluster.getTypeFactory().createSqlType( SqlTypeName.DECIMAL, 30, 2 );
                        final RexLiteral literal = cluster.getRexBuilder().makeExactLiteral( BigDecimal.ONE, type );
                        final RexNode call = cluster.getRexBuilder().makeCall( SqlStdOperatorTable.PLUS, literal, literal );
                        assertEquals( expected, call.getType().getPrecision() );
                        return null;
                    }
                } );
    }


    /**
     * Tests that the validator expands identifiers by default.
     *
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-593">[POLYPHENYDB-593] Validator in Frameworks should expand identifiers</a>.
     */
    @Test
    public void testFrameworksValidatorWithIdentifierExpansion() throws Exception {
        final SchemaPlus rootSchema = Frameworks.createRootSchema( true );
        final FrameworkConfig config = Frameworks.newConfigBuilder()
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
        final Planner planner = Frameworks.getPlanner( config );
        SqlNode parse = planner.parse( "select * from \"emps\" " );
        SqlNode val = planner.validate( parse );

        String valStr = val.toSqlString( AnsiSqlDialect.DEFAULT, false ).getSql();

        String expandedStr = "SELECT `emps`.`empid`, `emps`.`deptno`, `emps`.`name`, `emps`.`salary`, `emps`.`commission`\n" + "FROM `hr`.`emps` AS `emps`";
        assertThat( Util.toLinux( valStr ), equalTo( expandedStr ) );
    }


    /**
     * Test for {@link Path}.
     */
    @Test
    public void testSchemaPath() {
        final SchemaPlus rootSchema = Frameworks.createRootSchema( true );
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema( PolyphenyDbAssert.addSchema( rootSchema, PolyphenyDbAssert.SchemaSpec.HR ) )
                .build();
        final Path path = Schemas.path( config.getDefaultSchema() );
        assertThat( path.size(), is( 2 ) );
        assertThat( path.get( 0 ).left, is( "" ) );
        assertThat( path.get( 1 ).left, is( "hr" ) );
        assertThat( path.names().size(), is( 1 ) );
        assertThat( path.names().get( 0 ), is( "hr" ) );
        assertThat( path.schemas().size(), is( 2 ) );

        final Path parent = path.parent();
        assertThat( parent.size(), is( 1 ) );
        assertThat( parent.names().size(), is( 0 ) );

        final Path grandparent = parent.parent();
        assertThat( grandparent.size(), is( 0 ) );

        try {
            Object o = grandparent.parent();
            fail( "expected exception, got " + o );
        } catch ( IllegalArgumentException e ) {
            // ok
        }
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-1996">[POLYPHENYDB-1996] VALUES syntax</a>.
     *
     * With that bug, running a VALUES query would succeed before running a query that reads from a JDBC table, but fail after it. Before, the plan would use {@link ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableValues},
     * but after, it would use {@link ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcRules.JdbcValues}, and would generate invalid SQL syntax.
     *
     * Even though the SQL generator has been fixed, we are still interested in how JDBC convention gets lodged in the planner's state.
     */
    @Test
    public void testJdbcValues() throws Exception {
        PolyphenyDbAssert.that()
                .with( PolyphenyDbAssert.SchemaSpec.JDBC_SCOTT )
                .doWithConnection( connection -> {
                    try {
                        final FrameworkConfig config = Frameworks.newConfigBuilder()
                                .defaultSchema( connection.getRootSchema() )
                                .prepareContext( new ContextImpl(
                                        PolyphenyDbSchema.from( connection.getRootSchema() ),
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
                        final RelBuilder builder = RelBuilder.create( config );
                        final RelRunner runner = connection.unwrap( RelRunner.class );

                        final RelNode values =
                                builder.values( new String[]{ "a", "b" }, "X", 1, "Y", 2 )
                                        .project( builder.field( "a" ) )
                                        .build();

                        // If you run the "values" query before the "scan" query, everything works fine. JdbcValues is never instantiated in any of the 3 queries.
                        if ( false ) {
                            runner.prepare( values ).executeQuery();
                        }

                        final RelNode scan = builder.scan( "JDBC_SCOTT", "EMP" ).build();
                        runner.prepare( scan ).executeQuery();
                        builder.clear();

                        // running this after the scott query causes the exception
                        RelRunner runner2 = connection.unwrap( RelRunner.class );
                        runner2.prepare( values ).executeQuery();
                    } catch ( Exception e ) {
                        throw new RuntimeException( e );
                    }
                } );
    }


    /**
     * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-2039">[POLYPHENYDB-2039] AssertionError when pushing project to ProjectableFilterableTable</a> using UPDATE via {@link Frameworks}.
     */
    @Test
    public void testUpdate() throws Exception {
        Table table = new TableImpl();
        final SchemaPlus rootSchema = Frameworks.createRootSchema( true );
        SchemaPlus schema = rootSchema.add( "x", new AbstractSchema() );
        schema.add( "MYTABLE", table );
        List<RelTraitDef> traitDefs = new ArrayList<>();
        traitDefs.add( ConventionTraitDef.INSTANCE );
        traitDefs.add( RelDistributionTraitDef.INSTANCE );
        SqlParser.Config parserConfig =
                SqlParser.configBuilder( SqlParser.Config.DEFAULT )
                        .setCaseSensitive( false )
                        .build();

        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig( parserConfig )
                .defaultSchema( schema )
                .traitDefs( traitDefs )
                // define the rules you want to apply
                .ruleSets( RuleSets.ofList( AbstractConverter.ExpandConversionRule.INSTANCE ) )
                .programs( Programs.ofRules( Programs.RULE_SET ) )
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
        executeQuery( config, " UPDATE MYTABLE set id=7 where id=1", PolyphenyDbPrepareImpl.DEBUG );
    }


    private void executeQuery( FrameworkConfig config, @SuppressWarnings("SameParameterValue") String query, boolean debug ) throws RelConversionException, SqlParseException, ValidationException {
        Planner planner = Frameworks.getPlanner( config );
        if ( debug ) {
            System.out.println( "Query:" + query );
        }
        SqlNode n = planner.parse( query );
        n = planner.validate( n );
        RelNode root = planner.rel( n ).project();
        if ( debug ) {
            System.out.println( RelOptUtil.dumpPlan( "-- Logical Plan", root, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        RelOptCluster cluster = root.getCluster();
        final RelOptPlanner optPlanner = cluster.getPlanner();

        RelTraitSet desiredTraits = cluster.traitSet().replace( EnumerableConvention.INSTANCE );
        final RelNode newRoot = optPlanner.changeTraits( root, desiredTraits );
        if ( debug ) {
            System.out.println( RelOptUtil.dumpPlan( "-- Mid Plan", newRoot, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        optPlanner.setRoot( newRoot );
        RelNode bestExp = optPlanner.findBestExp();
        if ( debug ) {
            System.out.println( RelOptUtil.dumpPlan( "-- Best Plan", bestExp, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
    }


    /**
     * Modifiable, filterable table.
     */
    private static class TableImpl extends AbstractTable implements ModifiableTable, ProjectableFilterableTable {

        TableImpl() {
        }


        public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
            return typeFactory.builder()
                    .add( "id", typeFactory.createSqlType( SqlTypeName.INTEGER ) )
                    .add( "name", typeFactory.createSqlType( SqlTypeName.INTEGER ) )
                    .build();
        }


        public Statistic getStatistic() {
            return Statistics.of( 15D, ImmutableList.of( ImmutableBitSet.of( 0 ) ), ImmutableList.of() );
        }


        public Enumerable<Object[]> scan( DataContext root, List<RexNode> filters, int[] projects ) {
            throw new UnsupportedOperationException();
        }


        public Collection getModifiableCollection() {
            throw new UnsupportedOperationException();
        }


        public TableModify toModificationRel( RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child, TableModify.Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
            return LogicalTableModify.create( table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened );
        }


        public <T> Queryable<T> asQueryable( QueryProvider queryProvider, SchemaPlus schema, String tableName ) {
            throw new UnsupportedOperationException();
        }


        public Type getElementType() {
            return Object.class;
        }


        public Expression getExpression( SchemaPlus schema, String tableName, Class clazz ) {
            throw new UnsupportedOperationException();
        }
    }


    /**
     * Dummy type system, similar to Hive's, accessed via an INSTANCE member.
     */
    public static class HiveLikeTypeSystem extends RelDataTypeSystemImpl {

        public static final RelDataTypeSystem INSTANCE = new HiveLikeTypeSystem();


        private HiveLikeTypeSystem() {
        }


        @Override
        public int getMaxNumericPrecision() {
            assert super.getMaxNumericPrecision() == 19;
            return 25;
        }
    }


    /**
     * Dummy type system, similar to Hive's, accessed via a public default constructor.
     */
    public static class HiveLikeTypeSystem2 extends RelDataTypeSystemImpl {

        public HiveLikeTypeSystem2() {
        }


        @Override
        public int getMaxNumericPrecision() {
            assert super.getMaxNumericPrecision() == 19;
            return 38;
        }
    }
}

